﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using MySql.Data.Protocol.Serialization;

namespace MySql.Data.Serialization
{
	internal sealed class MySqlSession
	{
		public MySqlSession()
			: this(null, 0)
		{
		}

		public MySqlSession(ConnectionPool pool, int poolGeneration)
		{
			Pool = pool;
			PoolGeneration = poolGeneration;
		}

		public ServerVersion ServerVersion { get; set; }
		public int ConnectionId { get; set; }
		public byte[] AuthPluginData { get; set; }
		public ConnectionPool Pool { get; }
		public int PoolGeneration { get; }

		public void ReturnToPool() => Pool?.Return(this);

		public async Task DisposeAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			if (m_protocolLayer != null)
			{
				try
				{
					m_protocolLayer.StartNewConversation();
					await m_protocolLayer.WriteAsync(QuitPayload.Create(), ioBehavior).ConfigureAwait(false);
					await m_protocolLayer.ReadAsync(null, ProtocolErrorBehavior.Ignore, ioBehavior).ConfigureAwait(false);
				}
				catch (SocketException)
				{
					// socket may have been closed during shutdown; ignore
				}
				m_protocolLayer = null;
			}
			if (m_tcpClient != null)
			{
				try
				{
#if NETSTANDARD1_3
					m_tcpClient.Dispose();
#else
					m_tcpClient.Close();
#endif
				}
				catch (SocketException)
				{
				}
				m_tcpClient = null;
			}
			m_state = State.Closed;
		}

		public async Task ConnectAsync(IEnumerable<string> hosts, int port, string userId, string password, string database,
			SslMode sslMode, string certificateFile, string certificatePassword, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			var connected = await OpenSocketAsync(hosts, port, ioBehavior, cancellationToken).ConfigureAwait(false);
			if (!connected)
				throw new MySqlException("Unable to connect to any of the specified MySQL hosts.");

			var payload = await ReceiveAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
			var reader = new ByteArrayReader(payload.ArraySegment.Array, payload.ArraySegment.Offset, payload.ArraySegment.Count);
			var initialHandshake = new InitialHandshakePacket(reader);
			if (initialHandshake.AuthPluginName != "mysql_native_password")
				throw new NotSupportedException("Authentication method '{0}' is not supported.".FormatInvariant(initialHandshake.AuthPluginName));
			ServerVersion = new ServerVersion(Encoding.ASCII.GetString(initialHandshake.ServerVersion));
			ConnectionId = initialHandshake.ConnectionId;
			AuthPluginData = initialHandshake.AuthPluginData;

			if (sslMode != SslMode.None)
			{
				X509Certificate2 certificate;
				try
				{
					certificate = new X509Certificate2(certificateFile, certificatePassword);
				}
				catch (CryptographicException ex)
				{
					if (!File.Exists(certificateFile))
						throw new MySqlException("Cannot find SSL Certificate File", ex);
					throw new MySqlException("Either the SSL Certificate Password is incorrect or the SSL Certificate File is invalid", ex);
				}

				Func<object, string, X509CertificateCollection, X509Certificate, string[], X509Certificate> localCertificateCb =
					(lcbSender, lcbTargetHost, lcbLocalCertificates, lcbRemoteCertificate, lcbAcceptableIssuers) => lcbLocalCertificates[0];

				Func<object, X509Certificate, X509Chain, SslPolicyErrors, bool> remoteCertificateCb =
					(rcbSender, rcbCertificate, rcbChain, rcbPolicyErrors) =>
				{
					switch (rcbPolicyErrors)
					{
						case SslPolicyErrors.None:
							return true;
						case SslPolicyErrors.RemoteCertificateNameMismatch:
							return sslMode != SslMode.VerifyFull;
						default:
							return sslMode == SslMode.Required;
					}
				};

				var sslStream = new SslStream(m_tcpClient.GetStream(), false,
					new RemoteCertificateValidationCallback(remoteCertificateCb),
					new LocalCertificateSelectionCallback(localCertificateCb));
				var clientCertificates = new X509CertificateCollection { certificate };

				// SslProtocols.Tls1.2 throws an exception in Windows, see https://github.com/bgrainger/MySqlConnector/pull/101
				var sslProtocols = SslProtocols.Tls | SslProtocols.Tls11;
				if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
					sslProtocols |= SslProtocols.Tls12;

				var checkCertificateRevocation = sslMode == SslMode.VerifyFull;

				var initSsl = new PayloadData(new ArraySegment<byte>(HandshakeResponse41Packet.InitSsl(database)));
				await SendReplyAsync(initSsl, ioBehavior, cancellationToken).ConfigureAwait(false);

				try
				{
					if (ioBehavior == IOBehavior.Asynchronous)
					{
						await sslStream.AuthenticateAsClientAsync(m_hostname, clientCertificates, sslProtocols, checkCertificateRevocation).ConfigureAwait(false);
					}
					else
					{
#if NETSTANDARD1_3
						await sslStream.AuthenticateAsClientAsync(m_hostname, clientCertificates, sslProtocols, checkCertificateRevocation).ConfigureAwait(false);
#else
						sslStream.AuthenticateAsClient(m_hostname, clientCertificates, sslProtocols, checkCertificateRevocation);
#endif
					}
					var sslByteHandler = new StreamProtocolLayer(sslStream);
					m_protocolLayer.Inject(sslByteHandler);
				}
				catch (AuthenticationException ex)
				{
#if NETSTANDARD1_3
					m_tcpClient.Dispose();
#else
					m_tcpClient.Close();
#endif
					m_hostname = "";
					m_protocolLayer = null;
					m_state = State.Failed;
					m_tcpClient = null;
					throw new MySqlException("SSL Authentication Error", ex);
				}
			}

			var response = HandshakeResponse41Packet.Create(initialHandshake, userId, password, database);
			payload = new PayloadData(new ArraySegment<byte>(response));
			await SendReplyAsync(payload, ioBehavior, cancellationToken).ConfigureAwait(false);
			await ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
		}

		public async Task ResetConnectionAsync(string userId, string password, string database, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			if (ServerVersion.Version.CompareTo(ServerVersions.SupportsResetConnection) >= 0)
			{
				await SendAsync(ResetConnectionPayload.Create(), ioBehavior, cancellationToken).ConfigureAwait(false);
				var payload = await ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
				OkPayload.Create(payload);

				// the "reset connection" packet also resets the connection charset, so we need to change that back to our default
				payload = new PayloadData(new ArraySegment<byte>(PayloadUtilities.CreateEofStringPayload(CommandKind.Query, "SET NAMES utf8mb4;")));
				await SendAsync(payload, ioBehavior, cancellationToken).ConfigureAwait(false);
				payload = await ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
				OkPayload.Create(payload);
			}
			else
			{
				// optimistically hash the password with the challenge from the initial handshake (supported by MariaDB; doesn't appear to be supported by MySQL)
				var hashedPassword = AuthenticationUtility.CreateAuthenticationResponse(AuthPluginData, 0, password);
				var payload = ChangeUserPayload.Create(userId, hashedPassword, database);
				await SendAsync(payload, ioBehavior, cancellationToken).ConfigureAwait(false);
				payload = await ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
				if (payload.HeaderByte == AuthenticationMethodSwitchRequestPayload.Signature)
				{
					// if the server didn't support the hashed password; rehash with the new challenge
					var switchRequest = AuthenticationMethodSwitchRequestPayload.Create(payload);
					if (switchRequest.Name != "mysql_native_password")
						throw new NotSupportedException("Authentication method '{0}' is not supported.".FormatInvariant(switchRequest.Name));
					hashedPassword = AuthenticationUtility.CreateAuthenticationResponse(switchRequest.Data, 0, password);
					payload = new PayloadData(new ArraySegment<byte>(hashedPassword));
					await SendReplyAsync(payload, ioBehavior, cancellationToken).ConfigureAwait(false);
					payload = await ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
				}
				OkPayload.Create(payload);
			}
		}

		public async Task<bool> TryPingAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			await SendAsync(PingPayload.Create(), ioBehavior, cancellationToken).ConfigureAwait(false);
			try
			{
				var payload = await ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
				OkPayload.Create(payload);
				return true;
			}
			catch (EndOfStreamException)
			{
			}
			catch (SocketException)
			{
			}

			return false;
		}

		// Starts a new conversation with the server by sending the first packet.
		public ValueTask<int> SendAsync(PayloadData payload, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			m_protocolLayer.StartNewConversation();
			return TryAsync(m_protocolLayer.WriteAsync, payload.ArraySegment, ioBehavior, cancellationToken);
		}

		// Starts a new conversation with the server by receiving the first packet.
		public ValueTask<PayloadData> ReceiveAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			m_protocolLayer.StartNewConversation();
			return TryAsync(m_protocolLayer.ReadAsync, ioBehavior, cancellationToken);
		}

		// Continues a conversation with the server by receiving a response to a packet sent with 'Send' or 'SendReply'.
		public ValueTask<PayloadData> ReceiveReplyAsync(IOBehavior ioBehavior, CancellationToken cancellationToken) =>
			TryAsync(m_protocolLayer.ReadAsync, ioBehavior, cancellationToken);

		// Continues a conversation with the server by sending a reply to a packet received with 'Receive' or 'ReceiveReply'.
		public ValueTask<int> SendReplyAsync(PayloadData payload, IOBehavior ioBehavior, CancellationToken cancellationToken) =>
			TryAsync(m_protocolLayer.WriteAsync, payload.ArraySegment, ioBehavior, cancellationToken);

		private void VerifyConnected()
		{
			if (m_state == State.Closed)
				throw new ObjectDisposedException(nameof(MySqlSession));
			if (m_state != State.Connected)
				throw new InvalidOperationException("MySqlSession is not connected.");
		}

		private async Task<bool> OpenSocketAsync(IEnumerable<string> hostnames, int port, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			foreach (var hostname in hostnames)
			{
				IPAddress[] ipAddresses;
				try
				{
#if NETSTANDARD1_3
					// Dns.GetHostAddresses isn't available until netstandard 2.0: https://github.com/dotnet/corefx/pull/11950
					ipAddresses = await Dns.GetHostAddressesAsync(hostname).ConfigureAwait(false);
#else
					if (ioBehavior == IOBehavior.Asynchronous)
					{
						ipAddresses = await Dns.GetHostAddressesAsync(hostname).ConfigureAwait(false);
					}
					else
					{
						ipAddresses = Dns.GetHostAddresses(hostname);
					}
#endif
				}
				catch (SocketException)
				{
					// name couldn't be resolved
					continue;
				}

				// need to try IP Addresses one at a time: https://github.com/dotnet/corefx/issues/5829
				foreach (var ipAddress in ipAddresses)
				{
					TcpClient tcpClient = null;
					try
					{
						tcpClient = new TcpClient(ipAddress.AddressFamily);
						using (cancellationToken.Register(() => tcpClient?.Client?.Dispose()))
						{
							try
							{
								if (ioBehavior == IOBehavior.Asynchronous)
								{
									await tcpClient.ConnectAsync(ipAddress, port).ConfigureAwait(false);
								}
								else
								{
#if NETSTANDARD1_3
									await tcpClient.ConnectAsync(ipAddress, port).ConfigureAwait(false);
#else
									tcpClient.Connect(ipAddress, port);
#endif
								}
							}
							catch (ObjectDisposedException ex) when (cancellationToken.IsCancellationRequested)
							{
								throw new MySqlException("Connect Timeout expired.", ex);
							}
						}
					}
					catch (SocketException)
					{
						tcpClient?.Client?.Dispose();
						continue;
					}

					m_hostname = hostname;
					m_tcpClient = tcpClient;

					var socketProtocolLayer = new SocketProtocolLayer(m_tcpClient.Client);
					m_protocolLayer = new PayloadProtocolLayer(socketProtocolLayer);

					m_state = State.Connected;
					return true;
				}
			}
			return false;
		}

		private ValueTask<int> TryAsync<TArg>(Func<TArg, IOBehavior, ValueTask<int>> func, TArg arg, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			VerifyConnected();
			var task = func(arg, ioBehavior);
			if (task.IsCompletedSuccessfully)
				return task;

			return new ValueTask<int>(task.AsTask()
				.ContinueWith(TryAsyncContinuation, cancellationToken, TaskContinuationOptions.LazyCancellation | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default));
		}

		private int TryAsyncContinuation(Task<int> task)
		{
			if (task.IsFaulted)
			{
				SetFailed();
				task.GetAwaiter().GetResult();
			}
			return 0;
		}

		private ValueTask<PayloadData> TryAsync(Func<int?, ProtocolErrorBehavior, IOBehavior, ValueTask<ArraySegment<byte>>> func, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			VerifyConnected();
			var task = func(null, ProtocolErrorBehavior.Throw, ioBehavior);
			if (task.IsCompletedSuccessfully)
			{
				var payload = new PayloadData(task.Result);
				if (payload.HeaderByte != ErrorPayload.Signature)
					return new ValueTask<PayloadData>(payload);

				var exception = ErrorPayload.Create(payload).ToException();
				return ValueTaskExtensions.FromException<PayloadData>(exception);
			}

			return new ValueTask<PayloadData>(task.AsTask()
				.ContinueWith(TryAsyncContinuation, cancellationToken, TaskContinuationOptions.LazyCancellation | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default));
		}

		private PayloadData TryAsyncContinuation(Task<ArraySegment<byte>> task)
		{
			if (task.IsFaulted)
				SetFailed();
			var payload = new PayloadData(task.GetAwaiter().GetResult());
			payload.ThrowIfError();
			return payload;
		}

		private void SetFailed()
		{
			m_state = State.Failed;
		}

		private enum State
		{
			Created,
			Connected,
			Closed,
			Failed,
		}

		State m_state;
		string m_hostname;
		TcpClient m_tcpClient;
		IProtocolLayer m_protocolLayer;
	}
}
