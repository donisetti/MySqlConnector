using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using MySql.Data.Serialization;

namespace MySql.Data.Protocol.Serialization
{
	internal class CompressionLayer : IPacketWriter, IByteReader
	{
		private readonly IByteReader m_byteReader;
		private readonly IByteWriter m_byteWriter;
		private readonly byte[] m_buffer;
		private readonly Conversation m_conversation;

		public CompressionLayer(IByteReader byteReader, IByteWriter byteWriter)
		{
			m_byteReader = byteReader;
			m_byteWriter = byteWriter;
			m_buffer = new byte[16384];
			m_conversation = new Conversation();
		}

		public ValueTask<int> WritePacketAsync(Packet packet, IOBehavior ioBehavior, FlushBehavior flushBehavior)
		{
			if (packet.SequenceNumber == 0)
				m_conversation.StartNew();
			/*
			// TODO: Don't compress small packets
			ArraySegment<byte> compressedData;
			using (var compressedStream = new MemoryStream())
			{
				using (var deflateStream = new DeflateStream(compressedStream, CompressionLevel.Optimal, leaveOpen: true))
				{
					var length = new byte[3];
					SerializationUtility.WriteUInt32((uint) packet.Contents.Count, length, 0, 3);
					deflateStream.Write(length, 0, 3);
					deflateStream.WriteByte((byte) (packet.SequenceNumber & 0xFF));
					deflateStream.Write(packet.Contents.Array, packet.Contents.Offset, packet.Contents.Count);
				}

				if (!compressedStream.TryGetBuffer(out compressedData))
					throw new InvalidOperationException("Couldn't get buffer.");
			}

			var packetLength = compressedData.Count;
			var bufferLength = packetLength + 7;
			var buffer = ArrayPool<byte>.Shared.Rent(bufferLength);
			SerializationUtility.WriteUInt32((uint) packetLength, buffer, 0, 3);
			buffer[3] = (byte) m_conversation.GetNextSequenceNumber();
			SerializationUtility.WriteUInt32((uint) packet.Contents.Count + 4, buffer, 4, 3);
			Buffer.BlockCopy(compressedData.Array, compressedData.Offset, buffer, 7, compressedData.Count);
			return m_byteWriter.WriteBytesAsync(new ArraySegment<byte>(buffer, 0, bufferLength), ioBehavior);
			*/

			// send all data uncompressed
			var packetLength = packet.Contents.Count + 4;
			var bufferLength = packetLength + 7;
			var buffer = new byte[bufferLength];
			SerializationUtility.WriteUInt32((uint) packetLength, buffer, 0, 3);
			buffer[3] = (byte) m_conversation.GetNextSequenceNumber();
			SerializationUtility.WriteUInt32(0, buffer, 4, 3);
			SerializationUtility.WriteUInt32((uint) packet.Contents.Count, buffer, 7, 3);
			buffer[10] = (byte) (packet.SequenceNumber & 0xFF);
			Buffer.BlockCopy(packet.Contents.Array, packet.Contents.Offset, buffer, 11, packet.Contents.Count);
			return m_byteWriter.WriteBytesAsync(new ArraySegment<byte>(buffer, 0, bufferLength), ioBehavior);
		}

		public ValueTask<int> ReadBytesAsync(byte[] buffer, int offset, int count, IOBehavior ioBehavior)
		{
			if (m_remainingData.Count > 0)
			{
				int bytesToRead = Math.Min(m_remainingData.Count, count);
				Buffer.BlockCopy(m_remainingData.Array, m_remainingData.Offset, buffer, offset, bytesToRead);
				m_remainingData = new ArraySegment<byte>(m_remainingData.Array, m_remainingData.Offset + bytesToRead, m_remainingData.Count - bytesToRead);
				return new ValueTask<int>(bytesToRead);
			}

			return ReadBytesAsync(0, m_buffer, 0, 7, ioBehavior)
				.ContinueWith(headerBytesRead =>
				{
					if (headerBytesRead < 7)
						return ValueTaskExtensions.FromException<int>(new EndOfStreamException("Wanted to read 7 bytes but only read {0} when reading compressed packet header".FormatInvariant(headerBytesRead)));

					var payloadLength = (int) SerializationUtility.ReadUInt32(m_buffer, 0, 3);
					int sequenceNumber = m_buffer[3]; // TODO: Check this
					var uncompressedLength = (int) SerializationUtility.ReadUInt32(m_buffer, 4, 3);

					var temp = payloadLength <= m_buffer.Length ? m_buffer : new byte[payloadLength];
					return ReadBytesAsync(0, temp, 0, payloadLength, ioBehavior)
						.ContinueWith(payloadBytesRead =>
						{
							if (payloadBytesRead < payloadLength)
								return ValueTaskExtensions.FromException<int>(new EndOfStreamException("Wanted to read {0} bytes but only read {0} when reading compressed payload".FormatInvariant(payloadLength, payloadBytesRead)));

							if (uncompressedLength == 0)
							{
								// uncompressed
								m_remainingData = new ArraySegment<byte>(temp, 0, payloadLength);
							}
							else
							{
								var uncompressedData = new byte[uncompressedLength];
								using (var compressedStream = new MemoryStream(temp, 2, payloadLength - 6)) // TODO: handle zlib format correctly
								using (var decompressingStream = new DeflateStream(compressedStream, CompressionMode.Decompress))
								{
									var bytesRead = decompressingStream.Read(uncompressedData, 0, uncompressedLength);
									m_remainingData = new ArraySegment<byte>(uncompressedData, 0, bytesRead);
								}
							}

							int bytesToRead = Math.Min(m_remainingData.Count, count);
							Buffer.BlockCopy(m_remainingData.Array, m_remainingData.Offset, buffer, offset, bytesToRead);
							m_remainingData = new ArraySegment<byte>(m_remainingData.Array, m_remainingData.Offset + bytesToRead, m_remainingData.Count - bytesToRead);
							return new ValueTask<int>(bytesToRead);
						});
				});
		}

		private ValueTask<int> ReadBytesAsync(int previousBytesRead, byte[] buffer, int offset, int count, IOBehavior ioBehavior)
		{
			return m_byteReader.ReadBytesAsync(buffer, offset, count, ioBehavior)
				.ContinueWith(bytesRead =>
				{
					if (bytesRead == 0 || bytesRead == count)
						return new ValueTask<int>(previousBytesRead + bytesRead);

					return ReadBytesAsync(previousBytesRead + bytesRead, buffer, offset + bytesRead, count - bytesRead, ioBehavior);
				});
		}

		ArraySegment<byte> m_remainingData;
	}
}
