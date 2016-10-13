﻿using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.Protocol.Serialization;
using MySql.Data.Serialization;

namespace MySql.Data.MySqlClient
{
	public sealed class MySqlCommand : DbCommand
	{
		public MySqlCommand()
			: this(null, null, null)
		{
		}

		public MySqlCommand(string commandText)
			: this(commandText, null, null)
		{
		}

		public MySqlCommand(MySqlConnection connection, MySqlTransaction transaction)
			: this(null, connection, transaction)
		{
		}

		public MySqlCommand(string commandText, MySqlConnection connection)
			: this(commandText, connection, null)
		{
		}

		public MySqlCommand(string commandText, MySqlConnection connection, MySqlTransaction transaction)
		{
			CommandText = commandText;
			DbConnection = connection;
			DbTransaction = transaction;
			m_parameterCollection = new MySqlParameterCollection();
		}

		public new MySqlParameterCollection Parameters
		{
			get
			{
				VerifyNotDisposed();
				return m_parameterCollection;
			}
		}

		public override void Cancel()
		{
			// documentation says this shouldn't throw (but just fail silently), but for now make it explicit that this doesn't work
			throw new NotSupportedException("Use the Async overloads with a CancellationToken.");
		}

		public override int ExecuteNonQuery() =>
			ExecuteNonQueryAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();

		public override object ExecuteScalar() =>
			ExecuteScalarAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();

		public override void Prepare()
		{
			// NOTE: Prepared statements in MySQL are not currently supported.
			// 1) Only a subset of statements are actually preparable by the server: http://dev.mysql.com/worklog/task/?id=2871
			// 2) Although CLIENT_MULTI_STATEMENTS is supposed to mean that the Server "Can handle multiple statements per COM_QUERY and COM_STMT_PREPARE" (https://dev.mysql.com/doc/internals/en/capability-flags.html#flag-CLIENT_MULTI_STATEMENTS),
			//    this is not actually true because "Prepared statement handles are defined to work only with strings that contain a single statement" (http://dev.mysql.com/doc/refman/5.7/en/c-api-multiple-queries.html).
		}

		public override string CommandText { get; set; }
		public override int CommandTimeout { get; set; }

		public override CommandType CommandType
		{
			get
			{
				return CommandType.Text;
			}
			set
			{
				if (value != CommandType.Text)
					throw new ArgumentException("CommandType must be Text.", nameof(value));
			}
		}

		public override bool DesignTimeVisible { get; set; }

		public override UpdateRowSource UpdatedRowSource
		{
			get { throw new NotSupportedException(); }
			set { throw new NotSupportedException(); }
		}

		public long LastInsertedId { get; internal set; }

		protected override DbConnection DbConnection { get; set; }
		protected override DbParameterCollection DbParameterCollection => m_parameterCollection;
		protected override DbTransaction DbTransaction { get; set; }

		protected override DbParameter CreateDbParameter()
		{
			VerifyNotDisposed();
			return new MySqlParameter();
		}

		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) =>
			ExecuteReaderAsync(behavior, IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();

		public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken) =>
			ExecuteNonQueryAsync(Connection.AsyncIOBehavior, cancellationToken);

		internal async Task<int> ExecuteNonQueryAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			using (var reader = (MySqlDataReader) await ExecuteReaderAsync(CommandBehavior.Default, ioBehavior, cancellationToken).ConfigureAwait(false))
			{
				do
				{
					while (await reader.ReadAsync(ioBehavior, cancellationToken).ConfigureAwait(false))
					{
					}
				} while (await reader.NextResultAsync(ioBehavior, cancellationToken).ConfigureAwait(false));
				return reader.RecordsAffected;
			}
		}

		public override Task<object> ExecuteScalarAsync(CancellationToken cancellationToken) =>
			ExecuteScalarAsync(Connection.AsyncIOBehavior, cancellationToken);

		internal async Task<object> ExecuteScalarAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			object result = null;
			using (var reader = (MySqlDataReader) await ExecuteReaderAsync(CommandBehavior.SingleResult | CommandBehavior.SingleRow, ioBehavior, cancellationToken).ConfigureAwait(false))
			{
				do
				{
					if (await reader.ReadAsync(ioBehavior, cancellationToken).ConfigureAwait(false))
						result = reader.GetValue(0);
				} while (await reader.NextResultAsync(ioBehavior, cancellationToken).ConfigureAwait(false));
			}
			return result;
		}

		protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) =>
			ExecuteReaderAsync(behavior, Connection.AsyncIOBehavior, cancellationToken);

		internal async Task<DbDataReader> ExecuteReaderAsync(CommandBehavior behavior, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			VerifyValid();
			Connection.HasActiveReader = true;

			MySqlDataReader reader = null;
			try
			{
				LastInsertedId = -1;
				var connection = (MySqlConnection) DbConnection;
				var statementPreparerOptions = StatementPreparerOptions.None;
				if (connection.AllowUserVariables)
					statementPreparerOptions |= StatementPreparerOptions.AllowUserVariables;
				if (connection.OldGuids)
					statementPreparerOptions |= StatementPreparerOptions.OldGuids;
				var preparer = new MySqlStatementPreparer(CommandText, m_parameterCollection, statementPreparerOptions);
				var payload = new PayloadData(preparer.ParseAndBindParameters());
				await Session.SendAsync(payload, ioBehavior, cancellationToken).ConfigureAwait(false);
				reader = await MySqlDataReader.CreateAsync(this, behavior, ioBehavior, cancellationToken).ConfigureAwait(false);
				return reader;
			}
			finally
			{
				if (reader == null)
				{
					// received an error from MySQL and never created an active reader
					Connection.HasActiveReader = false;
				}
			}
		}

		protected override void Dispose(bool disposing)
		{
			try
			{
				if (disposing)
				{
					m_parameterCollection = null;
				}
			}
			finally
			{
				base.Dispose(disposing);
			}
		}

		internal new MySqlConnection Connection => (MySqlConnection) DbConnection;
		private MySqlSession Session => Connection.Session;

		private void VerifyNotDisposed()
		{
			if (m_parameterCollection == null)
				throw new ObjectDisposedException(GetType().Name);
		}

		private void VerifyValid()
		{
			VerifyNotDisposed();
			if (DbConnection == null)
				throw new InvalidOperationException("Connection property must be non-null.");
			if (DbConnection.State != ConnectionState.Open && DbConnection.State != ConnectionState.Connecting)
				throw new InvalidOperationException("Connection must be Open; current state is {0}".FormatInvariant(DbConnection.State));
			if (DbTransaction != Connection.CurrentTransaction)
				throw new InvalidOperationException("The transaction associated with this command is not the connection's active transaction.");
			if (string.IsNullOrWhiteSpace(CommandText))
				throw new InvalidOperationException("CommandText must be specified");
			if (Connection.HasActiveReader)
				throw new MySqlException("There is already an open DataReader associated with this Connection which must be closed first.");
		}

		MySqlParameterCollection m_parameterCollection;
	}
}
