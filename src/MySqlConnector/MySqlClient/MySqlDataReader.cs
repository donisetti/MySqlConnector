﻿using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.Protocol.Serialization;
using MySql.Data.Serialization;

namespace MySql.Data.MySqlClient
{
	public sealed class MySqlDataReader : DbDataReader
	{
		public override bool NextResult() =>
			NextResultAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();

		public override Task<bool> NextResultAsync(CancellationToken cancellationToken) =>
			NextResultAsync(m_command.Connection.AsyncIOBehavior, cancellationToken);

		internal async Task<bool> NextResultAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			VerifyNotDisposed();

			while (m_state == State.ReadingRows || m_state == State.ReadResultSetHeader)
				await ReadAsync(ioBehavior, cancellationToken).ConfigureAwait(false);

			var oldState = m_state;
			Reset();
			if (oldState == State.NoMoreData || oldState == State.None)
				return false;
			if (oldState != State.HasMoreData)
				throw new InvalidOperationException("Invalid state: {0}".FormatInvariant(oldState));

			await ReadResultSetHeaderAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
			return true;
		}

		public override bool Read()
		{
			VerifyNotDisposed();
			return ReadAsync(IOBehavior.Synchronous, CancellationToken.None).GetAwaiter().GetResult();
		}

		public override Task<bool> ReadAsync(CancellationToken cancellationToken) =>
			ReadAsync(m_command.Connection.AsyncIOBehavior, cancellationToken);

		internal Task<bool> ReadAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			VerifyNotDisposed();

			// if we've already read past the end of this resultset, Read returns false
			if (m_state == State.HasMoreData || m_state == State.NoMoreData || m_state == State.None)
				return s_falseTask;

			if (m_state != State.AlreadyReadFirstRow)
			{
				var payloadTask = m_session.ReceiveReplyAsync(ioBehavior, cancellationToken);
				if (payloadTask.IsCompletedSuccessfully)
					return ReadAsyncRemainder(payloadTask.Result) ? s_trueTask : s_falseTask;
				return ReadAsyncAwaited(payloadTask.AsTask());
			}

			m_state = State.ReadingRows;
			return s_trueTask;
		}

		private async Task<bool> ReadAsyncAwaited(Task<PayloadData> payloadTask)
		{
			var payload = await payloadTask.ConfigureAwait(false);
			return ReadAsyncRemainder(payload);
		}

		private bool ReadAsyncRemainder(PayloadData payload)
		{
			if (EofPayload.IsEof(payload))
			{
				var eof = EofPayload.Create(payload);
				m_state = eof.ServerStatus.HasFlag(ServerStatus.MoreResultsExist) ? State.HasMoreData : State.NoMoreData;
				return false;
			}

			var reader = new ByteArrayReader(payload.ArraySegment);
			for (var column = 0; column < m_dataOffsets.Length; column++)
			{
				var length = checked((int) ReadFieldLength(reader));
				m_dataLengths[column] = length == -1 ? 0 : length;
				m_dataOffsets[column] = length == -1 ? -1 : reader.Offset;
				reader.Offset += m_dataLengths[column];
			}

			m_currentRow = payload.ArraySegment.Array;

			m_state = State.ReadingRows;
			return true;
		}

		public override bool IsClosed => m_command == null;

		public override int RecordsAffected => m_recordsAffected;

		public override bool GetBoolean(int ordinal)
		{
			var value = GetValue(ordinal);
			if (value is bool)
				return (bool) value;

			if (value is sbyte)
				return (sbyte) value != 0;
			if (value is byte)
				return (byte) value != 0;
			if (value is short)
				return (short) value != 0;
			if (value is ushort)
				return (ushort) value != 0;
			if (value is int)
				return (int) value != 0;
			if (value is uint)
				return (uint) value != 0;
			if (value is long)
				return (long) value != 0;
			if (value is ulong)
				return (ulong) value != 0;
			return (bool) value;
		}

		public sbyte GetSByte(int ordinal)
		{
			return (sbyte) GetValue(ordinal);
		}

		public override byte GetByte(int ordinal)
		{
			return (byte) GetValue(ordinal);
		}

		public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
		{
			VerifyRead();

			if (m_dataOffsets[ordinal] == -1)
				throw new InvalidCastException("Column is NULL.");

			var column = m_columnDefinitions[ordinal];
			var columnType = column.ColumnType;
			if (!column.ColumnFlags.HasFlag(ColumnFlags.Binary) ||
				(columnType != ColumnType.String && columnType != ColumnType.VarString && columnType != ColumnType.TinyBlob &&
				columnType != ColumnType.Blob && columnType != ColumnType.MediumBlob && columnType != ColumnType.LongBlob))
			{
				throw new InvalidCastException("Can't convert {0} to bytes.".FormatInvariant(columnType));
			}

			if (buffer == null)
			{
				// this isn't required by the DbDataReader.GetBytes API documentation, but is what mysql-connector-net does
				// (as does SqlDataReader: http://msdn.microsoft.com/en-us/library/system.data.sqlclient.sqldatareader.getbytes.aspx)
				return m_dataLengths[ordinal];
			}

			if (bufferOffset + length > buffer.Length)
				throw new ArgumentException("bufferOffset + length cannot exceed buffer.Length", nameof(length));

			int lengthToCopy = Math.Min(m_dataLengths[ordinal] - (int) dataOffset, length);
			Buffer.BlockCopy(m_currentRow, checked((int) (m_dataOffsets[ordinal] + dataOffset)), buffer, bufferOffset, lengthToCopy);
			return lengthToCopy;
		}

		public override char GetChar(int ordinal)
		{
			return (char) GetValue(ordinal);
		}

		public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
		{
			throw new NotImplementedException();
		}

		public override Guid GetGuid(int ordinal)
		{
			var value = GetValue(ordinal);
			if (value is Guid)
				return (Guid) value;

			Guid guid;
			if (Guid.TryParse(value as string, out guid))
				return guid;

			byte[] bytes = value as byte[];
			if (bytes != null && bytes.Length == 16)
				return new Guid(bytes);

			throw new MySqlException("The value could not be converted to a GUID: {0}".FormatInvariant(value));
		}

		public override short GetInt16(int ordinal)
		{
			object value = GetValue(ordinal);
			if (value is short)
				return (short) value;

			if (value is sbyte)
				return (sbyte) value;
			if (value is byte)
				return (byte) value;
			if (value is ushort)
				return checked((short) (ushort) value);
			if (value is int)
				return checked((short) (int) value);
			if (value is uint)
				return checked((short) (uint) value);
			if (value is long)
				return checked((short) (long) value);
			if (value is ulong)
				return checked((short) (ulong) value);
			return (short) value;
		}

		public override int GetInt32(int ordinal)
		{
			object value = GetValue(ordinal);
			if (value is int)
				return (int) value;

			if (value is sbyte)
				return (sbyte) value;
			if (value is byte)
				return (byte) value;
			if (value is short)
				return (short) value;
			if (value is ushort)
				return (ushort) value;
			if (value is uint)
				return checked((int) (uint) value);
			if (value is long)
				return checked((int) (long) value);
			if (value is ulong)
				return checked((int) (ulong) value);
			return (int) value;
		}

		public override long GetInt64(int ordinal)
		{
			object value = GetValue(ordinal);
			if (value is long)
				return (long) value;

			if (value is sbyte)
				return (sbyte) value;
			if (value is byte)
				return (byte) value;
			if (value is short)
				return (short) value;
			if (value is ushort)
				return (ushort) value;
			if (value is int)
				return (int) value;
			if (value is uint)
				return (uint) value;
			if (value is ulong)
				return checked((long) (ulong) value);
			return (long) value;
		}

		public override DateTime GetDateTime(int ordinal)
		{
			return (DateTime) GetValue(ordinal);
		}

		public override string GetString(int ordinal)
		{
			return (string) GetValue(ordinal);
		}

		public override decimal GetDecimal(int ordinal)
		{
			return (decimal) GetValue(ordinal);
		}

		public override double GetDouble(int ordinal)
		{
			object value = GetValue(ordinal);
			if (value is float)
				return (float) value;
			return (double) value;
		}

		public override float GetFloat(int ordinal)
		{
			return (float) GetValue(ordinal);
		}

		public override string GetName(int ordinal)
		{
			if (m_columnDefinitions == null)
				throw new IndexOutOfRangeException("There is no current result set.");
			if (ordinal < 0 || ordinal > m_columnDefinitions.Length)
				throw new IndexOutOfRangeException("value must be between 0 and {0}".FormatInvariant(m_columnDefinitions.Length - 1));
			return m_columnDefinitions[ordinal].Name;
		}

		public override int GetValues(object[] values)
		{
			VerifyRead();
			int count = Math.Min(values.Length, m_columnDefinitions.Length);
			for (int i = 0; i < count; i++)
				values[i] = GetValue(i);
			return count;
		}

		public override bool IsDBNull(int ordinal)
		{
			VerifyRead();
			// TODO: Correct exception for invalid ordinal?
			return m_dataOffsets[ordinal] == -1;
		}

		public override int FieldCount
		{
			get
			{
				VerifyNotDisposed();
				return m_columnDefinitions?.Length ?? 0;
			}
		}

		public override object this[int ordinal] => GetValue(ordinal);

		public override object this[string name] => GetValue(GetOrdinal(name));

		public override bool HasRows
		{
			get
			{
				VerifyNotDisposed();
				if (m_state == State.ReadResultSetHeader)
				{
					if (Read())
					{
						m_state = State.AlreadyReadFirstRow;
						return true;
					}
					return false;
				}
				return m_state == State.ReadingRows;
			}
		}

		public override int GetOrdinal(string name)
		{
			VerifyHasResult();

			for (int column = 0; column < m_columnDefinitions.Length; column++)
			{
				if (m_columnDefinitions[column].Name.Equals(name, StringComparison.OrdinalIgnoreCase))
					return column;
			}

			// TODO: Correct exception
			throw new IndexOutOfRangeException("The column name '{0}' does not exist in the result set.".FormatInvariant(name));
		}

		public override string GetDataTypeName(int ordinal)
		{
			VerifyHasResult();
			if (ordinal < 0 || ordinal > m_columnDefinitions.Length)
				throw new ArgumentOutOfRangeException(nameof(ordinal), "value must be between 0 and {0}.".FormatInvariant(m_columnDefinitions.Length));

			var columnDefinition = m_columnDefinitions[ordinal];
			switch (columnDefinition.ColumnType)
			{
			case ColumnType.Tiny:
				return columnDefinition.ColumnLength == 1 ? "BOOL" : "TINYINT";

			case ColumnType.Short:
				return "SMALLINT";

			case ColumnType.Int24:
				return "MEDIUMINT";

			case ColumnType.Long:
				return "INT";

			case ColumnType.Longlong:
				return "BIGINT";

			case ColumnType.Bit:
				return "BIT";

			case ColumnType.String:
				return columnDefinition.CharacterSet == CharacterSet.Binary ? "BLOB" :
					string.Format(CultureInfo.InvariantCulture, "CHAR({0})", columnDefinition.ColumnLength / SerializationUtility.GetBytesPerCharacter(columnDefinition.CharacterSet));

			case ColumnType.VarString:
			case ColumnType.TinyBlob:
			case ColumnType.Blob:
			case ColumnType.MediumBlob:
			case ColumnType.LongBlob:
				return columnDefinition.CharacterSet == CharacterSet.Binary ? "BLOB" : "VARCHAR";

			case ColumnType.Date:
				return "DATE";

			case ColumnType.DateTime:
				return "DATETIME";

			case ColumnType.Timestamp:
				return "TIMESTAMP";

			case ColumnType.Time:
				return "TIME";

			case ColumnType.Year:
				return "YEAR";

			case ColumnType.Float:
				return "FLOAT";

			case ColumnType.Double:
				return "DOUBLE";

			case ColumnType.Decimal:
			case ColumnType.NewDecimal:
				return "DECIMAL";

			case ColumnType.Json:
				return "JSON";

			default:
				throw new NotImplementedException("GetDataTypeName for {0} is not implemented".FormatInvariant(columnDefinition.ColumnType));
			}
		}

		public override Type GetFieldType(int ordinal)
		{
			VerifyHasResult();
			if (ordinal < 0 || ordinal > m_columnDefinitions.Length)
				throw new ArgumentOutOfRangeException(nameof(ordinal), "value must be between 0 and {0}.".FormatInvariant(m_columnDefinitions.Length));

			var columnDefinition = m_columnDefinitions[ordinal];
			var isUnsigned = columnDefinition.ColumnFlags.HasFlag(ColumnFlags.Unsigned);
			switch (columnDefinition.ColumnType)
			{
			case ColumnType.Tiny:
				return columnDefinition.ColumnLength == 1 ? typeof(bool) :
					isUnsigned ? typeof(byte) : typeof(sbyte);

			case ColumnType.Int24:
			case ColumnType.Long:
				return isUnsigned ? typeof(uint) : typeof(int);

			case ColumnType.Longlong:
				return isUnsigned ? typeof(ulong) : typeof(long);

			case ColumnType.Bit:
				return typeof(ulong);

			case ColumnType.String:
				if (!Connection.OldGuids && columnDefinition.ColumnLength / SerializationUtility.GetBytesPerCharacter(columnDefinition.CharacterSet) == 36)
					return typeof(Guid);
				goto case ColumnType.VarString;

			case ColumnType.VarString:
			case ColumnType.TinyBlob:
			case ColumnType.Blob:
			case ColumnType.MediumBlob:
			case ColumnType.LongBlob:
				return columnDefinition.CharacterSet == CharacterSet.Binary ?
					(Connection.OldGuids && columnDefinition.ColumnLength == 16 ? typeof(Guid) : typeof(byte[])) :
					typeof(string);

			case ColumnType.Json:
				return typeof(string);

			case ColumnType.Short:
				return isUnsigned ? typeof(ushort) : typeof(short);

			case ColumnType.Date:
			case ColumnType.DateTime:
			case ColumnType.Timestamp:
				return typeof(DateTime);

			case ColumnType.Time:
				return typeof(TimeSpan);

			case ColumnType.Year:
				return typeof(int);

			case ColumnType.Float:
				return typeof(float);

			case ColumnType.Double:
				return typeof(double);

			case ColumnType.Decimal:
			case ColumnType.NewDecimal:
				return typeof(decimal);

			default:
				throw new NotImplementedException("GetFieldType for {0} is not implemented".FormatInvariant(columnDefinition.ColumnType));
			}
		}

		public override object GetValue(int ordinal)
		{
			VerifyRead();
			if (ordinal < 0 || ordinal > m_columnDefinitions.Length)
				throw new ArgumentOutOfRangeException(nameof(ordinal), "value must be between 0 and {0}.".FormatInvariant(m_columnDefinitions.Length));

			if (m_dataOffsets[ordinal] == -1)
				return DBNull.Value;

			var data = new ArraySegment<byte>(m_currentRow, m_dataOffsets[ordinal], m_dataLengths[ordinal]);
			var columnDefinition = m_columnDefinitions[ordinal];
			var isUnsigned = columnDefinition.ColumnFlags.HasFlag(ColumnFlags.Unsigned);
			switch (columnDefinition.ColumnType)
			{
			case ColumnType.Tiny:
				var value = int.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture);
				if (columnDefinition.ColumnLength == 1)
					return value != 0;
				return isUnsigned ? (object) (byte) value : (sbyte) value;

			case ColumnType.Int24:
			case ColumnType.Long:
				return isUnsigned ? (object) uint.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture) :
					int.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture);

			case ColumnType.Longlong:
				return isUnsigned ? (object) ulong.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture) :
					long.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture);

			case ColumnType.Bit:
				// BIT column is transmitted as MSB byte array
				ulong bitValue = 0;
				for (int i = 0; i < m_dataLengths[ordinal]; i++)
					bitValue = bitValue * 256 + m_currentRow[m_dataOffsets[ordinal] + i];
				return bitValue;

			case ColumnType.String:
				if (!Connection.OldGuids && columnDefinition.ColumnLength / SerializationUtility.GetBytesPerCharacter(columnDefinition.CharacterSet) == 36)
					return Guid.Parse(Encoding.UTF8.GetString(data));
				goto case ColumnType.VarString;

			case ColumnType.VarString:
			case ColumnType.VarChar:
			case ColumnType.TinyBlob:
			case ColumnType.Blob:
			case ColumnType.MediumBlob:
			case ColumnType.LongBlob:
				if (columnDefinition.CharacterSet == CharacterSet.Binary)
				{
					var result = new byte[m_dataLengths[ordinal]];
					Buffer.BlockCopy(m_currentRow, m_dataOffsets[ordinal], result, 0, result.Length);
					return Connection.OldGuids && columnDefinition.ColumnLength == 16 ? (object) new Guid(result) : result;
				}
				return Encoding.UTF8.GetString(data);

			case ColumnType.Json:
				return Encoding.UTF8.GetString(data);

			case ColumnType.Short:
				return isUnsigned ? (object) ushort.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture) :
					short.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture);

			case ColumnType.Date:
			case ColumnType.DateTime:
			case ColumnType.Timestamp:
				return ParseDateTime(data);

			case ColumnType.Time:
				return ParseTimeSpan(data);

			case ColumnType.Year:
				return int.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture);

			case ColumnType.Float:
				return float.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture);

			case ColumnType.Double:
				return double.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture);

			case ColumnType.Decimal:
			case ColumnType.NewDecimal:
				return decimal.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture);

			default:
				throw new NotImplementedException("Reading {0} not implemented".FormatInvariant(columnDefinition.ColumnType));
			}
		}

		public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

		public override int Depth
		{
			get { throw new NotSupportedException(); }
		}

		protected override DbDataReader GetDbDataReader(int ordinal)
		{
			throw new NotSupportedException();
		}

		public override int VisibleFieldCount => FieldCount;

#if NET45
		public override DataTable GetSchemaTable()
		{
			throw new NotSupportedException();
		}

		public override void Close()
		{
			DoClose();
		}
#endif

		protected override void Dispose(bool disposing)
		{
			try
			{
				if (disposing)
					DoClose();
			}
			finally
			{
				base.Dispose(disposing);
			}
		}

		private void DoClose()
		{
			if (m_command != null)
			{
				while (NextResult())
				{
				}

				Reset();
				m_session = null;

				var connection = m_command.Connection;
				connection.HasActiveReader = false;
				if (m_behavior.HasFlag(CommandBehavior.CloseConnection))
				{
					m_command.Dispose();
					connection.Close();
				}
				m_command = null;
			}
		}

		internal static async Task<MySqlDataReader> CreateAsync(MySqlCommand command, CommandBehavior behavior, IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			var dataReader = new MySqlDataReader(command, behavior);
			await dataReader.ReadResultSetHeaderAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
			return dataReader;
		}

		internal DateTime ParseDateTime(ArraySegment<byte> value)
		{
			var parts = Encoding.UTF8.GetString(value).Split('-', ' ', ':', '.');

			var year = int.Parse(parts[0], CultureInfo.InvariantCulture);
			var month = int.Parse(parts[1], CultureInfo.InvariantCulture);
			var day = int.Parse(parts[2], CultureInfo.InvariantCulture);

			if (year == 0 && month == 0 && day == 0)
			{
				if (Connection.ConvertZeroDateTime)
					return DateTime.MinValue;
				throw new InvalidCastException("Unable to convert MySQL date/time to System.DateTime.");
			}

			if (parts.Length == 3)
				return new DateTime(year, month, day);

			var hour = int.Parse(parts[3], CultureInfo.InvariantCulture);
			var minute = int.Parse(parts[4], CultureInfo.InvariantCulture);
			var second = int.Parse(parts[5], CultureInfo.InvariantCulture);
			if (parts.Length == 6)
				return new DateTime(year, month, day, hour, minute, second);

			var microseconds = int.Parse(parts[6] + new string('0', 6 - parts[6].Length), CultureInfo.InvariantCulture);
			return new DateTime(year, month, day, hour, minute, second, microseconds / 1000).AddTicks(microseconds % 1000 * 10);
		}

		internal static TimeSpan ParseTimeSpan(ArraySegment<byte> value)
		{
			var parts = Encoding.UTF8.GetString(value).Split(':', '.');

			var hours = int.Parse(parts[0], CultureInfo.InvariantCulture);
			var minutes = int.Parse(parts[1], CultureInfo.InvariantCulture);
			if (hours < 0)
				minutes = -minutes;
			var seconds = int.Parse(parts[2], CultureInfo.InvariantCulture);
			if (hours < 0)
				seconds = -seconds;
			if (parts.Length == 3)
				return new TimeSpan(hours, minutes, seconds);

			var microseconds = int.Parse(parts[3] + new string('0', 6 - parts[3].Length), CultureInfo.InvariantCulture);
			if (hours < 0)
				microseconds = -microseconds;
			return new TimeSpan(0, hours, minutes, seconds, microseconds / 1000) + TimeSpan.FromTicks(microseconds % 1000 * 10);
		}

		private MySqlDataReader(MySqlCommand command, CommandBehavior behavior)
		{
			m_command = command;
			m_session = Connection.Session;
			m_behavior = behavior;
		}

		private MySqlConnection Connection => m_command.Connection;

		private async Task ReadResultSetHeaderAsync(IOBehavior ioBehavior, CancellationToken cancellationToken)
		{
			while (true)
			{
				var payload = await m_session.ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);

				var firstByte = payload.HeaderByte;
				if (firstByte == OkPayload.Signature)
				{
					var ok = OkPayload.Create(payload);
					m_recordsAffected += ok.AffectedRowCount;
					m_command.LastInsertedId = ok.LastInsertId;
					m_columnDefinitions = null;
					m_state = ok.ServerStatus.HasFlag(ServerStatus.MoreResultsExist) ? State.HasMoreData : State.NoMoreData;
					if (m_state == State.NoMoreData)
						break;
				}
				else if (firstByte == 0xFB)
				{
					throw new NotSupportedException("Don't support LOCAL_INFILE_Request");
				}
				else
				{
					var reader = new ByteArrayReader(payload.ArraySegment);
					var columnCount = (int) reader.ReadLengthEncodedInteger();
					m_columnDefinitions = new ColumnDefinitionPayload[columnCount];
					m_dataOffsets = new int[columnCount];
					m_dataLengths = new int[columnCount];

					for (var column = 0; column < m_columnDefinitions.Length; column++)
					{
						payload = await m_session.ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
						m_columnDefinitions[column] = ColumnDefinitionPayload.Create(payload);
					}

					payload = await m_session.ReceiveReplyAsync(ioBehavior, cancellationToken).ConfigureAwait(false);
					EofPayload.Create(payload);

					m_command.LastInsertedId = -1;
					m_state = State.ReadResultSetHeader;
					break;
				}
			}
		}

		private void Reset()
		{
			m_columnDefinitions = null;
			m_dataOffsets = null;
			m_dataLengths = null;
			m_state = State.None;
		}

		private void VerifyHasResult()
		{
			VerifyNotDisposed();
			if (m_state != State.ReadResultSetHeader && m_state != State.ReadingRows)
				throw new InvalidOperationException("There is no current result set.");
		}

		private void VerifyRead()
		{
			VerifyHasResult();
			if (m_state != State.ReadingRows)
				throw new InvalidOperationException("Read must be called first.");
		}

		private void VerifyNotDisposed()
		{
			if (m_command == null)
				throw new ObjectDisposedException(GetType().Name);
		}

		private static long ReadFieldLength(ByteArrayReader reader)
		{
			var leadByte = reader.ReadByte();
			switch (leadByte)
			{
			case 0xFB:
				return -1;
			case 0xFC:
				return reader.ReadFixedLengthUInt32(2);
			case 0xFD:
				return reader.ReadFixedLengthUInt32(3);
			case 0xFE:
				return checked((long) reader.ReadFixedLengthUInt64(8));
			default:
				return leadByte;
			}
		}

		private enum State
		{
			None,
			ReadResultSetHeader,
			AlreadyReadFirstRow,
			ReadingRows,
			HasMoreData,
			NoMoreData,
		}

		static readonly Task<bool> s_falseTask = Task.FromResult(false);
		static readonly Task<bool> s_trueTask = Task.FromResult(true);

		MySqlCommand m_command;
		MySqlSession m_session;
		State m_state;
		readonly CommandBehavior m_behavior;
		int m_recordsAffected;
		ColumnDefinitionPayload[] m_columnDefinitions;
		int[] m_dataOffsets;
		int[] m_dataLengths;
		byte[] m_currentRow;
	}
}
