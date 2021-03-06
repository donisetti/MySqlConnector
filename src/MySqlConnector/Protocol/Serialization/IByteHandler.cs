﻿using System;
using System.Threading.Tasks;
using MySql.Data.Serialization;

namespace MySql.Data.Protocol.Serialization
{
	internal interface IByteHandler
	{
		ValueTask<int> ReadBytesAsync(byte[] buffer, int offset, int count, IOBehavior ioBehavior);
		ValueTask<int> WriteBytesAsync(ArraySegment<byte> payload, IOBehavior ioBehavior);
	}
}
