/* Cognite Extractor for OPC-UA
Copyright (C) 2021 Cognite AS

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. */

using Cognite.Extensions;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;

namespace Cognite.OpcUa.Types
{
    /// <summary>
    /// Represents a single value at specified timestamp
    /// </summary>
    public class UADataPoint
    {
        public DateTime Timestamp { get; }
        public string Id { get; }
        public double? DoubleValue { get; }
        public string? StringValue { get; }
        [MemberNotNullWhen(false, nameof(DoubleValue))]
        [MemberNotNullWhen(true, nameof(StringValue))]
#pragma warning disable CS8775 // Member must have a non-null value when exiting in some condition. - Implicit due to constructors.
        public bool IsString => !DoubleValue.HasValue;
#pragma warning restore CS8775 // Member must have a non-null value when exiting in some condition.
        /// <param name="timestamp">Timestamp in ms since epoch</param>
        /// <param name="id">Converted id of node this belongs to, equal to externalId of timeseries in CDF</param>
        /// <param name="value">Value to set</param>
        public UADataPoint(DateTime timestamp, string id, double value)
        {
            Timestamp = timestamp;
            Id = id;
            DoubleValue = value;
        }
        /// <param name="timestamp">Timestamp in ms since epoch</param>
        /// <param name="id">Converted id of node this belongs to, equal to externalId of timeseries in CDF</param>
        /// <param name="value">Value to set</param>
        public UADataPoint(DateTime timestamp, string id, string value)
        {
            Timestamp = timestamp;
            Id = id;
            StringValue = value;
        }
        /// <summary>
        /// Copy given datapoint with given replacement value
        /// </summary>
        /// <param name="other">Datapoint to copy</param>
        /// <param name="replacement">Replacement value</param>
        public UADataPoint(UADataPoint other, string replacement)
        {
            Timestamp = other.Timestamp;
            Id = other.Id;
            StringValue = replacement;
        }
        /// <summary>
        /// Copy given datapoint with given replacement value
        /// </summary>
        /// <param name="other">Datapoint to copy</param>
        /// <param name="replacement">Replacement value</param>
        public UADataPoint(UADataPoint other, double replacement)
        {
            Timestamp = other.Timestamp;
            Id = other.Id;
            DoubleValue = replacement;
        }
        /// <summary>
        /// Converts datapoint into an array of bytes which may be written to file.
        /// The structure is as follows: ushort size | unknown encoding string externalid | double value | long timestamp
        /// </summary>
        /// <remarks>
        /// This will obviously break if the system encoding changes somehow, it is not intended as any kind of cross-system storage.
        /// Convert the datapoint into an array of bytes. The string is converted directly, as the system encoding is unknown, but can
        /// be assumed to be constant.
        /// </remarks>
        /// <returns>Array of bytes</returns>
        public byte[] ToStorableBytes()
        {
            var bytes = new List<byte>(16);
            bytes.AddRange(CogniteUtils.StringToStorable(Id));
            bytes.AddRange(BitConverter.GetBytes(Timestamp.ToBinary()));
            bytes.AddRange(BitConverter.GetBytes(IsString));

            if (IsString)
            {
                bytes.AddRange(CogniteUtils.StringToStorable(StringValue));
            }
            else
            {
                bytes.AddRange(BitConverter.GetBytes(DoubleValue.Value));
            }

            return bytes.ToArray();
        }
        /// <summary>
        /// Initializes BufferedDataPoint from array of bytes, array should not contain the short size, which is just used to know how much
        /// to read at a time.
        /// </summary>
        /// <param name="stream">Stream to read bytes from</param>
        public static UADataPoint? FromStream(Stream stream)
        {
            string id = CogniteUtils.StringFromStream(stream);
            if (id == null) return null;
            var buffer = new byte[sizeof(long)];
            if (stream.Read(buffer, 0, sizeof(long)) < sizeof(long)) return null;
            DateTime ts = DateTime.FromBinary(BitConverter.ToInt64(buffer, 0));
            if (stream.Read(buffer, 0, sizeof(bool)) < sizeof(bool)) return null;
            bool isstr = BitConverter.ToBoolean(buffer, 0);
            if (isstr)
            {
                var value = CogniteUtils.StringFromStream(stream);
                return new UADataPoint(ts, id, value);
            }
            else
            {
                if (stream.Read(buffer, 0, sizeof(double)) < sizeof(double)) return null;
                var value = BitConverter.ToDouble(buffer, 0);
                return new UADataPoint(ts, id, value);
            }
        }
        public override string ToString()
        {
            return $"Update timeseries {Id} to {(IsString ? "\"" + StringValue + "\"" : DoubleValue.Value.ToString(CultureInfo.InvariantCulture))}" +
                   $" at {Timestamp.ToString(CultureInfo.InvariantCulture)}";
        }
    }
}
