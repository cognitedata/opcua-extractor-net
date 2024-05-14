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
using CogniteSdk;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using StatusCode = Opc.Ua.StatusCode;

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

        public bool IsString { get; }

        public StatusCode Status { get; }
        /// <param name="timestamp">Timestamp in ms since epoch</param>
        /// <param name="id">Converted id of node this belongs to, equal to externalId of timeseries in CDF</param>
        /// <param name="value">Value to set</param>
        public UADataPoint(DateTime timestamp, string id, double value, StatusCode status)
        {
            Timestamp = timestamp;
            Id = id;
            DoubleValue = value;
            Status = status;
            IsString = false;
        }
        /// <param name="timestamp">Timestamp in ms since epoch</param>
        /// <param name="id">Converted id of node this belongs to, equal to externalId of timeseries in CDF</param>
        /// <param name="value">Value to set</param>
        public UADataPoint(DateTime timestamp, string id, string? value, StatusCode status)
        {
            Timestamp = timestamp;
            Id = id;
            StringValue = value;
            Status = status;
            IsString = true;
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
            Status = other.Status;
            IsString = true;
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
            Status = other.Status;
            IsString = false;
        }

        /// <summary>
        /// Create a data point with null value.
        /// </summary>
        /// <param name="timestamp">Timestamp</param>
        /// <param name="id">Converted id of node this belongs to</param>
        /// <param name="isString">Whether this datapoint is for a string data type</param>
        /// <param name="status">Status code</param>
        public UADataPoint(DateTime timestamp, string id, bool isString, StatusCode status)
        {
            Timestamp = timestamp;
            Id = id;
            IsString = isString;
            Status = status;
        }


        public Datapoint? ToCDFDataPoint(bool includeStatusCodes, ILogger logger)
        {
            Extensions.StatusCode? status = null;
            if (includeStatusCodes)
            {
                var res = Extensions.StatusCode.TryCreate(Status.Code, out status);
                if (res != null)
                {
                    logger.LogWarning("Invalid status code, skipping data point: {Status}", res);
                    return null;
                }
            }

            if (!IsString)
            {
                if (DoubleValue.HasValue)
                {
                    return new Datapoint(Timestamp, DoubleValue.Value, status);
                }
                else
                {
                    return new Datapoint(Timestamp, false, status);
                }
            }
            else
            {
                return new Datapoint(Timestamp, StringValue);
            }
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
            var bytes = new List<byte>(20);
            bytes.AddRange(CogniteUtils.StringToStorable(Id));
            bytes.AddRange(BitConverter.GetBytes(Timestamp.ToBinary()));
            bytes.AddRange(BitConverter.GetBytes(IsString));
            bytes.AddRange(BitConverter.GetBytes(Status.Code));

            if (IsString)
            {
                bytes.AddRange(CogniteUtils.StringToStorable(StringValue));
            }
            else
            {
                if (DoubleValue.HasValue)
                {
                    bytes.AddRange(BitConverter.GetBytes(true));
                    bytes.AddRange(BitConverter.GetBytes(DoubleValue.Value));
                }
                else
                {
                    bytes.AddRange(BitConverter.GetBytes(false));
                }
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
            string? id = CogniteUtils.StringFromStream(stream);
            if (id == null) return null;
            var buffer = new byte[sizeof(long)];
            if (stream.Read(buffer, 0, sizeof(long)) < sizeof(long)) return null;
            DateTime ts = DateTime.FromBinary(BitConverter.ToInt64(buffer, 0));

            if (stream.Read(buffer, 0, sizeof(bool)) < sizeof(bool)) return null;
            bool isstr = BitConverter.ToBoolean(buffer, 0);

            if (stream.Read(buffer, 0, sizeof(uint)) < sizeof(uint)) return null;
            var status = new StatusCode(BitConverter.ToUInt32(buffer, 0));

            if (isstr)
            {
                var value = CogniteUtils.StringFromStream(stream);
                return new UADataPoint(ts, id, value, status);
            }
            else
            {
                if (stream.Read(buffer, 0, sizeof(bool)) < sizeof(bool)) return null;
                var hasValue = BitConverter.ToBoolean(buffer, 0);

                if (hasValue)
                {
                    if (stream.Read(buffer, 0, sizeof(double)) < sizeof(double)) return null;
                    var value = BitConverter.ToDouble(buffer, 0);
                    return new UADataPoint(ts, id, value, status);
                }
                else
                {
                    return new UADataPoint(ts, id, false, status);
                }

            }
        }
        public override string ToString()
        {
            var builder = new StringBuilder();

            builder.AppendFormat("Update timeseries {0} to ", Id);

            if (IsString)
            {
                builder.AppendFormat("\"{0}\"", StringValue);
            }
            else
            {
                builder.AppendFormat("{0}", DoubleValue);
            }

            builder.AppendFormat(" at {0}", Timestamp.ToString(CultureInfo.InvariantCulture));
            builder.AppendFormat(" with status {0}", StatusCode.LookupSymbolicId(Status.Code));

            return builder.ToString();
        }
    }
}
