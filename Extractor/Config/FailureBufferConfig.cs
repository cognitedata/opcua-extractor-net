/* Cognite Extractor for OPC-UA
Copyright (C) 2023 Cognite AS

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

namespace Cognite.OpcUa.Config
{
    public class FailureBufferConfig
    {
        /// <summary>
        /// Set to true to enable the failurebuffer at all.
        /// </summary>
        public bool Enabled { get; set; }
        /// <summary>
        /// If state-storage is configured, this can be used to store the ranges of points buffered in influxdb, so that
        /// they can be recovered even if the extractor goes down.
        /// </summary>
        public bool InfluxStateStore { get; set; }
        /// <summary>
        /// Use an influxdb pusher as buffer. Requires an influxdb destination to be configured.
        /// This is intended to be used if there is a local influxdb instance running.
        /// If points are received on non-historical points while the connection to CDF is down,
        /// they are read from influxdb once the connection is restored.
        /// </summary>
        public bool Influx { get; set; }
        /// <summary>
        /// Store datapoints to a binary file. There is no safety, and a bad write can corrupt the file,
        /// but it is very fast.
        /// Path to a local binary buffer file for datapoints.
        /// </summary>
        public string? DatapointPath { get; set; }
        /// <summary>
        /// Path to a local binary buffer file for events.
        /// The two buffer file paths must be different.
        /// </summary>
        public string? EventPath { get; set; }
        public long MaxBufferSize { get; set; }
    }

}
