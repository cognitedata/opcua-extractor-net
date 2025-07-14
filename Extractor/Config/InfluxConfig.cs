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

using System.ComponentModel;
using System.ComponentModel.DataAnnotations;

namespace Cognite.OpcUa.Config
{
    public class InfluxPusherConfig : IPusherConfig
    {
        /// <summary>
        /// Set to true to enable this destination
        /// </summary>
        public bool Enabled { get; set; }

        /// <summary>
        /// Host URI, ex localhost:8086
        /// </summary>
        [Required]
        public string? Host { get; set; }
        /// <summary>
        /// Username to use for the influxdb server.
        /// </summary>
        public string? Username { get; set; }
        /// <summary>
        /// Password to use for the influxdb server.
        /// </summary>
        public string? Password { get; set; }
        /// <summary>
        /// Database to connect to on the influxdb server.
        /// </summary>
        [Required]
        public string? Database { get; set; }
        /// <summary>
        /// Max number of points to send in each request to influx.
        /// </summary>
        [DefaultValue(100_000)]
        public int PointChunkSize { get; set; } = 100000;
        /// <summary>
        /// DEPRECATED. Debug mode, if true, Extractor will not push to target.
        /// </summary>
        public bool Debug { get; set; }
        /// <summary>
        /// Whether to read start/end-points on startup, where possible. At least one pusher should be able to do this,
        /// or the state store should be enabled,
        /// otherwise back/frontfill will run for the entire history every restart.
        /// </summary>
        public bool ReadExtractedRanges { get; set; } = true;
        /// <summary>
        /// Whether to read start/end-points for events on startup, where possible. At least one pusher should be able to do this,
        /// or the state store should be enabled,
        /// otherwise back/frontfill will run for the entire history every restart.
        /// </summary>
        public bool ReadExtractedEventRanges { get; set; } = true;
        /// <summary>
        /// Replace all instances of NaN or Infinity with this floating point number. If left empty, ignore instead.
        /// </summary>
        public double? NonFiniteReplacement
        {
            get => nonFiniteReplacement;
            set
            {
                if (value == null) return;
                nonFiniteReplacement = double.IsFinite(value.Value) ? value : null;
            }
        }
        private double? nonFiniteReplacement;
    }
}
