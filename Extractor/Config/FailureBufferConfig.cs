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
