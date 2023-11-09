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

using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;

namespace Cognite.OpcUa.Config
{
    public class StateStorageConfig : StateStoreConfig
    {
        public TimeSpanWrapper IntervalValue { get; } = new TimeSpanWrapper(false, "s", "0");
        /// <summary>
        /// Interval between each write to the buffer file, in seconds. 0 or less disables the state storage.
        /// Alternatively, use N[timeunit] where timeunit is w, d, h, m, s or ms.
        /// </summary>
        public string? Interval
        {
            get => IntervalValue.RawValue; set => IntervalValue.RawValue = value!;
        }
        /// <summary>
        /// Name of the raw table or litedb for namespace publication dates.
        /// </summary>
        public string NamespacePublicationDateStore { get; set; } = "namespace_publication_dates";
        /// <summary>
        /// Name of the raw table or litedb store for variable ranges.
        /// </summary>
        public string VariableStore { get; set; } = "variable_states";
        /// <summary>
        /// Name of the raw table or litedb store for event ranges.
        /// </summary>
        public string EventStore { get; set; } = "event_states";
        /// <summary>
        /// Name of the raw table or litedb store for influxdb failurebuffer variable ranges.
        /// </summary>
        public string InfluxVariableStore { get; set; } = "influx_variable_states";
        /// <summary>
        /// Name of the raw table or litedb store for influxdb failurebuffer event ranges.
        /// </summary>
        public string InfluxEventStore { get; set; } = "influx_event_states";
        /// <summary>
        /// Name of the raw table or litedb store for storing known object-type nodes, used for detecting deleted nodes.
        /// </summary>
        public string KnownObjectsStore { get; set; } = "known_objects";
        /// <summary>
        /// Name of the raw table or litedb store for storing known variable-type nodes, used for detecting deleted nodes.
        /// </summary>
        public string KnownVariablesStore { get; set; } = "known_variables";
        /// <summary>
        /// Name of the raw table or litedb store for storing known reference-type nodes, used for detecting deleted nodes.
        /// </summary>
        public string KnownReferencesStore { get; set; } = "known_references";
    }
}
