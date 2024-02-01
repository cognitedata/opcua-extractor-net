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
using Cognite.OpcUa.History;
using Opc.Ua;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text.RegularExpressions;

namespace Cognite.OpcUa.Config
{
    public class DataSubscriptionConfig
    {
        /// <summary>
        /// What changes to a variable trigger an update.
        /// One of Status, StatusValue, or StatusValueTimestamp.
        /// </summary>
        [DefaultValue(DataChangeTrigger.StatusValue)]
        public DataChangeTrigger Trigger { get => filter.Trigger; set => filter.Trigger = value; }
        /// <summary>
        /// Deadband for numeric values.
        /// One of None, Absolute, or Percent.
        /// </summary>
        [DefaultValue(DeadbandType.None)]
        public DeadbandType DeadbandType { get => (DeadbandType)filter.DeadbandType; set => filter.DeadbandType = (uint)value; }
        /// <summary>
        /// Value of deadband.
        /// </summary>
        public double DeadbandValue { get => filter.DeadbandValue; set => filter.DeadbandValue = value; }
        private readonly DataChangeFilter filter = new DataChangeFilter()
        {
            Trigger = DataChangeTrigger.StatusValue,
            DeadbandType = (uint)DeadbandType.None,
            DeadbandValue = 0.0
        };
        public DataChangeFilter Filter => filter;
    }
    public class SubscriptionInstanceConfig
    {
        /// <summary>
        /// Modify the DataChangeFilter used for datapoint subscriptions. See OPC-UA reference part 4 7.17.2 for details.
        /// These are just passed to the server, they have no effect on extractor behavior.
        /// Filters are applied to all nodes, but deadband should only affect some, according to the standard.
        /// </summary>
        public DataSubscriptionConfig? DataChangeFilter { get; set; }
        /// <summary>
        /// Requested sample interval per variable on the server.
        /// This is how often the extractor requests the server sample changes to values.
        /// The server has no obligation to use this value, or to use sampling at all,
        /// but on compliant servers this sets the lowest rate of changes.
        /// </summary>
        [DefaultValue(100)]
        public int SamplingInterval { get; set; } = 100;
        /// <summary>
        /// Requested length of queue for each variable on the server.
        /// </summary>
        [DefaultValue(100)]
        public int QueueLength { get; set; } = 100;
    }

    public class SubscriptionConfig : SubscriptionInstanceConfig
    {
        /// <summary>
        /// Enable subscriptions on data-points.
        /// </summary>
        public bool DataPoints { get; set; } = true;
        /// <summary>
        /// Enable subscriptions on events. Requires events.enabled to be set to true.
        /// </summary>
        public bool Events { get; set; } = true;
        /// <summary>
        /// Ignore the access level parameter for history and datapoints.
        /// This means using the "Historizing" parameter for history, and subscribing to all timeseries, independent of AccessLevel.
        /// </summary>
        public bool IgnoreAccessLevel { get; set; }
        /// <summary>
        /// Log bad subscription datapoints
        /// </summary>
        public bool LogBadValues { get; set; } = true;
        /// <summary>
        /// The number of empty publish requests before the server should remove the subscription entirely.
        /// Note that the extractor will detect this and recreate the subscription.
        /// This shall be at least 3 * KeepAliveCount.
        /// </summary>
        public uint LifetimeCount { get; set; } = 1000;
        /// <summary>
        /// The number of empty publish requests before a keep-alive message is sent from the server.
        /// </summary>
        public uint KeepAliveCount { get; set; } = 10;
        /// <summary>
        /// Recreate subscriptions that have stopped publishing. True by default.
        /// </summary>
        public bool RecreateStoppedSubscriptions { get; set; } = true;

        /// <summary>
        /// Optional grace period for recreating stopped subscriptions.
        /// Defaults to 8 * Publishing interval
        /// </summary>
        public string RecreateSubscriptionGracePeriod
        {
            get => RecreateSubscriptionGracePeriodValue.RawValue; set => RecreateSubscriptionGracePeriodValue.RawValue = value!;
        }
        public TimeSpanWrapper RecreateSubscriptionGracePeriodValue { get; } = new TimeSpanWrapper(true, "s", "-1");

        /// <summary>
        /// List of alternative subscription configurations.
        /// The first match will be applied, or the top level if none match.
        /// </summary>
        public IEnumerable<FilteredSubscriptionConfig>? AlternativeConfigs { get; set; }

        public SubscriptionInstanceConfig GetMatchingConfig(UAHistoryExtractionState state)
        {
            if (AlternativeConfigs == null) return this;
            foreach (var config in AlternativeConfigs)
            {
                if (config.Filter == null || config.Filter.IsMatch(state)) return config;
            }
            return this;
        }
    }

    public class FilteredSubscriptionConfig : SubscriptionInstanceConfig
    {
        /// <summary>
        /// Filter required to match in order to apply this config.
        /// </summary>
        public SubscriptionConfigFilter? Filter { get; set; }
    }



    public class SubscriptionConfigFilter
    {
        private string? id;
        private Regex? idRegex;
        public string? Id
        {
            get => id; set
            {
                id = value;
                idRegex = new Regex(value, RegexOptions.Compiled);
            }
        }
        private string? dataType;
        private Regex? dataTypeRegex;
        public string? DataType
        {
            get => dataType; set
            {
                dataType = value;
                dataTypeRegex = new Regex(value, RegexOptions.Compiled);
            }
        }

        public bool? IsEventState { get; set; }

        public bool IsMatch(UAHistoryExtractionState state)
        {
            if (idRegex != null && !idRegex.IsMatch(state.Id)) return false;
            if (dataTypeRegex != null)
            {
                if (state is not VariableExtractionState varState) return false;
                if (!dataTypeRegex.IsMatch(varState.DataType.ToString())) return false;
            }
            if (IsEventState != null)
            {
                if (IsEventState.Value && state is not EventExtractionState) return false;
                if (!IsEventState.Value && state is not VariableExtractionState) return false;
            }
            return true;
        }
    }
}
