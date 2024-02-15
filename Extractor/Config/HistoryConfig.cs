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

using System;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using Cognite.Extractor.Common;

namespace Cognite.OpcUa.Config
{
    public class HistoryConfig
    {
        /// <summary>
        /// Enable/disable history synchronization from the OPC-UA server to CDF.
        /// This is a master switch covering both events and data
        /// </summary>
        public bool Enabled { get; set; }
        /// <summary>
        /// Enable or disable data history on nodes with history. "Enabled" must be true.
        /// By default nodes with AccessLevel ReadHistory are read.
        /// </summary>
        [DefaultValue(true)]
        public bool Data { get; set; } = true;
        /// <summary>
        /// Enable/disable backfill behavior. If this is false, data is read using frontfill only. (Pre 1.1 behavior)
        /// This applies to both datapoints and events.
        /// Backfill means that points are read backwards from the earliest known point. This means that
        /// when reading new variables, the most recent data is discovered first.
        /// </summary>
        public bool Backfill { get; set; }
        /// <summary>
        /// True to require Historizing to be set on timeseries to read history.
        /// Historizing means that the node writes history based on values written to it.
        /// </summary>
        public bool RequireHistorizing { get; set; }
        /// <summary>
        /// Max number of datapoints per variable for each history read request, 0 for server specified
        /// </summary>
        [DefaultValue(1000)]
        [Range(0, 100_000)]
        public int DataChunk { get => dataChunk; set => dataChunk = Math.Max(0, value); }
        private int dataChunk = 1000;
        /// <summary>
        /// Max number of simultaneous nodes per historyRead request for datapoints.
        /// </summary>
        [DefaultValue(100)]
        [Range(1, 10_000)]
        public int DataNodesChunk { get => dataNodesChunk; set => dataNodesChunk = Math.Max(1, value); }
        private int dataNodesChunk = 100;
        /// <summary>
        /// Maximum number of events per emitter for each per history read request, 0 for server specified.
        /// </summary>
        [DefaultValue(1000)]
        [Range(0, 100_000)]
        public int EventChunk { get => eventChunk; set => eventChunk = Math.Max(0, value); }
        private int eventChunk = 1000;
        /// <summary>
        /// Maximum number of simultaneous nodes per historyRead request for events.
        /// </summary>
        [DefaultValue(100)]
        [Range(1, 10_000)]
        public int EventNodesChunk { get => eventNodesChunk; set => eventNodesChunk = Math.Max(1, value); }
        private int eventNodesChunk = 100;

        public TimeSpanWrapper MaxReadLengthValue { get; } = new TimeSpanWrapper(true, "s", "0");
        /// <summary>
        /// Maximum length of each read of history, in seconds.
        /// If this is set greater than zero, history will be read in chunks of maximum this size, until the end.
        /// This can potentially take a very long time if end-time is much larger than start-time.
        /// Alternatively, use N[timeunit] where timeunit is w, d, h, m, s or ms.
        /// </summary>
        public string? MaxReadLength { get => MaxReadLengthValue.RawValue; set => MaxReadLengthValue.RawValue = value!; }
        /// <summary>
        /// The earliest timestamp to be read from history on the OPC-UA server, in milliseconds since 1/1/1970.
        /// Alternatively, use syntax N[timeunit](-ago) where timeunit is w, d, h, m, s or ms. In past if -ago is added,
        /// future if not.
        /// </summary>
        public string? StartTime { get; set; }
        /// <summary>
        /// Timestamp to be considered the end of forward history. Only relevant if max-read-length is set.
        /// In milliseconds since 1/1/1970. Default is current time, if this is null.
        /// Alternatively, use syntax N[timeunit](-ago) where timeunit is w, d, h, m, s or ms. In past if -ago is added,
        /// future if not.
        /// </summary>
        public string? EndTime { get; set; }
        public TimeSpanWrapper GranularityValue { get; } = new TimeSpanWrapper(true, "s", "600");
        /// <summary>
        /// Granularity to use when doing historyRead, in seconds. Nodes with last known timestamp within this range of eachother will
        /// be read together. Should not be smaller than usual average update rate
        /// Leave at 0 to always read a single node each time.
        /// Alternatively, use N[timeunit] where timeunit is w, d, h, m, s or ms.
        /// </summary>
        [DefaultValue("600")]
        public string? Granularity { get => GranularityValue.RawValue; set => GranularityValue.RawValue = value!; }

        /// <summary>
        /// Set to true to attempt to read history without using continationPoints, instead using the Time of events, and
        /// SourceTimestamp of datapoints to incrementally change the start time of the request until no points are returned.
        /// </summary>
        public bool IgnoreContinuationPoints { get; set; }

        public CronTimeSpanWrapper RestartPeriodValue { get; } = new CronTimeSpanWrapper(false, false, "s", "0");
        /// <summary>
        /// Time in seconds to wait between each restart of history. Setting this too low may impact performance.
        /// Leave at 0 to disable periodic restarts.
        /// Alternatively, use N[timeunit] where timeunit is w, d, h, m, s or ms.
        /// You may also use a cron expression on the form "[minute] [hour] [day of month] [month] [day of week]"
        /// </summary>
        public string? RestartPeriod
        {
            get => RestartPeriodValue.RawValue; set => RestartPeriodValue.RawValue = value!;
        }
        /// <summary>
        /// Configuration for throttling history.
        /// </summary>
        public ContinuationPointThrottlingConfig Throttling
        {
            get => throttling; set => throttling = value ?? throttling;
        }
        private ContinuationPointThrottlingConfig throttling = new ContinuationPointThrottlingConfig();
        /// <summary>
        /// Log bad history datapoints, count per read at debug and each datapoint at verbose.
        /// </summary>
        [DefaultValue(true)]
        public bool LogBadValues { get; set; } = true;

        /// <summary>
        /// Threshold for the percentage of read operations failed before the run is considered erroneous. 
        /// Example: 10.0 -> History read operation would consider the run as failed if more that %10 of read operations fail.
        /// </summary>
        [DefaultValue(10.0)]
        public double ErrorThreshold { get; set; } = 10.0;
    }
}
