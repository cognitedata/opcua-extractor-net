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

using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cognite.OpcUa.Config
{
    public class SessionSummary
    {
        public IList<string> Endpoints { get; set; } = new List<string>();
        public bool Secure { get; set; }

        public void Log(ILogger log)
        {
            if (Endpoints.Any())
            {
                log.LogInformation("{Count} endpoints were found: ", Endpoints.Count);
                foreach (var endpoint in Endpoints)
                {
                    log.LogInformation("    {EndPoint}", endpoint);
                }

                if (Secure)
                {
                    log.LogInformation("At least one of these are secure, meaning that the Secure config option can and should be enabled.");
                }
                else
                {
                    log.LogInformation("None of these are secure, so enabling the Secure config option will probably not work.");
                }
            }
            else
            {
                log.LogInformation("No endpoints were found, but the client was able to connect. This is not necessarily an issue, " +
                                "but there may be a different discovery URL connected to the server that exposes further endpoints.");
            }
        }
    }

    public class BrowseSummary
    {
        public int BrowseNodesChunk { get; set; }
        public int BrowseChunk { get; set; }
        public bool BrowseNextWarning { get; set; }

        public void Log(ILogger log)
        {
            if (BrowseChunk == 0)
            {
                log.LogInformation("Settled on browsing the children of {BrowseNodesChunk} nodes at a time and letting the server decide how many results " +
                                "to return for each request", BrowseNodesChunk);
            }
            else
            {
                log.LogInformation("Settled on browsing the children of {BrowseNodesChunk} nodes at a time and expecting {BrowseChunk} results maximum for each request",
                    BrowseNodesChunk, BrowseChunk);
            }

            if (BrowseNextWarning)
            {
                log.LogWarning("This server most likely does not correctly support BrowseNext. This potentially makes it impossible to retrieve all " +
                    "nodes. The chosen values for browse nodes chunk and browse chunk gave the best results given the size of the server, " +
                    "but this is not guaranteed to work in the future.");
            }
        }
    }

    public class DataTypeSummary
    {
        public int CustomNumTypesCount { get; set; }
        public int MaxArraySize { get; set; }
        public int MaxUnlimitedArraySize { get; set; }
        public bool StringVariables { get; set; }
        public bool Enums { get; set; }
        public bool NullDataType { get; set; }
        public bool MissingDataType { get; set; }
        public bool UnspecificDimensions { get; set; }
        public bool HighDimensions { get; set; }
        public bool LargeArrays { get; set; }

        public bool Any =>
            CustomNumTypesCount > 0 || MaxArraySize > 1 || MaxUnlimitedArraySize > 0 || StringVariables
            || Enums || NullDataType || MissingDataType || UnspecificDimensions || HighDimensions || LargeArrays;

        public void Log(ILogger log)
        {
            if (CustomNumTypesCount > 0)
            {
                log.LogInformation("{Count} custom numeric types were discovered", CustomNumTypesCount);
            }
            if (MaxArraySize > 1)
            {
                log.LogInformation("Arrays of size {Size} were discovered", MaxArraySize);
            }
            if (MaxUnlimitedArraySize > 0)
            {
                log.LogWarning("Arrays of sizes up to {Max} were discovered, but these were not used due to being over the limit for the explorer.", MaxUnlimitedArraySize);
            }
            if (StringVariables)
            {
                log.LogInformation("There are variables that would be mapped to strings in CDF, if this is not correct " +
                                "they may be numeric types that the auto detection did not catch, or they may need to be filtered out");
            }
            if (Enums)
            {
                log.LogInformation("There are variables with enum datatype. These can either be mapped to raw integer values with labels in " +
                    "metadata, or to string timeseries with labels as values.");
            }
            if (NullDataType)
            {
                log.LogWarning("There were nodes with a null data type. The extractor has no way to deal with these beyond setting extraction.data-types.null-as-numeric " +
                    "and assuming they are all numeric.");
            }
            if (MissingDataType)
            {
                log.LogWarning("There were nodes with a data type that does not exist in the main data type hierarchy. Datatype discovery mechanisms may not work correctly. " +
                    "This is not valid server behavior.");
            }
            if (HighDimensions)
            {
                log.LogWarning("There were nodes with high dimensionality (two or more dimensions are unsupported)");
            }
            if (LargeArrays)
            {
                log.LogWarning("There were nodes with too large array size. By default analysis skips arrays larger than 10, though the extractor can extract them. " +
                    "This is because some data types are represented as very large arrays. The max detected size is {Max}, you may set MaxArraySize yourself if reading these is desired.",
                    MaxUnlimitedArraySize);
            }
            if (UnspecificDimensions)
            {
                log.LogWarning("There were nodes with unspecific dimensions. By default the extractor requires a specific ValueRank. " +
                    "Options under extraction.data-types may be used to work around this.");
            }
        }
    }

    public class AttributeSummary
    {
        public int ChunkSize { get; set; }
        public bool LimitWarning { get; set; }
        public int KnownCount { get; set; }

        public void Log(ILogger log)
        {
            log.LogInformation("Settled on reading {Count} attributes per Read call", ChunkSize);
            if (LimitWarning && KnownCount < ChunkSize)
            {
                log.LogInformation("This is not a completely safe option, as the actual number of attributes is lower than the limit, so if " +
                    "the number of nodes increases in the future, it may fail. To be completely safe you may use a value smaller than or equal to {Known}",
                    KnownCount);
            }
        }
    }

    public class SubscriptionSummary
    {
        public int ChunkSize { get; set; }
        public bool LimitWarning { get; set; }
        public int KnownCount { get; set; }
        public bool SilentWarning { get; set; }
        public bool Unsupported { get; set; }
        public bool UnableToUnsubscribe { get; set; }
        public bool Enabled { get; set; }
        public bool NoSubscribable { get; set; }

        public void Log(Summary parent, ILogger log)
        {
            if (Enabled)
            {
                log.LogInformation("Successfully subscribed to data variables");
                log.LogInformation("Settled on subscription chunk size: {SubscriptionChunk}", ChunkSize);
                if (LimitWarning && KnownCount < ChunkSize)
                {
                    log.LogInformation("This is not a completely safe option, as the actual number of extractable nodes is lower than the limit, " +
                                    "so if the number of variables increases in the future, it may fail. To be completely safe you may use a value " +
                                    "smaller than or equal to {Known}", KnownCount);
                }

                if (SilentWarning)
                {
                    log.LogInformation("Though subscriptions were successfully created, no data was received. This may be an issue if " +
                                    "data is expected to appear within a five second window");
                }
            }
            else
            {
                if (NoSubscribable)
                {
                    log.LogWarning("The explorer was unable to find any nodes that it could subscribe to. If this is not expected it is quite likely an " +
                        "issue with ValueRank or with null data types.");
                    if (parent.DataTypes.HighDimensions)
                    {
                        log.LogWarning("NOTE: nodes were skipped due to high dimensionality (two or more dimensions are unsupported)");
                    }
                    if (parent.DataTypes.LargeArrays)
                    {
                        log.LogWarning("NOTE: nodes were skipped due to too large array size. By default analysis skips arrays larger than 10, though the extractor can extract them. " +
                            "This is because some data types are represented as very large arrays. The max detected size is {Max}, you may set MaxArraySize yourself if reading these is desired.",
                            parent.DataTypes.MaxUnlimitedArraySize);
                    }
                    if (parent.DataTypes.UnspecificDimensions)
                    {
                        log.LogWarning("NOTE: nodes were skipped due to unspecific dimensions. By default the extractor requires a specific ValueRank. " +
                            "Options under extraction.data-types may be used to work around this.");
                    }
                }
                log.LogInformation("The explorer was unable to subscribe to data variables, because none exist or due to a server issue");
            }
        }
    }

    public class HistorySummary
    {
        public int ChunkSize { get; set; }
        public bool NoHistorizingNodes { get; set; }
        public bool BackfillRecommended { get; set; }
        public bool Enabled { get; set; }
        public TimeSpan Granularity { get; set; }
        public void Log(ILogger log)
        {
            if (Enabled)
            {
                log.LogInformation("Successfully read datapoint history");
                log.LogInformation("Settled on history chunk size {HistoryChunk} with granularity {HistoryGranularity}",
                    ChunkSize, Granularity);
                if (BackfillRecommended)
                {
                    log.LogInformation("There are large enough amounts of datapoints for certain variables that " +
                                    "enabling backfill is recommended. This increases startup time a bit, but makes the extractor capable of " +
                                    "reading live data and historical data at the same time");
                }
            }
            else if (NoHistorizingNodes)
            {
                log.LogInformation("No historizing nodes detected, the server may support history, but the extractor will only read " +
                                "history from nodes with the Historizing attribute set to true");
            }
            else
            {
                log.LogInformation("The explorer was unable to read history");
            }
        }
    }
    public class EventSummary
    {
        public bool AnyEvents { get; set; }
        public bool HistoricalEvents { get; set; }
        public int NumEmitters { get; set; }
        public int NumHistEmitters { get; set; }
        public bool Auditing { get; set; }

        public void Log(ILogger log)
        {
            if (AnyEvents)
            {
                log.LogInformation("Successfully found support for events on the server");
                log.LogInformation("Found {Count} nodes emitting events", NumEmitters);
                if (HistoricalEvents)
                {
                    log.LogInformation("{Count} historizing event emitters were found", NumHistEmitters);
                }
                if (NumEmitters == 0)
                {
                    log.LogWarning("Found GeneratesEvent references, but no nodes had correctly configured EventNotifier");
                    log.LogWarning("Any emitters must be configured manually");
                }
            }
            else
            {
                log.LogInformation("No regular relevant events were able to be read from the server");
            }

            if (Auditing)
            {
                log.LogInformation("The server likely supports auditing, which may be used to detect addition of nodes and references");
            }
        }
    }

    /// <summary>
    /// Contains data about a run of the configuration tool.
    /// </summary>
    public class Summary
    {
        public SessionSummary Session { get; } = new SessionSummary();
        public BrowseSummary Browse { get; } = new BrowseSummary();
        public DataTypeSummary DataTypes { get; } = new DataTypeSummary();
        public AttributeSummary Attributes { get; } = new AttributeSummary();
        public SubscriptionSummary Subscriptions { get; } = new SubscriptionSummary();
        public HistorySummary History { get; } = new HistorySummary();
        public EventSummary Events { get; } = new EventSummary();

        public IList<string> NamespaceMap { get; set; } = new List<string>();

        public void Log(ILogger log)
        {
            log.LogInformation("");
            log.LogInformation("Server analysis successfully completed, no critical issues were found");
            log.LogInformation("==== SUMMARY ====");
            log.LogInformation("");

            Session.Log(log);
            log.LogInformation("");

            Browse.Log(log);
            log.LogInformation("");

            DataTypes.Log(log);
            if (DataTypes.Any) log.LogInformation("");

            Attributes.Log(log);
            log.LogInformation("");

            Subscriptions.Log(this, log);
            log.LogInformation("");

            History.Log(log);
            log.LogInformation("");

            Events.Log(log);
            log.LogInformation("");

            log.LogInformation("The following NamespaceMap was suggested: ");
            foreach (string ns in NamespaceMap)
            {
                log.LogInformation("    {Namespace}", ns);
            }
        }
    }
}
