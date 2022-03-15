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

using Cognite.OpcUa.Types;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

[assembly: CLSCompliant(false)]
namespace Cognite.OpcUa.Config
{
    public partial class UAServerExplorer : UAClient
    {
        private readonly FullConfig baseConfig;
        private readonly HashSet<UANode> dataTypes = new HashSet<UANode>();
        private readonly List<UANode> nodeList = new List<UANode>();
        private readonly List<UANode> eventTypes = new List<UANode>();
        private Dictionary<string, string>? namespaceMap;

        private readonly ILogger<UAServerExplorer> log;
        private readonly IServiceProvider provider;

        private bool nodesRead;
        private bool dataTypesRead;
        private bool nodeDataRead;

        public UAServerExplorer(IServiceProvider provider, FullConfig config, FullConfig baseConfig) : base(provider, config)
        {
            log = provider.GetRequiredService<ILogger<UAServerExplorer>>();
            this.provider = provider;
            this.baseConfig = baseConfig ?? new FullConfig();
            this.Config = config ?? throw new ArgumentNullException(nameof(config));

            this.baseConfig.Source.EndpointUrl = config.Source.EndpointUrl;
            this.baseConfig.Source.Password = config.Source.Password;
            this.baseConfig.Source.Username = config.Source.Username;
            this.baseConfig.Source.Secure = config.Source.Secure;
        }
        public Summary Summary { get; private set; } = new Summary();
        public void ResetSummary()
        {
            Summary = new Summary();
        }
        public void ResetNodes()
        {
            nodesRead = false;
            nodeList.Clear();
            dataTypesRead = false;
            dataTypes.Clear();
            nodeDataRead = false;
            ClearEventFields();
        }

        private async Task LimitConfigValues(CancellationToken token)
        {
            var helper = new ServerInfoHelper(provider.GetRequiredService<ILogger<ServerInfoHelper>>(), this);
            await helper.LimitConfigValues(Config, token);

            baseConfig.Source.BrowseThrottling.MaxNodeParallelism = Config.Source.BrowseThrottling.MaxNodeParallelism;
            baseConfig.History.Throttling.MaxNodeParallelism = Config.History.Throttling.MaxNodeParallelism;
            baseConfig.Source.SubscriptionChunk = Config.Source.SubscriptionChunk;
            baseConfig.Source.BrowseNodesChunk = Config.Source.BrowseNodesChunk;
            baseConfig.History.DataNodesChunk = Config.History.DataNodesChunk;
            baseConfig.History.EventNodesChunk = Config.History.EventNodesChunk;
            baseConfig.Source.AttributesChunk = Config.Source.AttributesChunk;
        }

        
        /// <summary>
        /// Populate the nodeList if it has not already been populated.
        /// </summary>
        private async Task PopulateNodes(CancellationToken token)
        {
            if (nodesRead) return;
            nodeList.Clear();
            log.LogInformation("Mapping out node hierarchy");
            var roots = Config.Extraction.GetRootNodes(this);
            try
            {
                await Browser.BrowseNodeHierarchy(roots, ToolUtil.GetSimpleListWriterCallback(nodeList, this, log), token, false,
                    "populating the main node hierarchy");
                nodesRead = true;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to populate node hierarchy");
                throw;
            }
        }
        /// <summary>
        /// Populate the dataTypes list with nodes representing data types if it has not already been populated.
        /// </summary>
        private async Task PopulateDataTypes(CancellationToken token)
        {
            if (dataTypesRead) return;
            dataTypes.Clear();
            nodeDataRead = false;
            log.LogInformation("Mapping out data type hierarchy");
            try
            {
                await Browser.BrowseDirectory(
                    new List<NodeId> { DataTypes.BaseDataType },
                    ToolUtil.GetSimpleListWriterCallback(dataTypes, this, log),
                    token,
                    ReferenceTypeIds.HasSubtype,
                    (uint)NodeClass.DataType | (uint)NodeClass.ObjectType,
                    false,
                    purpose: "populating the data type hierarchy");
                dataTypesRead = true;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to populate node hierarchy");
                throw;
            }
        }
        /// <summary>
        /// Read node data for the contents of the nodeList, if it has not already been read.
        /// Reconfigures the extractor to extract as much data as possible, but resets configuration
        /// before returning.
        /// </summary>
        private async Task ReadNodeData(CancellationToken token)
        {
            if (nodeDataRead) return;
            int oldArraySize = Config.Extraction.DataTypes.MaxArraySize;
            bool oldEvents = Config.Events.Enabled;
            bool oldHistory = Config.History.Enabled;
            bool oldHistoryData = Config.History.Data;
            Config.Extraction.DataTypes.MaxArraySize = 10;
            Config.Events.Enabled = true;
            Config.History.Enabled = true;
            Config.History.Data = true;
            await ReadNodeData(nodeList, token);
            Config.Extraction.DataTypes.MaxArraySize = oldArraySize;
            Config.Events.Enabled = oldEvents;
            Config.History.Enabled = oldHistory;
            Config.History.Data = oldHistoryData;
        }

        /// <summary>
        /// Transform a NodeId to a ProtoNodeId, for writing to yml config file.
        /// </summary>
        /// <param name="id">NodeId to convert</param>
        /// <returns>Converted ProtoNodeId</returns>
        public ProtoNodeId NodeIdToProto(NodeId id)
        {
            if (id == null) return new ProtoNodeId();
            string nodeidstr = id.ToString();
            string nsstr = $"ns={id.NamespaceIndex};";
            int pos = nodeidstr.IndexOf(nsstr, StringComparison.CurrentCulture);
            if (pos == 0)
            {
                nodeidstr = nodeidstr.Substring(0, pos) + nodeidstr.Substring(pos + nsstr.Length);
            }
            return new ProtoNodeId
            {
                NamespaceUri = Session!.NamespaceUris.GetString(id.NamespaceIndex),
                NodeId = nodeidstr
            };
        }
        
        /// <summary>
        /// Generate an abbreviated string for each namespace,
        /// splits on non-numeric characters, then uses the first letter of each part,
        /// finally appends numbers to make sure all are distinct.
        /// </summary>
        /// <param name="namespaces"></param>
        /// <returns></returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Globalization", "CA1308:Normalize strings to uppercase", Justification = "Lowercase namespaces are prettier")]
        public static Dictionary<string, string> GenerateNamespaceMap(IEnumerable<string> namespaces)
        {
            var startRegex = new Regex("^.*://");
            var splitRegex = new Regex("[^a-zA-Z\\d]");

            var map = namespaces.ToDictionary(ns => ns, ns =>
                ns == "http://opcfoundation.org/UA/" ? "base" :
                    string.Concat(splitRegex.Split(startRegex.Replace(ns, ""))
                        .Where(sub => !string.IsNullOrEmpty(sub) && sub.Length > 3)
                        .Select(sub => sub.First()))
            );

            var namespaceMap = new Dictionary<string, string>();

            foreach (var mapped in map)
            {
                var baseValue = mapped.Value;

                var nextValue = baseValue;

                int index = 1;

                while (namespaceMap.Any(kvp => nextValue.ToLowerInvariant() == kvp.Value && mapped.Key != kvp.Key))
                {
                    nextValue = baseValue + index;
                    index++;
                }

                namespaceMap.Add(mapped.Key, nextValue.ToLowerInvariant());
            }

            foreach (string key in namespaceMap.Keys.ToList())
            {
                namespaceMap[key] += ":";
            }

            return namespaceMap;
        }
        /// <summary>
        /// Generate an intelligent namespace-map, with unique values, base for the base opcfoundation namespace.
        /// </summary>
        public void GetNamespaceMap()
        {
            var indices = nodeList.Concat(dataTypes).Concat(eventTypes).Select(node => node.Id.NamespaceIndex).Distinct();

            var namespaces = indices.Select(idx => Session!.NamespaceUris.GetString(idx));

            namespaceMap = GenerateNamespaceMap(namespaces);

            log.LogInformation("Suggested namespaceMap: ");
            foreach (var kvp in namespaceMap)
            {
                log.LogInformation("    {Key}: {Value}", kvp.Key, kvp.Value);
            }

            Summary.NamespaceMap = namespaceMap.Select(kvp => $"{kvp.Key}: {kvp.Value}").ToList();

            baseConfig.Extraction.NamespaceMap = namespaceMap;
        }
        /// <summary>
        /// Log a summary of the run.
        /// </summary>
        public void LogSummary()
        {
            log.LogInformation("");
            log.LogInformation("Server analysis successfully completed, no critical issues were found");
            log.LogInformation("==== SUMMARY ====");
            log.LogInformation("");

            if (Summary.Endpoints.Any())
            {
                log.LogInformation("{Count} endpoints were found: ", Summary.Endpoints.Count);
                foreach (var endpoint in Summary.Endpoints)
                {
                    log.LogInformation("    {EndPoint}", endpoint);
                }

                if (Summary.Secure)
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
            log.LogInformation("");

            if (Summary.BrowseChunk == 0)
            {
                log.LogInformation("Settled on browsing the children of {BrowseNodesChunk} nodes at a time and letting the server decide how many results " +
                                "to return for each request", Summary.BrowseNodesChunk);
            }
            else
            {
                log.LogInformation("Settled on browsing the children of {BrowseNodesChunk} nodes at a time and expecting {BrowseChunk} results maximum for each request",
                    Summary.BrowseNodesChunk, Summary.BrowseChunk);
            }
            log.LogInformation("");

            if (Summary.CustomNumTypesCount > 0)
            {
                log.LogInformation("{Count} custom numeric types were discovered", Summary.CustomNumTypesCount);
            }
            if (Summary.MaxArraySize > 1)
            {
                log.LogInformation("Arrays of size {Size} were discovered", Summary.MaxArraySize);
            }
            if (Summary.StringVariables)
            {
                log.LogInformation("There are variables that would be mapped to strings in CDF, if this is not correct " +
                                "they may be numeric types that the auto detection did not catch, or they may need to be filtered out");
            }
            if (Summary.Enums)
            {
                log.LogInformation("There are variables with enum datatype. These can either be mapped to raw integer values with labels in" +
                    "metadata, or to string timeseries with labels as values.");
            }
            if (Summary.CustomNumTypesCount > 0 || Summary.MaxArraySize > 0 || Summary.StringVariables || Summary.Enums)
            {
                log.LogInformation("");
            }

            log.LogInformation("Settled on reading {Count} attributes per Read call", Summary.AttributeChunkSize);
            if (Summary.VariableLimitWarning)
            {
                log.LogInformation("This is not a completely safe option, as the actual number of attributes is lower than the limit, so if " +
                                "the number of nodes increases in the future, it may fail");
            }
            log.LogInformation("");

            if (Summary.Subscriptions)
            {
                log.LogInformation("Successfully subscribed to data variables");
                log.LogInformation("Settled on subscription chunk size: {SubscriptionChunk}", Summary.SubscriptionChunkSize);
                if (Summary.SubscriptionLimitWarning)
                {
                    log.LogInformation("This is not a completely safe option, as the actual number of extractable nodes is lower than the limit, " +
                                    "so if the number of variables increases in the future, it may fail");
                }

                if (Summary.SilentSubscriptionsWarning)
                {
                    log.LogInformation("Though subscriptions were successfully created, no data was received. This may be an issue if " +
                                    "data is expected to appear within a five second window");
                }
            }
            else
            {
                log.LogInformation("The explorer was unable to subscribe to data variables, because none exist or due to a server issue");
            }
            log.LogInformation("");

            if (Summary.History)
            {
                log.LogInformation("Successfully read datapoint history");
                log.LogInformation("Settled on history chunk size {HistoryChunk} with granularity {HistoryGranularity}",
                    Summary.HistoryChunkSize, Summary.HistoryGranularity);
                if (Summary.BackfillRecommended)
                {
                    log.LogInformation("There are large enough amounts of datapoints for certain variables that " +
                                    "enabling backfill is recommended. This increases startup time a bit, but makes the extractor capable of " +
                                    "reading live data and historical data at the same time");
                }
            }
            else if (Summary.NoHistorizingNodes)
            {
                log.LogInformation("No historizing nodes detected, the server may support history, but the extractor will only read " +
                                "history from nodes with the Historizing attribute set to true");
            }
            else
            {
                log.LogInformation("The explorer was unable to read history");
            }
            log.LogInformation("");

            if (Summary.AnyEvents)
            {
                log.LogInformation("Successfully found support for events on the server");
                log.LogInformation("Found {Count} nodes emitting events", Summary.NumEmitters);
                if (Summary.HistoricalEvents)
                {
                    log.LogInformation("{Count} historizing event emitters were found", Summary.NumHistEmitters);
                }
                if (Summary.NumEmitters == 0)
                {
                    log.LogWarning("Found GeneratesEvent references, but no nodes had correctly configured EventNotifier");
                    log.LogWarning("Any emitters must be configured manually");
                }
            }
            else
            {
                log.LogInformation("No regular relevant events were able to be read from the server");
            }

            if (Summary.Auditing)
            {
                log.LogInformation("The server likely supports auditing, which may be used to detect addition of nodes and references");
            }
            log.LogInformation("");

            log.LogInformation("The following NamespaceMap was suggested: ");
            foreach (string ns in Summary.NamespaceMap)
            {
                log.LogInformation("    {Namespace}", ns);
            }
        }

        public FullConfig FinalConfig => baseConfig;
    }
}
