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

using Cognite.Extractor.Common;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Subscriptions;
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
    public partial class UAServerExplorer : UAClient, IClientCallbacks
    {
        private readonly FullConfig baseConfig;
        private readonly HashSet<BaseUANode> dataTypes = new HashSet<BaseUANode>();
        private readonly List<BaseUANode> nodeList = new List<BaseUANode>();
        private readonly List<BaseUANode> eventTypes = new List<BaseUANode>();
        private Dictionary<string, string>? namespaceMap;

        private readonly ILogger<UAServerExplorer> log;
        private readonly IServiceProvider provider;

        private bool nodesRead;
        private bool dataTypesRead;
        private bool nodeDataRead;

        public UAServerExplorer(IServiceProvider provider, FullConfig config, FullConfig baseConfig, CancellationToken token) : base(provider, config)
        {
            log = provider.GetRequiredService<ILogger<UAServerExplorer>>();
            this.provider = provider;
            this.baseConfig = baseConfig ?? new FullConfig();
            this.Config = config ?? throw new ArgumentNullException(nameof(config));
            this.Callbacks = this;

            this.baseConfig.Source.EndpointUrl = config.Source.EndpointUrl;
            this.baseConfig.Source.Password = config.Source.Password;
            this.baseConfig.Source.Username = config.Source.Username;
            this.baseConfig.Source.Secure = config.Source.Secure;
            scheduler = new PeriodicScheduler(token);
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
            var roots = Config.Extraction.GetRootNodes(Context);
            try
            {
                await Browser.BrowseNodeHierarchy(roots, ToolUtil.GetSimpleListWriterCallback(nodeList, this, TypeManager, log), token,
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
                    ToolUtil.GetSimpleListWriterCallback(dataTypes, this, TypeManager, log),
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
            await ReadNodeData(nodeList, token, "node hierarchy information");
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
                NamespaceUri = NamespaceTable!.GetString(id.NamespaceIndex),
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

            var namespaces = indices.Select(idx => NamespaceTable!.GetString(idx));

            namespaceMap = GenerateNamespaceMap(namespaces);

            log.LogInformation("Suggested namespaceMap: ");
            foreach (var kvp in namespaceMap)
            {
                log.LogInformation("    {Key}: {Value}", kvp.Key, kvp.Value);
            }

            Summary.NamespaceMap = namespaceMap.Select(kvp => $"{kvp.Key}: {kvp.Value}").ToList();

            baseConfig.Extraction.NamespaceMap = namespaceMap;
        }

        public Task OnServerDisconnect(UAClient source)
        {
            return Task.CompletedTask;
        }

        public Task OnServerReconnect(UAClient source)
        {
            return Task.CompletedTask;
        }

        public Task OnServiceLevelAboveThreshold(UAClient source)
        {
            return Task.CompletedTask;
        }

        public Task OnServicelevelBelowThreshold(UAClient source)
        {
            return Task.CompletedTask;
        }

        public void OnSubscriptionFailure(SubscriptionName subscription)
        {
        }

        public void OnCreatedSubscription(SubscriptionName subscription)
        {
        }

        public FullConfig FinalConfig => baseConfig;

        private PeriodicScheduler scheduler = null!;
        public PeriodicScheduler TaskScheduler => scheduler;
    }
}
