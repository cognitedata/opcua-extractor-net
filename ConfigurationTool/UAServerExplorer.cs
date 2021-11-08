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
using Cognite.OpcUa.History;
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
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1815:Override equals and operator equals on value types", Justification = "Summary struct")]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1051:Do not declare visible instance fields", Justification = "Summary struct")]
    public struct Summary
    {
        public IList<string> Endpoints;
        public bool Secure;
        public int BrowseNodesChunk;
        public int BrowseChunk;
        public bool BrowseNextWarning;
        public int CustomNumTypesCount;
        public int MaxArraySize;
        public bool StringVariables;
        public int AttributeChunkSize;
        public bool VariableLimitWarning;
        public int SubscriptionChunkSize;
        public bool SubscriptionLimitWarning;
        public bool SilentSubscriptionsWarning;
        public int HistoryChunkSize;
        public bool NoHistorizingNodes;
        public bool BackfillRecommended;
        public bool HistoricalEvents;
        public bool AnyEvents;
        public int NumEmitters;
        public int NumHistEmitters;
        public IList<string> NamespaceMap;
        public TimeSpan HistoryGranularity;
        public bool Enums;
        public bool Auditing;
        public bool Subscriptions;
        public bool History;
    }
    public class UAServerExplorer : UAClient
    {
        private readonly FullConfig baseConfig;
        private readonly HashSet<UANode> dataTypes = new HashSet<UANode>();
        private readonly List<ProtoDataType> customNumericTypes = new List<ProtoDataType>();
        private readonly List<UANode> nodeList = new List<UANode>();
        private readonly List<UANode> eventTypes = new List<UANode>();
        private Dictionary<string, string>? namespaceMap;

        private bool history;

        private readonly ILogger<UAServerExplorer> log;
        private readonly IServiceProvider provider;

        private bool nodesRead;
        private bool dataTypesRead;
        private bool nodeDataRead;

        private readonly ICollection<int> testAttributeChunkSizes = new List<int>
        {
            100000,
            10000,
            1000,
            100,
            10
        };

        private readonly ICollection<int> testSubscriptionChunkSizes = new List<int>
        {
            10000,
            1000,
            100,
            10,
            1
        };

        private readonly ICollection<int> testHistoryChunkSizes = new List<int>
        {
            100,
            10,
            1
        };



        private Summary summary;

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
        public Summary Summary => summary;
        public void ResetSummary()
        {
            summary = new Summary();
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

        /// <summary>
        /// Try connecting to the server, and treating it as a discovery server, to list other endpoints on the same server.
        /// </summary>
        public async Task GetEndpoints(CancellationToken token)
        {
            log.LogInformation("Attempting to list endpoints using given url as discovery server");

            if (AppConfig == null)
            {
                await LoadAppConfig();
            }

            var context = AppConfig!.CreateMessageContext();
            var endpointConfig = EndpointConfiguration.Create(AppConfig);
            var endpoints = new EndpointDescriptionCollection();
            using (var channel = DiscoveryChannel.Create(new Uri(Config.Source.EndpointUrl), endpointConfig, context))
            {
                using var disc = new DiscoveryClient(channel);
                try
                {
                    endpoints = await disc.GetEndpointsAsync(null);
                    summary.Endpoints = endpoints.Select(ep => $"{ep.EndpointUrl}: {ep.SecurityPolicyUri}").ToList();
                }
                catch (Exception e)
                {
                    log.LogWarning("Endpoint discovery failed, the given URL may not be a discovery server.");
                    log.LogDebug(e, "Endpoint discovery failed");
                }
            }


            bool openExists = false;
            bool secureExists = false;

            foreach (var ep in endpoints)
            {
                log.LogInformation("Endpoint: {Url}, Security: {Security}", ep.EndpointUrl, ep.SecurityPolicyUri);
                openExists |= ep.SecurityPolicyUri == SecurityPolicies.None;
                secureExists |= ep.SecurityPolicyUri != SecurityPolicies.None;
                summary.Secure = secureExists;
            }

            if (Session == null || !Session.Connected)
            {
                try
                {
                    await Run(token);
                    await LimitConfigValues(token);
                }
                catch (Exception ex)
                {
                    log.LogError("Failed to connect to server using initial options");
                    log.LogDebug(ex, "Failed to connect to endpoint");
                }
            }

            if (Session == null || !Session.Connected)
            {
                if (!secureExists && !openExists)
                {
                    log.LogInformation("No endpoint found, make sure the given discovery url is correct");
                }
                else if (!secureExists && Config.Source.Secure)
                {
                    log.LogInformation("No secure endpoint exists, so connection will fail if Secure is true");
                }
                else if (openExists && Config.Source.Secure)
                {
                    log.LogInformation("Secure connection failed, username or password may be wrong, or the client" +
                                    "may need to be added to a trusted list in the server.");
                    log.LogInformation("An open endpoint exists, so if secure is set to false and no username/password is provided" +
                                    "connection may succeed");
                }
                else if (!Config.Source.Secure && !openExists)
                {
                    log.LogInformation("Secure is set to false, but no open endpoint exists. Either set secure to true," +
                                    "or add an open endpoint to the server");
                }

                throw new FatalException("Fatal: Provided configuration failed to connect to the server");
            }

            Session.KeepAliveInterval = Math.Max(Config.Source.KeepAliveInterval, 30000);
        }
        /// <summary>
        /// Try to get at least 10k nodes using the given node chunk when browsing.
        /// </summary>
        /// <param name="nodesChunk">Chunk size to use when browsing</param>
        /// <returns>A list of discovered UANodes</returns>
        private async Task<IEnumerable<UANode>> GetTestNodeChunk(int nodesChunk, CancellationToken token)
        {
            var root = ObjectIds.ObjectsFolder;

            // Try to find at least 10000 nodes
            var nodes = new List<UANode>();
            var callback = ToolUtil.GetSimpleListWriterCallback(nodes, this);

            var nextIds = new List<NodeId> { root };

            var localVisitedNodes = new HashSet<NodeId>
            {
                root
            };

            int totalChildCount = 0;

            log.LogInformation("Get test node chunk with BrowseNodesChunk {Count}", nodesChunk);

            do
            {
                // Recursively browse the node hierarchy until we get at least 10k nodes, or there are no more nodes to browse.
                var references = new Dictionary<NodeId, ReferenceDescriptionCollection>();
                var total = nextIds.Count;
                int count = 0;
                int countChildren = 0;
                foreach (var chunk in nextIds.ChunkBy(nodesChunk))
                {
                    if (token.IsCancellationRequested) return nodes;
                    var browseNodes = chunk.Select(node => new BrowseNode(node)).ToDictionary(node => node.Id);
                    await GetReferences(new BrowseParams
                    {
                        NodeClassMask = (uint)NodeClass.Object | (uint)NodeClass.Variable,
                        Nodes = browseNodes
                    }, true, token);

                    foreach (var node in browseNodes.Values)
                    {
                        if (node.Result == null) continue;
                        references[node.Id] = node.Result.References;
                        countChildren += node.Result.References.Count;
                    }
                    count += browseNodes.Count;
                    log.LogDebug("Read node children {Count} / {Total}. Children: {ChildCount}", count, total, countChildren);
                    totalChildCount += countChildren;
                    if (totalChildCount >= 10000) break;
                }

                nextIds.Clear();
                foreach (var (parentId, children) in references)
                {
                    foreach (var rd in children)
                    {
                        var nodeId = ToNodeId(rd.NodeId);
                        bool docb = true;
                        if (docb)
                        {
                            log.LogTrace("Discovered new node {NodeId}", nodeId);
                            callback?.Invoke(rd, parentId);
                        }
                        if (rd.NodeClass == NodeClass.Variable) continue;
                        if (localVisitedNodes.Add(nodeId))
                        {
                            nextIds.Add(nodeId);
                        }
                    }
                }
            } while (totalChildCount < 10001 && nextIds.Any());

            return nodes;
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
        /// Try to get the optimal values for the browse-chunk and browse-nodes-chunk config options.
        /// This defaults to just using as large values as possible, then performs an extra test
        /// to check for issues caused by lack of BrowseNext support.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task GetBrowseChunkSizes(CancellationToken token)
        {
            if (Session == null || !Session.Connected)
            {
                await Run(token);
                await LimitConfigValues(token);
            }

            IEnumerable<UANode>? testNodes = null;

            int browseChunkSize = 0;

            // First try to find a chunk size that works
            foreach (int chunkSize in new[] { 1000, 100, 10, 1 }.Where(chunk => chunk <= Config.Source.BrowseNodesChunk))
            {
                try
                {
                    testNodes = await ToolUtil.RunWithTimeout(Task.Run(() => GetTestNodeChunk(chunkSize, token)), 60);
                    browseChunkSize = chunkSize;
                    break;
                }
                catch (Exception ex)
                {
                    log.LogWarning("Failed to browse node hierarchy");
                    log.LogDebug(ex, "Failed to browse nodes");
                    if (ex is ServiceResultException exc && exc.StatusCode == StatusCodes.BadServiceUnsupported)
                    {
                        throw new FatalException(
                            "Browse unsupported by server, the extractor does not support servers without support for" +
                            " the \"Browse\" service");
                    }
                }
            }
            var parents = testNodes.Select(node => node.ParentId).Distinct().ToList();
            log.LogInformation("Found {Count} nodes over {ParentCount} parents", testNodes.Count(), parents.Count);

            // Test tolerance for large chunks
            log.LogInformation("Testing browseNodesChunk tolerance");
            // We got some indication of max legal size before, here we choose a chunkSize that is reasonable
            // for the discovered server size
            var validSizes = new[] { 10000, 1000, 100, 10, 1 }
                .Where(size => (browseChunkSize == 1000 || size <= browseChunkSize) && size <= testNodes.Count()).ToList();
            foreach (int chunkSize in validSizes)
            {
                var ids = testNodes.Select(node => node.Id).Take(chunkSize).ToList();
                try
                {
                    log.LogInformation("Try to get the children of {Count} nodes", ids.Count);
                    var browseNodes = ids.Select(node => new BrowseNode(node)).ToDictionary(node => node.Id);
                    await ToolUtil.RunWithTimeout(GetReferences(new BrowseParams
                    {
                        NodeClassMask = (uint)NodeClass.Object | (uint)NodeClass.Variable,
                        Nodes = browseNodes
                    }, true, token), 30);
                    break;
                }
                catch (Exception ex)
                {
                    log.LogWarning("Failed to browse node hierarchy");
                    log.LogDebug(ex, "Failed to browse nodes");
                }
                browseChunkSize = chunkSize;
            }
            log.LogInformation("Settled on a BrowseNodesChunk of {ChunkSize}", browseChunkSize);
            summary.BrowseNodesChunk = browseChunkSize;

            // Test if there are issues with BrowseNext.
            int originalChunkSize = Config.Source.BrowseChunk;
            foreach (int chunkSize in new[] { 10000, 1000, 100, 10, 1 })
            {
                var nodesByParent = testNodes.GroupBy(node => node.ParentId).OrderByDescending(group => group.Count()).Take(browseChunkSize);
                int total = 0;
                var toBrowse = nodesByParent.TakeWhile(chunk =>
                {
                    bool pass = total <= chunkSize * 2;
                    if (pass)
                    {
                        total += chunk.Count();
                    }
                    return pass;
                }).ToList();

                if (total < chunkSize) continue;

                Config.Source.BrowseChunk = chunkSize;
                Dictionary<NodeId, BrowseResult?> children;
                try
                {
                    log.LogInformation("Try to get the children of the {Count} largest parent nodes, with return chunk size {Size}",
                        toBrowse.Count, chunkSize);
                    var nodes = toBrowse.Select(group => new BrowseNode(group.Key)).ToDictionary(node => node.Id);
                    await ToolUtil.RunWithTimeout(GetReferences(new BrowseParams
                    {
                        NodeClassMask = (uint)NodeClass.Object | (uint)NodeClass.Variable,
                        Nodes = nodes
                    }, true, token), 60);
                    children = nodes.ToDictionary(node => node.Key, node => node.Value.Result);
                }
                catch (Exception ex)
                {
                    log.LogWarning("Failed to browse node hierarchy");
                    log.LogDebug(ex, "Failed to browse nodes");
                    continue;
                }
                int childCount = children.Aggregate(0, (seed, kvp) => seed + kvp.Value?.References?.Count ?? 0);
                if (childCount < total)
                {
                    log.LogWarning("Expected to receive {Count} nodes but only got {ChildCount}!", total, childCount);
                    log.LogWarning("There is likely an issue with returning large numbers of nodes from the server");
                    summary.BrowseNextWarning = true;
                    int largest = toBrowse.First().Count();
                    log.LogInformation("The largest discovered node has {Count} children", largest);
                    // Usually we will have found the largest parent by this point, unless the server is extremely large
                    // So we can try to choose a BrowseNodesChunk that lets us avoid the issue
                    summary.BrowseNodesChunk = Math.Max(1, (int)Math.Floor((double)chunkSize / largest));
                }
                summary.BrowseChunk = Math.Min(chunkSize, originalChunkSize);
                break;
            }
            Config.Source.BrowseChunk = summary.BrowseChunk;
            Config.Source.BrowseNodesChunk = summary.BrowseNodesChunk;
            baseConfig.Source.BrowseChunk = summary.BrowseChunk;
            baseConfig.Source.BrowseNodesChunk = summary.BrowseNodesChunk;
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
                await Browser.BrowseNodeHierarchy(roots, ToolUtil.GetSimpleListWriterCallback(nodeList, this), token, false);
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
                    ToolUtil.GetSimpleListWriterCallback(dataTypes, this),
                    token,
                    ReferenceTypeIds.HasSubtype,
                    (uint)NodeClass.DataType | (uint)NodeClass.ObjectType,
                    false);
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
        /// Returns true if the id is for a custom object. Tested by checking for non-integer identifiertype, or >0 namespaceUri.
        /// </summary>
        /// <param name="id">Id to test</param>
        /// <returns>True if id is a custom object</returns>
        private static bool IsCustomObject(NodeId id)
        {
            return id.NamespaceIndex != 0 || id.IdType != IdType.Numeric;
        }

        /// <summary>
        /// Try to identify the given UANode as a datatype, updating the summary and config
        /// based on the outcome.
        /// The goal is to identify enumerations and custom types, to determine whether
        /// custom datatype configuration is needed.
        /// </summary>
        /// <param name="type">Type to test.</param>
        private void TestDataType(UANode type)
        {
            if (!IsCustomObject(type.Id)) return;
            uint dataTypeSwitch = 0;
            bool inHierarchy = false;

            // The datatype may be placed correctly in the datatype hierarchy.
            if (ToolUtil.IsChildOf(dataTypes, type, DataTypes.Number))
            {
                dataTypeSwitch = DataTypes.Number;
                inHierarchy = true;
            }
            else if (ToolUtil.IsChildOf(dataTypes, type, DataTypes.Boolean))
            {
                dataTypeSwitch = DataTypes.Boolean;
                inHierarchy = true;
            }
            else if (ToolUtil.IsChildOf(dataTypes, type, DataTypes.Enumeration))
            {
                dataTypeSwitch = DataTypes.Enumeration;
                inHierarchy = true;
            }
            // If not, it may be placed incorrectly but contain naming that indicates what type it is.
            if (dataTypeSwitch == 0)
            {
                if (ToolUtil.NodeNameContains(type, "real")
                    || ToolUtil.NodeNameContains(type, "integer")
                    || ToolUtil.NodeNameStartsWith(type, "int")
                    || ToolUtil.NodeNameContains(type, "number"))
                {
                    dataTypeSwitch = DataTypes.Number;
                }
                else if (ToolUtil.NodeNameContains(type, "bool"))
                {
                    dataTypeSwitch = DataTypes.Boolean;
                }
                else if (ToolUtil.NodeNameContains(type, "enum"))
                {
                    dataTypeSwitch = DataTypes.Enumeration;
                }
            }
            // Finally, log the results and update the summary.
            switch (dataTypeSwitch)
            {
                case DataTypes.Number:
                    log.LogInformation("Found potential numeric type: {Id}", type.Id);
                    break;
                case DataTypes.Boolean:
                    log.LogInformation("Found potential boolean type: {Id}", type.Id);
                    break;
                case DataTypes.Enumeration:
                    log.LogInformation("Found potential enum type: {Id}, consider turning on extraction.enum-as-strings", type.Id);
                    summary.Enums = true;
                    break;
            }
            // Update configuration based on whether or not the node was found in hierarchy.
            if (dataTypeSwitch > 0)
            {
                if (inHierarchy)
                {
                    log.LogInformation("DataType {Id} is correctly in hierarchy, auto discovery can be used instead", type.Id);
                    baseConfig.Extraction.DataTypes.AutoIdentifyTypes = true;
                }
                else
                {
                    customNumericTypes.Add(new ProtoDataType
                    {
                        IsStep = dataTypeSwitch == DataTypes.Boolean,
                        Enum = dataTypeSwitch == DataTypes.Enumeration,
                        NodeId = NodeIdToProto(type.Id)
                    });
                }
                if (dataTypeSwitch == DataTypes.Enumeration)
                {
                    log.LogInformation("DataType {Id} is enum, and auto discovery should be enabled to discover labels", type.Id);
                    baseConfig.Extraction.DataTypes.AutoIdentifyTypes = true;
                }
            }
        }

        /// <summary>
        /// Browse the datatype hierarchy, checking for custom numeric datatypes.
        /// </summary>
        public async Task ReadCustomTypes(CancellationToken token)
        {
            if (Session == null || !Session.Connected)
            {
                await Run(token);
                await LimitConfigValues(token);
            }
            await PopulateDataTypes(token);

            customNumericTypes.Clear();
            foreach (var type in dataTypes)
            {
                TestDataType(type);
            }

            if (!summary.Enums && dataTypes.Any(type => ToolUtil.IsChildOf(dataTypes, type, DataTypes.Enumeration))) summary.Enums = true;

            log.LogInformation("Found {Count} custom datatypes outside of normal hierarchy", customNumericTypes.Count);
            summary.CustomNumTypesCount = customNumericTypes.Count;
            baseConfig.Extraction.DataTypes.CustomNumericTypes = customNumericTypes;
        }
        /// <summary>
        /// Get AttributeChunk config value, by attempting to read for various chunk sizes.
        /// Uses a value decently proportional to the server size, only 10k if the server is large enough
        /// for that to make sense.
        /// Terminates as soon as a read succeeds.
        /// </summary>
        public async Task GetAttributeChunkSizes(CancellationToken token)
        {
            log.LogInformation("Reading variable chunk sizes to determine the AttributeChunk property");

            if (Session == null || !Session.Connected)
            {
                await Run(token);
                await LimitConfigValues(token);
            }

            await PopulateNodes(token);

            int oldArraySize = Config.Extraction.DataTypes.MaxArraySize;
            int expectedAttributeReads = nodeList.Sum(node => node.Attributes.GetAttributeIds(Config).Count());
            Config.History.Enabled = true;
            Config.Extraction.DataTypes.MaxArraySize = 10;

            var testChunks = testAttributeChunkSizes.Where(chunkSize =>
                chunkSize <= expectedAttributeReads || chunkSize <= 1000);

            if (expectedAttributeReads < 1000)
            {
                log.LogWarning("Reading less than 1000 attributes maximum. Most servers should support more, but" +
                            " this server only has enough nodes to read {Reads}", expectedAttributeReads);
                summary.VariableLimitWarning = true;
            }

            bool succeeded = false;

            foreach (int chunkSize in testChunks)
            {
                int count = 0;
                var toCheck = nodeList.TakeWhile(node =>
                {
                    count += node.Attributes.GetAttributeIds(Config).Count();
                    return count < chunkSize + 10;
                }).ToList();
                log.LogInformation("Total {Total}", nodeList.Count);
                log.LogInformation("Attempting to read attributes for {Cnt} nodes with ChunkSize {ChunkSize}", toCheck.Count, chunkSize);
                Config.Source.AttributesChunk = chunkSize;
                try
                {
                    await ToolUtil.RunWithTimeout(ReadNodeData(toCheck, token), 120);
                }
                catch (Exception e)
                {
                    log.LogInformation(e, "Failed to read node attributes");

                    if (e is ServiceResultException exc && exc.StatusCode == StatusCodes.BadServiceUnsupported)
                    {
                        throw new FatalException(
                            "Attribute read is unsupported, the extractor does not support servers which do not " +
                            "support the \"Read\" service");
                    }

                    continue;
                }

                log.LogInformation("Settled on AttributesChunk: {Size}", chunkSize);
                succeeded = true;
                baseConfig.Source.AttributesChunk = chunkSize;
                break;
            }

            summary.AttributeChunkSize = baseConfig.Source.AttributesChunk;

            Config.Extraction.DataTypes.MaxArraySize = oldArraySize;

            if (!succeeded)
            {
                throw new FatalException("Failed to read node attributes for any chunk size");
            }
        }
        /// <summary>
        /// Look through the node hierarchy to find arrays and strings, setting MaxArraySize and StringVariables
        /// </summary>
        public async Task IdentifyDataTypeSettings(CancellationToken token)
        {
            var roots = Config.Extraction.GetRootNodes(this);

            int oldArraySize = Config.Extraction.DataTypes.MaxArraySize;
            int arrayLimit = Config.Extraction.DataTypes.MaxArraySize == 0 ? 10 : Config.Extraction.DataTypes.MaxArraySize;
            if (arrayLimit < 0) arrayLimit = int.MaxValue;

            Config.Extraction.DataTypes.MaxArraySize = 10;

            await PopulateNodes(token);
            await PopulateDataTypes(token);
            await ReadNodeData(token);

            log.LogInformation("Mapping out variable datatypes");

            var variables = nodeList
                .Where(node => (node is UAVariable variable) && !variable.IsProperty)
                .Select(node => node as UAVariable)
                .Where(node => node != null);

            history = false;
            bool stringVariables = false;
            int maxLimitedArrayLength = 0;

            var identifiedTypes = new List<UANode>();
            var missingTypes = new HashSet<NodeId>();
            foreach (var variable in variables)
            {
                if (variable == null) continue;
                if (variable.ArrayDimensions != null
                    && variable.ArrayDimensions.Length == 1
                    && variable.ArrayDimensions[0] <= arrayLimit
                    && variable.ArrayDimensions[0] > maxLimitedArrayLength)
                {
                    maxLimitedArrayLength = variable.ArrayDimensions[0];
                }
                else if (variable.ArrayDimensions != null
                         && (variable.ArrayDimensions.Length > 1
                             || variable.ArrayDimensions.Length == 1 &&
                             variable.ArrayDimensions[0] > arrayLimit)
                         || variable.ValueRank >= ValueRanks.TwoDimensions)
                {
                    continue;
                }

                if (variable.ReadHistory)
                {
                    history = true;
                }

                if (variable.DataType == null || variable.DataType.Raw == null || variable.DataType.Raw.IsNullNodeId)
                {
                    log.LogWarning("Variable datatype is null on id: {Id}", variable.Id);
                    continue;
                }

                var dataType = dataTypes.FirstOrDefault(type => type.Id == variable.DataType.Raw);

                if (dataType == null && missingTypes.Add(variable.DataType.Raw))
                {
                    log.LogWarning("DataType found on node but not in hierarchy, " +
                                "this may mean that some datatypes are defined outside of the main datatype hierarchy: {Type}", variable.DataType);
                    continue;
                }
                if (dataType == null) continue;
                if (identifiedTypes.Contains(dataType)) continue;
                identifiedTypes.Add(dataType);
            }

            log.LogInformation("Found {Count} distinct data-types in detected variables", identifiedTypes.Count);

            foreach (var dataType in identifiedTypes)
            {
                if (dataType.Id.NamespaceIndex == 0)
                {
                    uint identifier = (uint)dataType.Id.Identifier;
                    if ((identifier < DataTypes.Boolean || identifier > DataTypes.Double)
                           && identifier != DataTypes.Integer && identifier != DataTypes.UInteger)
                    {
                        stringVariables = true;
                    }
                }
                else
                {
                    stringVariables = true;
                }
            }

            if (stringVariables)
            {
                log.LogInformation("Variables with string datatype were discovered, and the AllowStringVariables config option " +
                                "will be set to true");
            }
            else if (!baseConfig.Extraction.DataTypes.AllowStringVariables)
            {
                log.LogInformation("No string variables found and the AllowStringVariables option will be set to false");
            }

            if (maxLimitedArrayLength > 0)
            {
                log.LogInformation("Arrays of length {Max} were found, which will be used to set the MaxArraySize option", maxLimitedArrayLength);
            }
            else
            {
                log.LogInformation("No arrays were found, MaxArraySize remains at its current setting, or 0 if unset");
            }

            if (history)
            {
                log.LogInformation("Historizing variables were found, tests on history chunkSizes will be performed later");
            }
            else
            {
                log.LogInformation("No historizing variables were found, tests on history chunkSizes will be skipped");
            }

            Config.Extraction.DataTypes.MaxArraySize = oldArraySize;

            baseConfig.Extraction.DataTypes.AllowStringVariables = baseConfig.Extraction.DataTypes.AllowStringVariables || stringVariables;
            baseConfig.Extraction.DataTypes.MaxArraySize = maxLimitedArrayLength > 0 ? maxLimitedArrayLength : oldArraySize;

            summary.StringVariables = stringVariables;
            summary.MaxArraySize = maxLimitedArrayLength;
        }
        /// <summary>
        /// Internal AllowTSMap, used to check whether a node should be mapped over or not.
        /// </summary>
        /// <param name="node">Node to test</param>
        /// <returns>True if the config tool should keep the variable</returns>
        private bool AllowTSMap(UAVariable node)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));

            if (node.ValueRank == ValueRanks.Scalar) return true;

            if (node.ArrayDimensions == null || node.ArrayDimensions.Length != 1) return false;

            int length = node.ArrayDimensions.First();

            return Config.Extraction.DataTypes.MaxArraySize < 0 || length > 0 && length <= Config.Extraction.DataTypes.MaxArraySize;

        }
        /// <summary>
        /// Attempts different chunk sizes for subscriptions. (number of created monitored items per attempt, 
        /// most servers should support at least one subscription).
        /// </summary>
        public async Task GetSubscriptionChunkSizes(CancellationToken token)
        {
            await PopulateNodes(token);
            await ReadNodeData(token);

            bool failed = true;
            var states = nodeList.Where(node =>
                    (node is UAVariable variable) && !variable.IsProperty
                    && AllowTSMap(variable))
                .Select(node => new VariableExtractionState(this, (node as UAVariable)!, false, false)).ToList();

            log.LogInformation("Get chunkSizes for subscribing to variables");

            if (states.Count == 0)
            {
                log.LogWarning("There are no extractable states, subscriptions will not be tested");
                return;
            }

            var testChunks = testSubscriptionChunkSizes.Where(chunkSize =>
                chunkSize <= states.Count || chunkSize <= 1000);

            if (states.Count < 1000)
            {
                log.LogWarning("There are only {Count} extractable variables, so expected chunksizes may not be accurate. " +
                            "The default is 1000, which generally works.", states.Count);
                summary.SubscriptionLimitWarning = true;
            }

            var dps = new List<UADataPoint>();

            foreach (int chunkSize in testChunks)
            {
                Config.Source.SubscriptionChunk = chunkSize;
                try
                {
                    await ToolUtil.RunWithTimeout(SubscribeToNodes(
                        states.Take(chunkSize),
                        ToolUtil.GetSimpleListWriterHandler(dps, states.ToDictionary(state => state.SourceId), this),
                        token), 120);
                    baseConfig.Source.SubscriptionChunk = chunkSize;
                    failed = false;
                    break;
                }
                catch (Exception e)
                {
                    log.LogError(e, "Failed to subscribe to nodes, retrying with different chunkSize");
                    bool critical = false;
                    try
                    {
                        await ToolUtil.RunWithTimeout(() => Session!.RemoveSubscriptions(Session.Subscriptions.ToList()), 120);
                    }
                    catch (Exception ex)
                    {
                        critical = true;
                        log.LogWarning(ex, "Unable to remove subscriptions, further analysis is not possible");
                    }

                    if (e is ServiceResultException exc && exc.StatusCode == StatusCodes.BadServiceUnsupported)
                    {
                        critical = true;
                        log.LogWarning("CreateMonitoredItems or CreateSubscriptions services unsupported, the extractor " +
                                    "will not be able to properly read datapoints live from this server");
                    }

                    if (critical) break;
                }
            }

            if (failed)
            {
                log.LogWarning("Unable to subscribe to nodes");
                return;
            }

            summary.Subscriptions = true;
            log.LogInformation("Settled on chunkSize: {Size}", baseConfig.Source.SubscriptionChunk);
            log.LogInformation("Waiting for datapoints to arrive...");
            summary.SubscriptionChunkSize = baseConfig.Source.SubscriptionChunk;

            for (int i = 0; i < 50; i++)
            {
                if (dps.Any()) break;
                await Task.Delay(100, token);
            }

            if (dps.Any())
            {
                log.LogInformation("Datapoints arrived, subscriptions confirmed to be working properly");
            }
            else
            {
                log.LogWarning("No datapoints arrived, subscriptions may not be working properly, " +
                            "or there may be no updates on the server");
                summary.SilentSubscriptionsWarning = true;
            }

            await Session!.RemoveSubscriptionsAsync(Session.Subscriptions.ToList());
        }
        /// <summary>
        /// Attempts history read if possible, getting chunk sizes. It also determines granularity, 
        /// and sets backfill to true if it works and it estimates that there are a lot of points in some variables.
        /// </summary>
        public async Task GetHistoryReadConfig(CancellationToken token)
        {
            await PopulateNodes(token);
            await ReadNodeData(token);

            var historizingStates = nodeList.Where(node =>
                    (node is UAVariable variable) && !variable.IsProperty && variable.ReadHistory)
                .Select(node => new VariableExtractionState(this, (node as UAVariable)!, true, true)).ToList();

            var stateMap = historizingStates.ToDictionary(state => state.SourceId);

            log.LogInformation("Read history to decide on decent history settings");

            if (!historizingStates.Any())
            {
                log.LogWarning("No historizing variables detected, unable analyze history");
                summary.NoHistorizingNodes = true;
                return;
            }

            DateTime earliestTime;
            if (Config.History.StartTime == null) earliestTime = CogniteTime.DateTimeEpoch;
            else earliestTime = CogniteTime.ParseTimestampString(Config.History.StartTime)!.Value;

            var details = new ReadRawModifiedDetails
            {
                IsReadModified = false,
                EndTime = DateTime.UtcNow.AddDays(10),
                StartTime = earliestTime,
                NumValuesPerNode = (uint)Config.History.DataChunk
            };

            long largestEstimate = 0;

            long sumDistance = 0;
            int count = 0;

            HistoryReadNode? nodeWithData = null;

            bool failed = true;
            bool done = false;

            foreach (int chunkSize in testHistoryChunkSizes)
            {
                var chunk = historizingStates.Take(chunkSize);
                var historyParams = new HistoryReadParams(
                    chunk.Select(state => new HistoryReadNode(HistoryReadType.FrontfillData, state)).ToList(), details);
                try
                {
                    await ToolUtil.RunWithTimeout(DoHistoryRead(historyParams, token), 10);

                    foreach (var node in historyParams.Items)
                    {
                        if (node.LastResult == null) continue;
                        var data = ToolUtil.ReadResultToDataPoints(node.LastResult, stateMap[node.Id], this);
                        // If we want to do analysis of how best to read history, we need some number of datapoints
                        // If this number is too low, it typically means that there is no real history to read.
                        // Some servers write a single datapoint to history on startup, having a decently large number here
                        // means that we don't base our history analysis on those.
                        if (data.Length > 100 && nodeWithData == null)
                        {
                            nodeWithData = node;
                        }


                        if (data.Length < 2) continue;
                        count++;
                        long avgTicks = (data.Last().Timestamp.Ticks - data.First().Timestamp.Ticks) / (data.Length - 1);
                        sumDistance += avgTicks;

                        if (node.Completed) continue;
                        if (avgTicks == 0) continue;
                        long estimate = (DateTime.UtcNow.Ticks - data.First().Timestamp.Ticks) / avgTicks;
                        if (estimate > largestEstimate)
                        {
                            nodeWithData = node;
                            largestEstimate = estimate;
                        }
                    }


                    failed = false;
                    baseConfig.History.DataNodesChunk = chunkSize;
                    Config.History.DataNodesChunk = chunkSize;
                    done = true;
                }
                catch (Exception e)
                {
                    failed = true;
                    done = false;
                    log.LogWarning(e, "Failed to read history");
                    if (e is ServiceResultException exc && (
                            exc.StatusCode == StatusCodes.BadHistoryOperationUnsupported
                            || exc.StatusCode == StatusCodes.BadServiceUnsupported))
                    {
                        log.LogWarning("History read unsupported, despite Historizing being set to true. " +
                                    "The history config option must be set to false, or this will cause issues");
                        done = true;
                        break;
                    }
                }

                if (done) break;
            }


            if (failed)
            {
                log.LogWarning("Unable to read data history");
                return;
            }

            summary.History = true;
            baseConfig.History.Enabled = true;
            log.LogInformation("Settled on chunkSize: {Size}", baseConfig.History.DataNodesChunk);
            summary.HistoryChunkSize = baseConfig.History.DataNodesChunk;
            log.LogInformation("Largest estimated number of datapoints in a single nodes history is {LargestEstimate}, " +
                            "this is found by looking at the first datapoints, then assuming the average frequency holds until now", largestEstimate);

            if (nodeWithData == null)
            {
                log.LogWarning("No nodes found with more than 100 datapoints in history, further history analysis is not possible");
                return;
            }

            long totalAvgDistance = sumDistance / count;

            log.LogInformation("Average distance between timestamps across all nodes with history: {Distance}",
                TimeSpan.FromTicks(totalAvgDistance));
            var granularity = TimeSpan.FromTicks(totalAvgDistance * 10).Seconds + 1;

            log.LogInformation("Suggested granularity is: {Granularity} seconds", granularity);
            Config.History.Granularity = granularity.ToString();

            summary.HistoryGranularity = TimeSpan.FromSeconds(granularity);

            bool backfillCapable = false;

            log.LogInformation("Read history backwards from {Time}", earliestTime);
            var backfillDetails = new ReadRawModifiedDetails
            {
                IsReadModified = false,
                StartTime = DateTime.UtcNow,
                EndTime = earliestTime,
                NumValuesPerNode = (uint)Config.History.DataChunk
            };

            nodeWithData.ContinuationPoint = null;

            var backfillParams = new HistoryReadParams(new[] { nodeWithData }, backfillDetails);

            try
            {
                await ToolUtil.RunWithTimeout(DoHistoryRead(backfillParams, token), 10);

                var data = ToolUtil.ReadResultToDataPoints(nodeWithData.LastResult!, stateMap[nodeWithData.Id], this);

                log.LogInformation("Last ts: {TimeStamp}", data.First().Timestamp);

                var last = data.First();
                bool orderOk = true;
                foreach (var dp in data)
                {
                    if (dp.Timestamp > last.Timestamp)
                    {
                        orderOk = false;
                    }
                }

                if (!orderOk)
                {
                    log.LogWarning("Backfill does not result in properly ordered results");
                }
                else
                {
                    log.LogInformation("Backfill config results in properly ordered results");
                    backfillCapable = true;
                }
            }
            catch (Exception e)
            {
                log.LogInformation(e, "Failed to perform backfill");
            }

            summary.BackfillRecommended = largestEstimate > 100000 && backfillCapable;

            if ((largestEstimate > 100000 || Config.History.Backfill) && backfillCapable)
            {
                log.LogInformation("Backfill is recommended or manually enabled, and the server is capable");
                baseConfig.History.Backfill = true;
            }
            else
            {
                log.LogInformation("Backfill is not recommended, or the server is incapable");
                baseConfig.History.Backfill = false;
            }

        }

        /// <summary>
        /// Look for emitter relationships, and attempt to listen to events on any identified emitters. Also look through the event hierarchy and find any
        /// custom events that may be interesting for cognite.
        /// Enables events if it seems like the server supports them.
        /// </summary>
        public async Task GetEventConfig(CancellationToken token)
        {
            await PopulateNodes(token);
            await ReadNodeData(token);

            log.LogInformation("Test for event configuration");
            eventTypes.Clear();

            try
            {
                Config.Events.AllEvents = true;
                Config.Events.Enabled = true;
                await GetEventFields(null, token);
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to read event types, the extractor will not be able to support events");
                return;
            }

            var server = await GetServerNode(token);

            var emitters = nodeList.Append(server).Where(node => (node.EventNotifier & EventNotifiers.SubscribeToEvents) != 0);
            var historizingEmitters = emitters.Where(node => (node.EventNotifier & EventNotifiers.HistoryRead) != 0);

            if (emitters.Any())
            {
                log.LogInformation("Discovered {EmitterCount} emitters, of which {HistCount} are historizing", emitters.Count(), historizingEmitters.Count());
                summary.NumEmitters = emitters.Count();
                summary.NumHistEmitters = historizingEmitters.Count();
                summary.AnyEvents = true;
                baseConfig.Events.Enabled = true;
                if (historizingEmitters.Any())
                {
                    summary.HistoricalEvents = true;
                    baseConfig.History.Enabled = true;
                    baseConfig.Events.History = true;
                }
            }

            log.LogInformation("Scan hierarchy for GeneratesEvent references");

            var emitterReferences = new List<UANode>();
            try
            {
                await Browser.BrowseDirectory(nodeList.Select(node => node.Id).Append(ObjectIds.Server).ToList(),
                    ToolUtil.GetSimpleListWriterCallback(emitterReferences, this),
                    token,
                    ReferenceTypeIds.GeneratesEvent, (uint)NodeClass.ObjectType, false);
            }
            catch (Exception ex)
            {
                log.LogWarning(ex, "Failed to look for GeneratesEvent references, this tool will not be able to identify emitted event types this way");
            }

            var referencedEvents = emitterReferences.Select(evt => evt.Id)
                .Distinct().ToHashSet();

            var emittedEvents = referencedEvents.ToList();

            if (emittedEvents.Any())
            {
                log.LogInformation("Identified {Count} events by looking at GeneratesEvent references", emittedEvents.Count);
                bool auditReferences = emitterReferences.Any(evt => evt.ParentId == ObjectIds.Server
                && (evt.Id == ObjectTypeIds.AuditAddNodesEventType || evt.Id == ObjectTypeIds.AuditAddReferencesEventType));

                summary.AnyEvents = true;

                baseConfig.Extraction.EnableAuditDiscovery |= auditReferences;
                summary.Auditing = auditReferences;

                if (auditReferences)
                {
                    log.LogInformation("Audit events on the server node detected, auditing can be enabled");
                }
            }

            if (!summary.Auditing)
            {
                try
                {
                    Session!.Read(
                        null,
                        0,
                        TimestampsToReturn.Neither,
                        new ReadValueIdCollection(new[] { new ReadValueId { NodeId = VariableIds.Server_Auditing, AttributeId = Attributes.Value } }),
                        out var results,
                        out var _
                        );
                    var result = (bool)results.First().GetValue(typeof(bool));
                    if (result)
                    {
                        log.LogInformation("Server capabilities indicate that auditing is enabled");
                        summary.Auditing = true;
                        baseConfig.Extraction.EnableAuditDiscovery = true;
                    }
                }
                catch (Exception ex)
                {
                    log.LogWarning(ex, "Failed to read auditing server configuration");
                }
            }

            if (!emitters.Any() || !historizingEmitters.Any())
            {
                log.LogInformation("No event configuration found");
                return;
            }

            log.LogInformation("Try subscribing to events on emitting nodes");

            var states = emitters.Select(emitter => new EventExtractionState(this, emitter.Id, false, false, true));

            try
            {
                await ToolUtil.RunWithTimeout(SubscribeToEvents(states.Take(baseConfig.Source.SubscriptionChunk), (item, args) => { }, token), 120);
            }
            catch (Exception ex)
            {
                log.LogWarning(ex, "Failed to subscribe to events. The extractor will not be able to support events.");
                return;
            }

            await Session!.RemoveSubscriptionsAsync(Session.Subscriptions.ToList());
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

            summary.NamespaceMap = namespaceMap.Select(kvp => $"{kvp.Key}: {kvp.Value}").ToList();

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

            if (summary.Endpoints.Any())
            {
                log.LogInformation("{Count} endpoints were found: ", summary.Endpoints.Count);
                foreach (var endpoint in summary.Endpoints)
                {
                    log.LogInformation("    {EndPoint}", endpoint);
                }

                if (summary.Secure)
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

            if (summary.BrowseChunk == 0)
            {
                log.LogInformation("Settled on browsing the children of {BrowseNodesChunk} nodes at a time and letting the server decide how many results " +
                                "to return for each request", summary.BrowseNodesChunk);
            }
            else
            {
                log.LogInformation("Settled on browsing the children of {BrowseNodesChunk} nodes at a time and expecting {BrowseChunk} results maximum for each request",
                    summary.BrowseNodesChunk, summary.BrowseChunk);
            }
            log.LogInformation("");

            if (summary.CustomNumTypesCount > 0)
            {
                log.LogInformation("{Count} custom numeric types were discovered", summary.CustomNumTypesCount);
            }
            if (summary.MaxArraySize > 1)
            {
                log.LogInformation("Arrays of size {Size} were discovered", summary.MaxArraySize);
            }
            if (summary.StringVariables)
            {
                log.LogInformation("There are variables that would be mapped to strings in CDF, if this is not correct " +
                                "they may be numeric types that the auto detection did not catch, or they may need to be filtered out");
            }
            if (summary.Enums)
            {
                log.LogInformation("There are variables with enum datatype. These can either be mapped to raw integer values with labels in" +
                    "metadata, or to string timeseries with labels as values.");
            }
            if (summary.CustomNumTypesCount > 0 || summary.MaxArraySize > 0 || summary.StringVariables || summary.Enums)
            {
                log.LogInformation("");
            }

            log.LogInformation("Settled on reading {Count} attributes per Read call", summary.AttributeChunkSize);
            if (summary.VariableLimitWarning)
            {
                log.LogInformation("This is not a completely safe option, as the actual number of attributes is lower than the limit, so if " +
                                "the number of nodes increases in the future, it may fail");
            }
            log.LogInformation("");

            if (summary.Subscriptions)
            {
                log.LogInformation("Successfully subscribed to data variables");
                log.LogInformation("Settled on subscription chunk size: {SubscriptionChunk}", summary.SubscriptionChunkSize);
                if (summary.SubscriptionLimitWarning)
                {
                    log.LogInformation("This is not a completely safe option, as the actual number of extractable nodes is lower than the limit, " +
                                    "so if the number of variables increases in the future, it may fail");
                }

                if (summary.SilentSubscriptionsWarning)
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

            if (summary.History)
            {
                log.LogInformation("Successfully read datapoint history");
                log.LogInformation("Settled on history chunk size {HistoryChunk} with granularity {HistoryGranularity}",
                    summary.HistoryChunkSize, summary.HistoryGranularity);
                if (summary.BackfillRecommended)
                {
                    log.LogInformation("There are large enough amounts of datapoints for certain variables that " +
                                    "enabling backfill is recommended. This increases startup time a bit, but makes the extractor capable of " +
                                    "reading live data and historical data at the same time");
                }
            }
            else if (summary.NoHistorizingNodes)
            {
                log.LogInformation("No historizing nodes detected, the server may support history, but the extractor will only read " +
                                "history from nodes with the Historizing attribute set to true");
            }
            else
            {
                log.LogInformation("The explorer was unable to read history");
            }
            log.LogInformation("");

            if (summary.AnyEvents)
            {
                log.LogInformation("Successfully found support for events on the server");
                log.LogInformation("Found {Count} nodes emitting events", summary.NumEmitters);
                if (summary.HistoricalEvents)
                {
                    log.LogInformation("{Count} historizing event emitters were found", summary.NumHistEmitters);
                }
                if (summary.NumEmitters == 0)
                {
                    log.LogWarning("Found GeneratesEvent references, but no nodes had correctly configured EventNotifier");
                    log.LogWarning("Any emitters must be configured manually");
                }
            }
            else
            {
                log.LogInformation("No regular relevant events were able to be read from the server");
            }

            if (summary.Auditing)
            {
                log.LogInformation("The server likely supports auditing, which may be used to detect addition of nodes and references");
            }
            log.LogInformation("");

            log.LogInformation("The following NamespaceMap was suggested: ");
            foreach (string ns in summary.NamespaceMap)
            {
                log.LogInformation("    {Namespace}", ns);
            }
        }

        public FullConfig FinalConfig => baseConfig;
    }
}
