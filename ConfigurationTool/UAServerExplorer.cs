/* Cognite Extractor for OPC-UA
Copyright (C) 2020 Cognite AS

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
using Cognite.OpcUa.HistoryStates;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Opc.Ua;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Config
{
    public class UAServerExplorer : UAClient
    {
        private readonly FullConfig baseConfig;
        private readonly FullConfig config;
        private List<UANode> dataTypes;
        private List<ProtoDataType> customNumericTypes;
        private List<UANode> nodeList;
        private List<NodeId> emittedEvents;
        private List<UANode> eventTypes;
        private Dictionary<string, string> namespaceMap;
        private Dictionary<NodeId, HashSet<EventField>> activeEventFields;

        private bool history;

        private readonly ILogger log = Log.Logger.ForContext(typeof(UAServerExplorer));

        private bool nodesRead;
        private bool dataTypesRead;

        private readonly List<int> testAttributeChunkSizes = new List<int>
        {
            100000,
            10000,
            1000,
            100,
            10
        };

        private readonly List<int> testSubscriptionChunkSizes = new List<int>
        {
            10000,
            1000,
            100,
            10,
            1
        };

        private readonly List<int> testHistoryChunkSizes = new List<int>
        {
            100,
            10,
            1
        };

        public struct Summary
        {
            public List<string> Endpoints;
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
            public bool GenericEvents;
            public int NumEventTypes;
            public bool BaseEventWarning;
            public List<string> NamespaceMap;
            public TimeSpan HistoryGranularity;
            public bool Enums;
            public bool Auditing;
            public bool Subscriptions;
            public bool History;
        }

        private Summary summary;

        public UAServerExplorer(FullConfig config, FullConfig baseConfig) : base(config)
        {
            this.baseConfig = baseConfig ?? new FullConfig();
            this.config = config ?? throw new ArgumentNullException(nameof(config));

            this.baseConfig.Source.EndpointUrl = config.Source.EndpointUrl;
            this.baseConfig.Source.Password = config.Source.Password;
            this.baseConfig.Source.Username = config.Source.Username;
            this.baseConfig.Source.Secure = config.Source.Secure;
        }
        public Summary GetSummary()
        {
            return summary;
        }
        public void ResetSummary()
        {
            summary = new Summary();
        }
        public void ResetNodes()
        {
            nodesRead = false;
            nodeList = new List<UANode>();
            dataTypesRead = false;
            dataTypes = new List<UANode>();
        }

        /// <summary>
        /// Try connecting to the server, and treating it as a discovery server, to list other endpoints on the same server.
        /// </summary>
        public async Task GetEndpoints(CancellationToken token)
        {
            log.Information("Attempting to list endpoints using given url as discovery server");

            var context = Appconfig.CreateMessageContext();
            var endpointConfig = EndpointConfiguration.Create(Appconfig);
            var endpoints = new EndpointDescriptionCollection();
            using (var channel = DiscoveryChannel.Create(new Uri(config.Source.EndpointUrl), endpointConfig, context))
            {
                using var disc = new DiscoveryClient(channel);
                try
                {
                    endpoints = disc.GetEndpoints(null);
                    summary.Endpoints = endpoints.Select(ep => $"{ep.EndpointUrl}: {ep.SecurityPolicyUri}").ToList();
                }
                catch (Exception e)
                {
                    log.Warning("Endpoint discovery failed, the given URL may not be a discovery server.");
                    log.Debug(e, "Endpoint discovery failed");
                }
            }


            bool openExists = false;
            bool secureExists = false;

            foreach (var ep in endpoints)
            {
                log.Information("Endpoint: {url}, Security: {security}", ep.EndpointUrl, ep.SecurityPolicyUri);
                openExists |= ep.SecurityPolicyUri == SecurityPolicies.None;
                secureExists |= ep.SecurityPolicyUri != SecurityPolicies.None;
                summary.Secure = secureExists;
            }

            if (Session == null || !Session.Connected)
            {
                try
                {
                    await Run(token);
                }
                catch (Exception ex)
                {
                    log.Error("Failed to connect to server using initial options");
                    log.Debug(ex, "Failed to connect to endpoint");
                }
            }

            if (Session == null || !Session.Connected)
            {
                if (!secureExists && !openExists)
                {
                    log.Information("No endpoint found, make sure the given discovery url is correct");
                }
                else if (!secureExists && config.Source.Secure)
                {
                    log.Information("No secure endpoint exists, so connection will fail if Secure is true");
                }
                else if (openExists && config.Source.Secure)
                {
                    log.Information("Secure connection failed, username or password may be wrong, or the client" +
                                    "may need to be added to a trusted list in the server.");
                    log.Information("An open endpoint exists, so if secure is set to false and no username/password is provided" +
                                    "connection may succeed");
                }
                else if (!config.Source.Secure && !openExists)
                {
                    log.Information("Secure is set to false, but no open endpoint exists. Either set secure to true," +
                                    "or add an open endpoint to the server");
                }

                throw new FatalException("Fatal: Provided configuration failed to connect to the server");
            }

            Session.KeepAliveInterval = Math.Max(config.Source.KeepAliveInterval, 30000);
        }

        private IEnumerable<UANode> GetTestNodeChunk(int nodesChunk, CancellationToken token)
        {
            var root = ObjectIds.ObjectsFolder;

            // Try to find at least 10000 nodes
            var nodes = new List<UANode>();
            var callback = ToolUtil.GetSimpleListWriterCallback(nodes, this);

            var nextIds = new List<NodeId> { root };

            var localVisitedNodes = new HashSet<NodeId>();
            localVisitedNodes.Add(root);

            int totalChildCount = 0;

            log.Information("Get test node chunk with BrowseNodesChunk {cnt}", nodesChunk);

            do
            {
                var references = new Dictionary<NodeId, ReferenceDescriptionCollection>();
                var total = nextIds.Count;
                int count = 0;
                int countChildren = 0;
                foreach (var chunk in nextIds.ChunkBy(nodesChunk))
                {
                    if (token.IsCancellationRequested) return nodes;
                    var result = GetNodeChildren(chunk, ReferenceTypeIds.HierarchicalReferences,
                        (uint)NodeClass.Object | (uint)NodeClass.Variable, token);
                    foreach (var res in result)
                    {
                        references[res.Key] = res.Value;
                        countChildren += res.Value.Count;
                    }
                    count += result.Count;
                    log.Debug("Read node children {cnt} / {total}. Children: {childcnt}", count, total, countChildren);
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
                            log.Verbose("Discovered new node {nodeid}", nodeId);
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
        public async Task GetBrowseChunkSizes(CancellationToken token)
        {
            if (Session == null || !Session.Connected)
            {
                await Run(token);
            }

            IEnumerable<UANode> testNodes = null;

            int browseChunkSize = 0;

            foreach (int chunkSize in new [] { 1000, 100, 10, 1 }.Where(chunk => chunk <= config.Source.BrowseNodesChunk))
            {
                try
                {
                    testNodes = await ToolUtil.RunWithTimeout(Task.Run(() => GetTestNodeChunk(chunkSize, token)), 60);
                    browseChunkSize = chunkSize;
                    break;
                }
                catch (Exception ex)
                {
                    log.Warning("Failed to browse node hierarchy");
                    log.Debug(ex, "Failed to browse nodes");
                    if (ex is ServiceResultException exc && exc.StatusCode == StatusCodes.BadServiceUnsupported)
                    {
                        throw new FatalException(
                            "Browse unsupported by server, the extractor does not support servers without support for" +
                            " the \"Browse\" service");
                    }
                }
            }
            var parents = testNodes.Select(node => node.ParentId).Distinct().ToList();
            log.Information("Found {cnt} nodes over {cnt2} parents", testNodes.Count(), parents.Count);

            // Test tolerance for large chunks
            log.Information("Testing browseNodesChunk tolerance");
            // We got some indication of max legal size before, here we choose a chunkSize that is reasonable
            // for the discovered server size
            var validSizes = new[] { 10000, 1000, 100, 10, 1 }
                .Where(size => (browseChunkSize == 1000 || size <= browseChunkSize) && size <= testNodes.Count()).ToList();
            foreach (int chunkSize in validSizes)
            {
                var ids = testNodes.Select(node => node.Id).Take(chunkSize).ToList();
                try
                {
                    log.Information("Try to get the children of {cnt} nodes", ids.Count);
                    var children = await ToolUtil.RunWithTimeout(Task.Run(() => GetNodeChildren(ids,
                        ReferenceTypeIds.HierarchicalReferences,
                        (uint)NodeClass.Object | (uint)NodeClass.Variable, token)), 30);
                    break;
                }
                catch (Exception ex)
                {
                    log.Warning("Failed to browse node hierarchy");
                    log.Debug(ex, "Failed to browse nodes");
                }
                browseChunkSize = chunkSize;
            }
            log.Information("Settled on a BrowseNodesChunk of {chunk}", browseChunkSize);
            summary.BrowseNodesChunk = browseChunkSize;

            // Test if there are issues with BrowseNext.
            int originalChunkSize = config.Source.BrowseChunk;
            foreach (int chunkSize in new[] { 10000, 1000, 100, 10, 1 })
            {
                var nodesByParent = testNodes.GroupBy(node => node.ParentId).OrderByDescending(group => group.Count()).Take(browseChunkSize);
                int total = 0;
                var toBrowse = nodesByParent.TakeWhile(chunk =>
                {
                    bool pass = (total + chunk.Count()) <= chunkSize * 2;
                    if (pass)
                    {
                        total += chunk.Count();
                    }
                    return pass;
                }).ToList();

                if (total < chunkSize) continue;

                config.Source.BrowseChunk = chunkSize;
                Dictionary<NodeId, ReferenceDescriptionCollection> children;
                try
                {
                    log.Information("Try to get the children of the {cnt} largest parent nodes, with return chunk size {size}", 
                        toBrowse.Count, chunkSize);
                    children = await ToolUtil.RunWithTimeout(Task.Run(() => GetNodeChildren(toBrowse.Select(group => group.Key),
                        ReferenceTypeIds.HierarchicalReferences,
                        (uint)NodeClass.Object | (uint)NodeClass.Variable, token)), 30);
                }
                catch (Exception ex)
                {
                    log.Warning("Failed to browse node hierarchy");
                    log.Debug(ex, "Failed to browse nodes");
                    continue;
                }
                int childCount = children.Aggregate(0, (seed, kvp) => seed + kvp.Value.Count);
                if (childCount < total)
                {
                    log.Warning("Expected to receive {cnt} nodes but only got {cnt2}!", total, childCount);
                    log.Warning("There is likely an issue with returning large numbers of nodes from the server");
                    summary.BrowseNextWarning = true;
                    int largest = toBrowse.First().Count();
                    // Usually we will have found the largest parent by this point, unless the server is extremely large
                    // So we can try to choose a BrowseNodesChunk that lets us avoid the issue
                    summary.BrowseNodesChunk = Math.Max(1, (int)Math.Floor((double)chunkSize / largest));
                }
                summary.BrowseChunk = Math.Min(chunkSize, originalChunkSize);
                break;
            }
            config.Source.BrowseChunk = summary.BrowseChunk;
            config.Source.BrowseNodesChunk = summary.BrowseNodesChunk;
            baseConfig.Source.BrowseChunk = summary.BrowseChunk;
            baseConfig.Source.BrowseNodesChunk = summary.BrowseNodesChunk;
        }

        private async Task PopulateNodes(CancellationToken token)
        {
            if (nodesRead) return;
            nodeList = new List<UANode>();
            log.Information("Mapping out node hierarchy");
            var root = config.Extraction.RootNode.ToNodeId(this, ObjectIds.ObjectsFolder);
            try
            {
                await BrowseNodeHierarchy(root, ToolUtil.GetSimpleListWriterCallback(nodeList, this), token, false);
                nodesRead = true;
            }
            catch (Exception ex)
            {
                log.Error(ex, "Failed to populate node hierarchy");
                throw;
            }
        }

        private void PopulateDataTypes(CancellationToken token)
        {
            if (dataTypesRead) return;
            dataTypes = new List<UANode>();
            log.Information("Mapping out data type hierarchy");
            try
            {
                BrowseDirectory(
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
                log.Error(ex, "Failed to populate node hierarchy");
                throw;
            }
            dataTypes = dataTypes.Distinct().ToList();
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
                NamespaceUri = Session.NamespaceUris.GetString(id.NamespaceIndex),
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

        private void TestDataType(UANode type)
        {
            if (!IsCustomObject(type.Id)) return;
            uint dataTypeSwitch = 0;
            bool inHierarchy = false;

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
            switch (dataTypeSwitch)
            {
                case DataTypes.Number:
                    log.Information("Found potential numeric type: {id}", type.Id);
                    break;
                case DataTypes.Boolean:
                    log.Information("Found potential boolean type: {id}", type.Id);
                    break;
                case DataTypes.Enumeration:
                    log.Information("Found potential enum type: {id}, consider turning on extraction.enum-as-strings", type.Id);
                    summary.Enums = true;
                    break;
            }
            if (dataTypeSwitch > 0)
            {
                if (inHierarchy)
                {
                    log.Information("DataType {id} is correctly in hierarchy, auto discovery can be used instead", type.Id);
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
                    log.Information("DataType {id} is enum, and auto discovery should be enabled to discover labels", type.Id);
                    baseConfig.Extraction.DataTypes.AutoIdentifyTypes = true;
                }
            }
        }


        /// <summary>
        /// Browse the datatype hierarchy, checking for custom numeric datatypes.
        /// </summary>
        public void ReadCustomTypes(CancellationToken token)
        {
            if (Session == null || !Session.Connected)
            {
                Run(token).Wait();
            }
            PopulateDataTypes(token);

            customNumericTypes = new List<ProtoDataType>();
            foreach (var type in dataTypes)
            {
                TestDataType(type);
            }

            if (!summary.Enums && dataTypes.Any(type => ToolUtil.IsChildOf(dataTypes, type, DataTypes.Enumeration))) summary.Enums = true;

            log.Information("Found {count} custom datatypes outside of normal hierarchy", customNumericTypes.Count);
            summary.CustomNumTypesCount = customNumericTypes.Count;
            baseConfig.Extraction.DataTypes.CustomNumericTypes = customNumericTypes;
        }
        /// <summary>
        /// Get AttributeChunk config value, by attempting to read for various chunk sizes.
        /// </summary>
        public async Task GetAttributeChunkSizes(CancellationToken token)
        {
            log.Information("Reading variable chunk sizes to determine the AttributeChunk property");

            if (Session == null || !Session.Connected)
            {
                await Run(token);
            }

            await PopulateNodes(token);

            int oldArraySize = config.Extraction.DataTypes.MaxArraySize;
            int expectedAttributeReads = nodeList.Aggregate(0, (acc, node) => acc + (node.IsVariable ? 5 : 1));
            config.History.Enabled = true;
            config.Extraction.DataTypes.MaxArraySize = 10;

            var testChunks = testAttributeChunkSizes.Where(chunkSize =>
                chunkSize <= expectedAttributeReads || chunkSize <= 1000);

            if (expectedAttributeReads < 1000)
            {
                log.Warning("Reading less than 1000 attributes maximum. Most servers should support more, but" +
                            " this server only has enough node to read {reads}", expectedAttributeReads);
                summary.VariableLimitWarning = true;
            }

            bool succeeded = false;

            foreach (int chunkSize in testChunks)
            {
                int count = 0;
                var toCheck = nodeList.TakeWhile(node =>
                {
                    count += node.IsVariable ? 5 : 1;
                    return count > chunkSize + 10;
                });
                log.Information("Attempting to read attributes with ChunkSize {chunkSize}", chunkSize);
                config.Source.AttributesChunk = chunkSize;
                try
                {
                    await ToolUtil.RunWithTimeout(() => ReadNodeData(toCheck, token), 120);
                }
                catch (Exception e)
                {
                    log.Information(e, "Failed to read node attributes");

                    if (e is ServiceResultException exc && exc.StatusCode == StatusCodes.BadServiceUnsupported)
                    {
                        throw new FatalException(
                            "Attribute read is unsupported, the extractor does not support servers which do not " +
                            "support the \"Read\" service");
                    }

                    continue;
                }

                log.Information("Settled on AttributesChunk: {size}", chunkSize);
                succeeded = true;
                baseConfig.Source.AttributesChunk = chunkSize;
                break;
            }

            summary.AttributeChunkSize = baseConfig.Source.AttributesChunk;

            config.Extraction.DataTypes.MaxArraySize = oldArraySize;

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
            var root = config.Extraction.RootNode.ToNodeId(this, ObjectIds.ObjectsFolder);

            int oldArraySize = config.Extraction.DataTypes.MaxArraySize;
            int arrayLimit = config.Extraction.DataTypes.MaxArraySize == 0 ? 10 : config.Extraction.DataTypes.MaxArraySize;
            if (arrayLimit < 0) arrayLimit = int.MaxValue;

            config.Extraction.DataTypes.MaxArraySize = 10;

            await PopulateNodes(token);
            PopulateDataTypes(token);

            log.Information("Mapping out variable datatypes");

            var variables = nodeList.Where(node =>
                node.IsVariable && (node is UAVariable variable) && !variable.IsProperty)
                .Select(node => node as UAVariable)
                .Where(node => node != null);

            ReadNodeData(variables, token);

            history = false;
            bool stringVariables = false;
            int maxLimitedArrayLength = 0;

            var identifiedTypes = new List<UANode>();
            var missingTypes = new HashSet<NodeId>();
            foreach (var variable in variables)
            {
                if (variable.ArrayDimensions != null
                    && variable.ArrayDimensions.Count == 1
                    && variable.ArrayDimensions[0] <= arrayLimit
                    && variable.ArrayDimensions[0] > maxLimitedArrayLength)
                {
                    maxLimitedArrayLength = variable.ArrayDimensions[0];
                }
                else if (variable.ArrayDimensions != null
                         && (variable.ArrayDimensions.Count > 1
                             || variable.ArrayDimensions.Count == 1 &&
                             variable.ArrayDimensions[0] > arrayLimit)
                         || variable.ValueRank >= ValueRanks.TwoDimensions)
                {
                    continue;
                }

                if (variable.Historizing)
                {
                    history = true;
                }

                if (variable.DataType == null || variable.DataType.Raw == null || variable.DataType.Raw.IsNullNodeId)
                {
                    log.Warning("Variable datatype is null on id: {id}", variable.Id);
                    continue;
                }

                var dataType = dataTypes.FirstOrDefault(type => type.Id == variable.DataType.Raw);

                if (dataType == null && missingTypes.Add(variable.DataType.Raw))
                {
                    log.Warning("DataType found on node but not in hierarchy, " +
                                "this may mean that some datatypes are defined outside of the main datatype hierarchy: {type}", variable.DataType);
                    continue;
                }

                if (identifiedTypes.Contains(dataType)) continue;
                identifiedTypes.Add(dataType);
            }

            log.Information("Found {cnt} distinct data-types in detected variables", identifiedTypes.Count);

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
                log.Information("Variables with string datatype were discovered, and the AllowStringVariables config option " +
                                "will be set to true");
            }
            else if (!baseConfig.Extraction.DataTypes.AllowStringVariables)
            {
                log.Information("No string variables found and the AllowStringVariables option will be set to false");
            }

            if (maxLimitedArrayLength > 0)
            {
                log.Information("Arrays of length {max} were found, which will be used to set the MaxArraySize option", maxLimitedArrayLength);
            }
            else
            {
                log.Information("No arrays were found, MaxArraySize remains at its current setting, or 0 if unset");
            }

            log.Information(history
                ? "Historizing variables were found, tests on history chunkSizes will be performed later"
                : "No historizing variables were found, tests on history chunkSizes will be skipped");

            config.Extraction.DataTypes.MaxArraySize = oldArraySize;

            baseConfig.Extraction.DataTypes.AllowStringVariables = baseConfig.Extraction.DataTypes.AllowStringVariables || stringVariables;
            baseConfig.Extraction.DataTypes.MaxArraySize = maxLimitedArrayLength > 0 ? maxLimitedArrayLength : oldArraySize;

            summary.StringVariables = stringVariables;
            summary.MaxArraySize = maxLimitedArrayLength;
        }

        private bool AllowTSMap(UAVariable node)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));

            if (node.ValueRank == ValueRanks.Scalar) return true;

            if (node.ArrayDimensions == null || node.ArrayDimensions.Count != 1) return false;

            int length = node.ArrayDimensions.First();

            return config.Extraction.DataTypes.MaxArraySize < 0 || length > 0 && length <= config.Extraction.DataTypes.MaxArraySize;

        }
        /// <summary>
        /// Attempts different chunk sizes for subscriptions. (number of created monitored items per attempt, most servers should support at least one subscription).
        /// </summary>
        public async Task GetSubscriptionChunkSizes(CancellationToken token)
        {
            bool failed = true;
            var states = nodeList.Where(node =>
                    node.IsVariable && (node is UAVariable variable) && !variable.IsProperty
                    && AllowTSMap(variable))
                .Select(node => new VariableExtractionState(this, node as UAVariable, false, false)).ToList();

            log.Information("Get chunkSizes for subscribing to variables");

            if (states.Count == 0)
            {
                log.Warning("There are no extractable states, subscriptions will not be tested");
                return;
            }

            var testChunks = testSubscriptionChunkSizes.Where(chunkSize =>
                chunkSize <= states.Count || chunkSize <= 1000);

            if (states.Count < 1000)
            {
                log.Warning("There are only {count} extractable variables, so expected chunksizes may not be accurate. " +
                            "The default is 1000, which generally works.", states.Count);
                summary.SubscriptionLimitWarning = true;
            }

            var dps = new List<UADataPoint>();

            foreach (int chunkSize in testChunks)
            {
                config.Source.SubscriptionChunk = chunkSize;
                try
                {
                    await ToolUtil.RunWithTimeout(() =>
                        SubscribeToNodes(states.Take(chunkSize),
                            ToolUtil.GetSimpleListWriterHandler(dps, states.ToDictionary(state => state.SourceId), this), token), 120);
                    baseConfig.Source.SubscriptionChunk = chunkSize;
                    failed = false;
                    break;
                }
                catch (Exception e)
                {
                    log.Error(e, "Failed to subscribe to nodes, retrying with different chunkSize");
                    bool critical = false;
                    try
                    {
                        await ToolUtil.RunWithTimeout(() => Session.RemoveSubscriptions(Session.Subscriptions.ToList()), 120);
                    }
                    catch (Exception ex)
                    {
                        critical = true;
                        log.Warning(ex, "Unable to remove subscriptions, further analysis is not possible");
                    }

                    if (e is ServiceResultException exc && exc.StatusCode == StatusCodes.BadServiceUnsupported)
                    {
                        critical = true;
                        log.Warning("CreateMonitoredItems or CreateSubscriptions services unsupported, the extractor " +
                                    "will not be able to properly read datapoints live from this server");
                    }

                    if (critical) break;
                }
            }

            if (failed)
            {
                log.Warning("Unable to subscribe to nodes");
                return;
            }

            summary.Subscriptions = true;
            log.Information("Settled on chunkSize: {size}", baseConfig.Source.SubscriptionChunk);
            log.Information("Waiting for datapoints to arrive...");
            summary.SubscriptionChunkSize = baseConfig.Source.SubscriptionChunk;

            for (int i = 0; i < 50; i++)
            {
                if (dps.Any()) break;
                await Task.Delay(200, token);
            }

            if (dps.Any())
            {
                log.Information("Datapoints arrived, subscriptions confirmed to be working properly");
            }
            else
            {
                log.Warning("No datapoints arrived, subscriptions may not be working properly, " +
                            "or there may be no updates on the server");
                summary.SilentSubscriptionsWarning = true;
            }

            Session.RemoveSubscriptions(Session.Subscriptions.ToList());
        }
        /// <summary>
        /// Attempts history read if possible, getting chunk sizes. It also determines granularity, and sets backfill to true if it works and it estimates that there
        /// are a lot of points in some variables.
        /// </summary>
        public async Task GetHistoryReadConfig()
        {
            var historizingStates = nodeList.Where(node =>
                    node.IsVariable && (node is UAVariable variable) && !variable.IsProperty && variable.Historizing)
                .Select(node => new VariableExtractionState(this, node as UAVariable, true, true)).ToList();

            var stateMap = historizingStates.ToDictionary(state => state.SourceId);

            log.Information("Read history to decide on decent history settings");

            if (!historizingStates.Any())
            {
                log.Warning("No historizing variables detected, unable analyze history");
                summary.NoHistorizingNodes = true;
                return;
            }

            var earliestTime = DateTimeOffset.FromUnixTimeMilliseconds(config.History.StartTime).DateTime;

            var details = new ReadRawModifiedDetails
            {
                IsReadModified = false,
                EndTime = DateTime.UtcNow.AddDays(10),
                StartTime = earliestTime,
                NumValuesPerNode = (uint)config.History.DataChunk
            };

            long largestEstimate = 0;

            long sumDistance = 0;
            int count = 0;

            NodeId nodeWithData = null;

            bool failed = true;
            bool done = false;

            foreach (int chunkSize in testHistoryChunkSizes)
            {
                var chunk = historizingStates.Take(chunkSize);
                var historyParams = new HistoryReadParams(chunk.Select(state => state.SourceId), details);
                try
                {
                    var result = await ToolUtil.RunWithTimeout(() => DoHistoryRead(historyParams), 10);

                    foreach (var (id, rawData) in result)
                    {
                        var data = ToolUtil.ReadResultToDataPoints(rawData, stateMap[id], this);
                        // If we want to do analysis of how best to read history, we need some number of datapoints
                        // If this number is too low, it typically means that there is no real history to read.
                        // Some servers write a single datapoint to history on startup, having a decently large number here
                        // means that we don't base our history analysis on those.
                        if (data.Length > 100 && nodeWithData == null)
                        {
                            nodeWithData = id;
                        }


                        if (data.Length < 2) continue;
                        count++;
                        long avgTicks = (data.Last().Timestamp.Ticks - data.First().Timestamp.Ticks) / (data.Length - 1);
                        sumDistance += avgTicks;

                        if (historyParams.Completed[id]) continue;
                        if (avgTicks == 0) continue;
                        long estimate = (DateTime.UtcNow.Ticks - data.First().Timestamp.Ticks) / avgTicks;
                        if (estimate > largestEstimate)
                        {
                            nodeWithData = id;
                            largestEstimate = estimate;
                        }
                    }


                    failed = false;
                    baseConfig.History.DataNodesChunk = chunkSize;
                    config.History.DataNodesChunk = chunkSize;
                    done = true;
                }
                catch (Exception e)
                {
                    failed = true;
                    done = false;
                    log.Warning(e, "Failed to read history");
                    if (e is ServiceResultException exc && (
                            exc.StatusCode == StatusCodes.BadHistoryOperationUnsupported
                            || exc.StatusCode == StatusCodes.BadServiceUnsupported))
                    {
                        log.Warning("History read unsupported, despite Historizing being set to true. " +
                                    "The history config option must be set to false, or this will cause issues");
                        done = true;
                        break;
                    }
                }

                if (done) break;
            }


            if (failed)
            {
                log.Warning("Unable to read data history");
                return;
            }

            summary.History = true;
            baseConfig.History.Enabled = true;
            log.Information("Settled on chunkSize: {size}", baseConfig.History.DataNodesChunk);
            summary.HistoryChunkSize = baseConfig.History.DataNodesChunk;
            log.Information("Largest estimated number of datapoints in a single nodes history is {largestEstimate}, " +
                            "this is found by looking at the first datapoints, then assuming the average frequency holds until now", largestEstimate);

            if (nodeWithData == null)
            {
                log.Warning("No nodes found with more than 100 datapoints in history, further history analysis is not possible");
                return;
            }

            long totalAvgDistance = sumDistance / count;

            log.Information("Average distance between timestamps across all nodes with history: {dist}",
                TimeSpan.FromTicks(totalAvgDistance));
            var granularity = Math.Max(TimeSpan.FromTicks(totalAvgDistance).Seconds, 1) * 10;
            log.Information("Suggested granularity is: {gran} seconds", granularity);
            config.History.Granularity = granularity;
            summary.HistoryGranularity = TimeSpan.FromSeconds(granularity);

            bool backfillCapable = false;

            var backfillDetails = new ReadRawModifiedDetails
            {
                IsReadModified = false,
                StartTime = DateTime.UtcNow,
                EndTime = earliestTime,
                NumValuesPerNode = (uint)config.History.DataChunk
            };

            var backfillParams = new HistoryReadParams(new[] { nodeWithData }, backfillDetails);

            try
            {
                var result = await ToolUtil.RunWithTimeout(() => DoHistoryRead(backfillParams), 10);

                var data = ToolUtil.ReadResultToDataPoints(result.First().RawData, stateMap[result.First().Id], this);

                log.Information("Last ts: {ts}, {now}", data.First().Timestamp, DateTime.UtcNow);

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
                    log.Warning("Backfill does not result in properly ordered results");
                }
                else
                {
                    log.Information("Backfill config results in properly ordered results");
                    backfillCapable = true;
                }
            }
            catch (Exception e)
            {
                log.Information(e, "Failed to perform backfill");
            }

            summary.BackfillRecommended = largestEstimate > 100000 && backfillCapable;

            if ((largestEstimate > 100000 || config.History.Backfill) && backfillCapable)
            {
                log.Information("Backfill is recommended or manually enabled, and the server is capable");
                baseConfig.History.Backfill = true;
            }
            else
            {
                log.Information("Backfill is not recommended, or the server is incapable");
                baseConfig.History.Backfill = false;
            }

        }
        private UAEvent ConstructEvent(EventFilter filter, VariantCollection eventFields, NodeId emitter)
        {
            int eventTypeIndex = filter.SelectClauses.FindIndex(atr => atr.TypeDefinitionId == ObjectTypeIds.BaseEventType
                                                                       && atr.BrowsePath[0] == BrowseNames.EventType);
            if (eventTypeIndex < 0)
            {
                log.Warning("Triggered event has no type, ignoring.");
                return null;
            }
            var eventType = eventFields[eventTypeIndex].Value as NodeId;
            // Many servers don't handle filtering on history data.
            if (eventType == null || !activeEventFields.ContainsKey(eventType))
            {
                log.Verbose("Invalid event type: {eventType}", eventType);
                return null;
            }
            var targetEventFields = activeEventFields[eventType];

            var extractedProperties = new Dictionary<string, object>();

            for (int i = 0; i < filter.SelectClauses.Count; i++)
            {
                var clause = filter.SelectClauses[i];
                if (clause.BrowsePath.Count != 1
                    || !targetEventFields.Contains(new EventField(clause.TypeDefinitionId, clause.BrowsePath[0]))) continue;

                string name = clause.BrowsePath[0].Name;
                if (config.Events.DestinationNameMap.ContainsKey(name) && name != "EventId" && name != "SourceNode" && name != "EventType")
                {
                    name = config.Events.DestinationNameMap[name];
                }
                if (!extractedProperties.ContainsKey(name) || extractedProperties[name] == null)
                {
                    extractedProperties[name] = eventFields[i].Value;
                }
            }
            try
            {
                var buffEvent = new UAEvent
                {
                    Message = ConvertToString(extractedProperties.GetValueOrDefault("Message")),
                    EventId = config.Extraction.IdPrefix + Convert.ToBase64String((byte[])extractedProperties["EventId"]),
                    SourceNode = (NodeId)extractedProperties["SourceNode"],
                    Time = (DateTime)extractedProperties.GetValueOrDefault("Time"),
                    EventType = (NodeId)extractedProperties["EventType"],
                    MetaData = extractedProperties
                        .Where(kvp => kvp.Key != "Message" && kvp.Key != "EventId" && kvp.Key != "SourceNode"
                                      && kvp.Key != "Time" && kvp.Key != "EventType")
                        .ToDictionary(kvp => kvp.Key, kvp => kvp.Value),
                    EmittingNode = emitter
                };
                return buffEvent;
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to construct bufferedEvent from Raw fields");
                return null;
            }
        }

        private IEnumerable<UAEvent> ReadResultToEvents(IEncodeable rawEvts, NodeId emitterId, ReadEventDetails details)
        {
            if (rawEvts == null) return Array.Empty<UAEvent>();
            if (!(rawEvts is HistoryEvent evts))
            {
                log.Warning("Incorrect return type of history read events");
                return Array.Empty<UAEvent>();
            }

            var filter = details.Filter;
            if (filter == null)
            {
                log.Warning("No event filter, ignoring");
                return Array.Empty<UAEvent>();
            }

            if (evts.Events == null) return Array.Empty<UAEvent>();

            return evts.Events.Select(evt => ConstructEvent(filter, evt.EventFields, emitterId)).ToArray();
        }
        /// <summary>
        /// Look for emitter relationships, and attempt to listen to events on any identified emitters. Also look through the event hierarchy and find any
        /// custom events that may be interesting for cognite. Then attempt to listen for audit events on the server node, if any at all are detected
        /// assume that the server is auditing.
        /// </summary>
        public async Task GetEventConfig(CancellationToken token)
        {
            log.Information("Test for event configuration");
            eventTypes = new List<UANode>();

            try
            {
                VisitedNodes.Clear();
                BrowseDirectory(new List<NodeId> { ObjectTypeIds.BaseEventType }, ToolUtil.GetSimpleListWriterCallback(eventTypes, this),
                    token,
                    ReferenceTypeIds.HasSubtype, (uint)NodeClass.ObjectType);
            }
            catch (Exception ex)
            {
                log.Error(ex, "Failed to read event types, the extractor will not be able to support events");
                return;
            }


            var notifierPairs = new List<(NodeId id, byte notifier)>();

            log.Information("Read EventNotifier property for each node");

            try
            {
                var readValueIds = nodeList.Select(node => new ReadValueId
                {
                    AttributeId = Attributes.EventNotifier,
                    NodeId = node.Id
                }).Append(new ReadValueId { AttributeId = Attributes.EventNotifier, NodeId = ObjectIds.Server });
                foreach (var chunk in readValueIds.ChunkBy(baseConfig.Source.AttributesChunk))
                {
                    Session.Read(
                        null,
                        0,
                        TimestampsToReturn.Neither,
                        new ReadValueIdCollection(chunk),
                        out var results,
                        out var _
                        );
                    var chunkEnum = chunk.GetEnumerator();
                    foreach (var result in results)
                    {
                        chunkEnum.MoveNext();
                        notifierPairs.Add((chunkEnum.Current.NodeId, result.GetValue((byte)0)));
                    }
                }
            }
            catch (Exception ex)
            {
                log.Warning(ex, "Failed to read EventNotifier property, the server most likely does not support events");
                return;
            }

            var emitters = notifierPairs.Where(pair => (pair.notifier & EventNotifiers.SubscribeToEvents) != 0);
            var historizingEmitters = emitters.Where(pair => (pair.notifier & EventNotifiers.HistoryRead) != 0);

            if (emitters.Any())
            {
                log.Information("Discovered {cnt} emitters, of which {cnt2} are historizing", emitters.Count(), historizingEmitters.Count());
                if (historizingEmitters.Any())
                {
                    summary.HistoricalEvents = true;
                    baseConfig.History.Enabled = true;
                    baseConfig.Events.History = true;
                }
            }

            log.Information("Scan hierarchy for GeneratesEvent references");

            var emitterReferences = new List<UANode>();
            try
            {
                VisitedNodes.Clear();
                BrowseDirectory(nodeList.Select(node => node.Id).Append(ObjectIds.Server).ToList(),
                    ToolUtil.GetSimpleListWriterCallback(emitterReferences, this),
                    token,
                    ReferenceTypeIds.GeneratesEvent, (uint)NodeClass.ObjectType, false);
            }
            catch (Exception ex)
            {
                log.Warning(ex, "Failed to look for GeneratesEvent references, this tool will not be able to identify emitted event types this way");
            }

            VisitedNodes.Clear();

            var referencedEvents = emitterReferences.Select(evt => evt.Id)
                .Distinct()
                .Where(id => IsCustomObject(id) || id == ObjectTypeIds.BaseEventType).ToHashSet();

            emittedEvents = referencedEvents.ToList();


            log.Information("Identified {cnt} events by looking at GeneratesEvent references", emittedEvents.Count);

            bool auditReferences = emitterReferences.Any(evt => evt.ParentId == ObjectIds.Server
                && (evt.Id == ObjectTypeIds.AuditAddNodesEventType || evt.Id == ObjectTypeIds.AuditAddReferencesEventType));

            baseConfig.Extraction.EnableAuditDiscovery |= auditReferences;
            summary.Auditing = auditReferences;

            if (auditReferences)
            {
                log.Information("Audit events on the server node detected, auditing can be enabled");
            }

            log.Information("Listening to events on the server node a little while in order to find further events");

            config.Events.AllEvents = true;
            config.Events.Enabled = true;

            activeEventFields = GetEventFields(token);

            var events = new List<UAEvent>();

            object listLock = new object();

            try
            {
                await ToolUtil.RunWithTimeout(() => SubscribeToEvents(new[] { new EventExtractionState(this, ObjectIds.Server, false, false) },
                    (item, args) =>
                    {
                        if (!(args.NotificationValue is EventFieldList triggeredEvent))
                        {
                            log.Warning("No event in event subscription notification: {}", item.StartNodeId);
                            return;
                        }
                        var eventFields = triggeredEvent.EventFields;
                        if (!(item.Filter is EventFilter filter))
                        {
                            log.Warning("Triggered event without filter");
                            return;
                        }
                        var buffEvent = ConstructEvent(filter, eventFields, item.ResolvedNodeId);
                        if (buffEvent == null) return;

                        lock (listLock)
                        {
                            events.Add(buffEvent);
                        }

                        log.Verbose(buffEvent.ToString());

                    }, token), 120);
            }
            catch (Exception ex)
            {
                log.Warning(ex, "Failed to subscribe to events. The extractor will not be able to support events.");
                return;
            }

            try
            {
                await ToolUtil.RunWithTimeout(() => SubscribeToAuditEvents(
                    (item, args) =>
                    {
                        if (!(args.NotificationValue is EventFieldList triggeredEvent))
                        {
                            log.Warning("No event in event subscription notification: {}", item.StartNodeId);
                            return;
                        }

                        var eventFields = triggeredEvent.EventFields;
                        if (!(item.Filter is EventFilter filter))
                        {
                            log.Warning("Triggered event without filter");
                            return;
                        }
                        int eventTypeIndex = filter.SelectClauses.FindIndex(atr => atr.TypeDefinitionId == ObjectTypeIds.BaseEventType
                                                                                    && atr.BrowsePath[0] == BrowseNames.EventType);
                        if (eventTypeIndex < 0)
                        {
                            log.Warning("Triggered event has no type, ignoring");
                            return;
                        }
                        var eventType = eventFields[eventTypeIndex].Value as NodeId;
                        if (eventType == null || eventType != ObjectTypeIds.AuditAddNodesEventType && eventType != ObjectTypeIds.AuditAddReferencesEventType)
                        {
                            log.Warning("Non-audit event triggered on audit event listener");
                            return;
                        }

                        var buffEvent = new UAEvent
                        {
                            EventType = eventType
                        };
                        lock (listLock)
                        {
                            events.Add(buffEvent);
                        }

                        log.Verbose(buffEvent.ToString());

                    }), 120);
            }
            catch (Exception ex)
            {
                log.Warning(ex, "Failed to subscribe to audit events. The extractor will not be able to support auditing.");
            }


            await Task.Delay(5000, token);

            Session.RemoveSubscriptions(Session.Subscriptions.ToList());

            if (!events.Any())
            {
                log.Information("No events detected after 5 seconds");
            }
            else
            {
                log.Information("Detected {cnt} events in 5 seconds", events.Count);
                var discoveredEvents = events.Select(evt => evt.EventType).Distinct();

                if (discoveredEvents.Any(id =>
                    ToolUtil.IsChildOf(eventTypes, eventTypes.Find(type => type.Id == id), ObjectTypeIds.AuditEventType)))
                {
                    log.Information("Audit events detected on server node, auditing can be enabled");
                    baseConfig.Extraction.EnableAuditDiscovery = true;
                    summary.Auditing = true;
                }

                emittedEvents = emittedEvents
                    .Concat(discoveredEvents)
                    .Distinct()
                    .Where(id => !ToolUtil.IsChildOf(eventTypes, eventTypes.Find(type => type.Id == id), ObjectTypeIds.AuditEventType))
                    .ToList();
            }

            if (emittedEvents.Any(id => id == ObjectTypeIds.BaseEventType))
            {
                log.Warning("Using BaseEventType directly is not recommended, consider switching to a custom event type instead.");
                summary.BaseEventWarning = true;
            }

            log.Information("Detected a total of {cnt} event types", emittedEvents.Count);

            if (emittedEvents.Count > 0)
            {
                log.Information("Detected potential events, the server should discover events");
                baseConfig.Events.Enabled = true;
                summary.NumEventTypes = emittedEvents.Count;
                if (emittedEvents.Any(id => id.NamespaceIndex == 0))
                {
                    log.Information("Detected non-custom events");
                    baseConfig.Events.AllEvents = true;
                    summary.GenericEvents = true;
                }
            }

            try
            {
                Session.Read(
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
                    log.Information("Server capabilities indicate that auditing is enabled");
                    summary.Auditing = true;
                }
            }
            catch (Exception ex)
            {
                log.Warning(ex, "Failed to read auditing server configuration");
            }

            summary.NumEventTypes = emittedEvents.Count;
        }

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

#pragma warning disable CA1308 // Normalize strings to uppercase. Lowercase is prettier in externalId.
                while (namespaceMap.Any(kvp => nextValue.ToLowerInvariant() == kvp.Value && mapped.Key != kvp.Key))
                {
                    nextValue = baseValue + index;
                    index++;
                }

                namespaceMap.Add(mapped.Key, nextValue.ToLowerInvariant());
#pragma warning restore CA1308 // Normalize strings to uppercase
            }

            foreach (string key in namespaceMap.Keys.ToList())
            {
                namespaceMap[key] += ":";
            }

            return namespaceMap;
        }
        /// <summary>
        /// Generate an intelligent namespace-map, with unique values, base for the base opcfoundation namespace (I think that appears in most servers).
        /// </summary>
        public void GetNamespaceMap()
        {
            var indices = nodeList.Concat(dataTypes).Concat(eventTypes).Select(node => node.Id.NamespaceIndex).Distinct();

            var namespaces = indices.Select(idx => Session.NamespaceUris.GetString(idx));

            namespaceMap = GenerateNamespaceMap(namespaces);

            log.Information("Suggested namespaceMap: ");
            foreach (var kvp in namespaceMap)
            {
                log.Information("    {key}: {value}", kvp.Key, kvp.Value);
            }

            summary.NamespaceMap = namespaceMap.Select(kvp => $"{kvp.Key}: {kvp.Value}").ToList();

            baseConfig.Extraction.NamespaceMap = namespaceMap;
        }
        /// <summary>
        /// Log a summary of the run.
        /// </summary>
        public void LogSummary()
        {
            log.Information("");
            log.Information("Server analysis successfully completed, no critical issues were found");
            log.Information("==== SUMMARY ====");
            log.Information("");

            if (summary.Endpoints.Any())
            {
                log.Information("{cnt} endpoints were found: ", summary.Endpoints.Count);
                foreach (var endpoint in summary.Endpoints)
                {
                    log.Information("    {ep}", endpoint);
                }

                log.Information(summary.Secure
                    ? "At least one of these are secure, meaning that the Secure config option can and should be enabled"
                    : "None of these are secure, so enabling the Secure config option will probably not work.");
            }
            else
            {
                log.Information("No endpoints were found, but the client was able to connect. This is not necessarily an issue, " +
                                "but there may be a different discovery URL connected to the server that exposes further endpoints.");
            }
            log.Information("");

            if (summary.BrowseChunk == 0)
            {
                log.Information("Settled on browsing the children of {bnc} nodes at a time and letting the server decide how many results " +
                                "to return for each request", summary.BrowseNodesChunk);
            }
            else
            {
                log.Information("Settled on browsing the children of {bnc} nodes at a time and expecting {bc} results maximum for each request",
                    summary.BrowseNodesChunk, summary.BrowseChunk);
            }
            log.Information("");

            if (summary.CustomNumTypesCount > 0)
            {
                log.Information("{cnt} custom numeric types were discovered", summary.CustomNumTypesCount);
            }
            if (summary.MaxArraySize > 1)
            {
                log.Information("Arrays of size {size} were discovered", summary.MaxArraySize);
            }
            if (summary.StringVariables)
            {
                log.Information("There are variables that would be mapped to strings in CDF, if this is not correct " +
                                "they may be numeric types that the auto detection did not catch, or they may need to be filtered out");
            }
            if (summary.Enums)
            {
                log.Information("There are variables with enum datatype. These can either be mapped to raw integer values with labels in" +
                    "metadata, or to string timeseries with labels as values.");
            }
            if (summary.CustomNumTypesCount > 0 || summary.MaxArraySize > 0 || summary.StringVariables || summary.Enums)
            {
                log.Information("");
            }

            log.Information("Settled on reading {cnt} attributes per Read call", summary.AttributeChunkSize);
            if (summary.VariableLimitWarning)
            {
                log.Information("This is not a completely safe option, as the actual number of attributes is lower than the limit, so if " +
                                "the number of nodes increases in the future, it may fail");
            }
            log.Information("");

            if (summary.Subscriptions)
            {
                log.Information("Successfully subscribed to data variables");
                log.Information("Settled on subscription chunk size: {chunk}", summary.SubscriptionChunkSize);
                if (summary.SubscriptionLimitWarning)
                {
                    log.Information("This is not a completely safe option, as the actual number of extractable nodes is lower than the limit, " +
                                    "so if the number of variables increases in the future, it may fail");
                }

                if (summary.SilentSubscriptionsWarning)
                {
                    log.Information("Though subscriptions were successfully created, no data was received. This may be an issue if " +
                                    "data is expected to appear within a five second window");
                }
            }
            else
            {
                log.Information("The explorer was unable to subscribe to data variables, because none exist or due to a server issue");
            }
            log.Information("");

            if (summary.History)
            {
                log.Information("Successfully read datapoint history");
                log.Information("Settled on history chunk size {chunk} with granularity {g}",
                    summary.HistoryChunkSize, summary.HistoryGranularity);
                if (summary.BackfillRecommended)
                {
                    log.Information("There are large enough amounts of datapoints for certain variables that " +
                                    "enabling backfill is recommended. This increases startup time a bit, but makes the extractor capable of " +
                                    "reading live data and historical data at the same time");
                }
            }
            else if (summary.NoHistorizingNodes)
            {
                log.Information("No historizing nodes detected, the server may support history, but the extractor will only read " +
                                "history from nodes with the Historizing attribute set to true");
            }
            else
            {
                log.Information("The explorer was unable to read history");
            }
            log.Information("");

            if (summary.NumEventTypes > 0)
            {
                log.Information("Successfully found support for events on the server");
                log.Information("{types} different event types were found", summary.NumEventTypes);
                if (summary.HistoricalEvents)
                {
                    log.Information("Historizing event emitters were found and successfully read from");
                }

                if (summary.BaseEventWarning)
                {
                    log.Information("BaseEventType events were observed to be emitted directly. This is not ideal, it is better to " +
                                    "only use custom or predefined event types.");
                }
            }
            else
            {
                log.Information("No regular relevant events were able to be read from the server");
            }

            if (summary.Auditing)
            {
                log.Information("The server likely supports auditing, which may be used to detect addition of nodes and references");
            }
            log.Information("");

            log.Information("The following NamespaceMap was suggested: ");
            foreach (string ns in summary.NamespaceMap)
            {
                log.Information("    {ns}", ns);
            }
        }

        public FullConfig GetFinalConfig()
        {
            return baseConfig;
        }
    }
}
