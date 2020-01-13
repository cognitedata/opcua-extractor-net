using System;
using System.Collections.Generic;
using System.Linq;
using System.Security;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Opc.Ua;
using Opc.Ua.Client;
using Serilog;
using Exception = System.Exception;
using KeyValuePair = System.Collections.Generic.KeyValuePair;

namespace Cognite.OpcUa.Config
{
    class UAServerExplorer : UAClient
    {
        private readonly FullConfig baseConfig;
        private readonly FullConfig config;
        private List<BufferedNode> dataTypes;
        private List<ProtoDataType> customNumericTypes;
        private List<BufferedNode> nodeList;
        private List<NodeId> emitterIds;
        private List<NodeId> emittedEvents;
        private List<NodeId> historizingEmitters;
        private Dictionary<string, string> NamespaceMap;
        private Dictionary<NodeId, IEnumerable<(NodeId, QualifiedName)>> activeEventFields;
        private bool history;
        private bool useServer = false;

        private static readonly int serverSizeBase = 125;
        private static readonly int serverWidest = 47;

        private readonly List<(int, int)> testBrowseChunkSizes = new List<(int, int)>
        {
            (1000, 1000),
            (100, 1000),
            (1000, 100),
            (100, 100),
            (1, 1000),
            (1000, 0),
            (1, 0)
        };

        private readonly List<int> testAttributeChunkSizes = new List<int>
        {
            10000,
            1000,
            100,
            10
        };

        private readonly List<int> testSubscriptionChunkSizes = new List<int>
        {
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



        public UAServerExplorer(FullConfig config, FullConfig baseConfig) : base(config)
        {
            this.baseConfig = baseConfig;
            this.config = config;

            this.baseConfig.Source = config.Source;
        }

        public async Task GetEndpoints(CancellationToken token)
        {
            bool failed = false;
            try
            {
                await Run(token);
            }
            catch (Exception ex)
            {
                failed = true;
                Log.Error("Failed to connect to server using initial options");
                Log.Debug(ex, "Failed to connect to endpoint");
            }
            Log.Information("Attempting to list endpoints using given url as discovery server");

            var context = appconfig.CreateMessageContext();
            var endpointConfig = EndpointConfiguration.Create(appconfig);
            var channel = DiscoveryChannel.Create(new Uri(config.Source.EndpointURL), endpointConfig, context);
            DiscoveryClient disc = new DiscoveryClient(channel);
            EndpointDescriptionCollection endpoints = new EndpointDescriptionCollection();
            try
            {
                endpoints = disc.GetEndpoints(null);
            }
            catch (Exception e)
            {
                Log.Warning("Endpoint discovery failed, the given URL may not be a discovery server.");
                Log.Debug(e, "Endpoint discovery failed");
            }

            bool openExists = false;
            bool secureExists = false;

            foreach (var ep in endpoints)
            {
                Log.Information("Endpoint: {url}, Security: {security}", ep.EndpointUrl, ep.SecurityPolicyUri);
                openExists |= ep.SecurityPolicyUri == SecurityPolicies.None;
                secureExists |= ep.SecurityPolicyUri != SecurityPolicies.None;
            }

            if (failed)
            {
                if (!secureExists && !openExists)
                {
                    Log.Information("No endpoint found, make sure the given discovery url is correct");
                } else if (!secureExists && config.Source.Secure)
                {
                    Log.Information("No secure endpoint exists, so connection will fail if Secure is true");
                }
                else if (openExists && config.Source.Secure)
                {
                    Log.Information("Secure connection failed, username or password may be wrong, or the client" +
                                    "may need to be added to a trusted list in the server.");
                    Log.Information("An open endpoint exists, so if secure is set to false and no username/password is provided" +
                                    "connection may succeed");
                }
                else if (!config.Source.Secure && !openExists)
                {
                    Log.Information("Secure is set to false, but no open endpoint exists. Either set secure to true," +
                                    "or add an open endpoint to the server");
                }

                throw new Exception("Fatal: Provided configuration failed to connect to the server");
            }
        }

        private int CountWidestLevel(NodeId root, IList<BufferedNode> nodes)
        {
            int max = 0;

            IEnumerable<NodeId> lastLayer = new List<NodeId> {root}; 
            IEnumerable<NodeId> layer = new List<NodeId>();

            do
            {
                layer = nodes.Where(node => lastLayer.Contains(node.ParentId)).Select(node => node.Id).ToList();
                int count = layer.Count();
                if (count > max)
                {
                    max = count;
                }

                lastLayer = layer;

            } while (layer.Any());

            return max;
        }

        private Action<ReferenceDescription, NodeId> GetSimpleListWriterCallback(List<BufferedNode> target)
        {
            return (node, parentId) =>
            {
                if (node.NodeClass == NodeClass.Object || node.NodeClass == NodeClass.DataType || node.NodeClass == NodeClass.ObjectType)
                {
                    var bufferedNode = new BufferedNode(ToNodeId(node.NodeId),
                        node.DisplayName.Text, parentId);
                    Log.Verbose("HandleNode Object {name}", bufferedNode.DisplayName);
                    target.Add(bufferedNode);
                }
                else if (node.NodeClass == NodeClass.Variable)
                {
                    var bufferedNode = new BufferedVariable(ToNodeId(node.NodeId),
                        node.DisplayName.Text, parentId);
                    if (node.TypeDefinition == VariableTypeIds.PropertyType)
                    {
                        bufferedNode.IsProperty = true;
                    }

                    Log.Verbose("HandleNode Variable {name}", bufferedNode.DisplayName);
                    target.Add(bufferedNode);
                }
            };
        }

        public async Task GetBrowseChunkSizes(CancellationToken token)
        {
            var results = new List<BrowseMapResult>();

            var root = useServer
                ? ObjectIds.Server
                : config.Extraction.RootNode.ToNodeId(this, ObjectIds.ObjectsFolder);

            foreach ((int _browseNodesChunk, int _browseChunk) in testBrowseChunkSizes)
            {
                var nodes = new List<BufferedNode>();

                visitedNodes.Clear();

                var browseNodesChunk = Math.Min(_browseNodesChunk, baseConfig.Source.BrowseNodesChunk);
                var browseChunk = Math.Min(_browseChunk, baseConfig.Source.BrowseChunk);

                if (results.Any(res => res.BrowseNodesChunk == browseNodesChunk && res.BrowseChunk == browseChunk))
                {
                    continue;
                }

                Log.Information("Browse with BrowseNodesChunk: {bnc}, BrowseChunk: {bc}", browseNodesChunk,
                    browseChunk);

                var result = new BrowseMapResult {BrowseNodesChunk = browseNodesChunk, BrowseChunk = browseChunk};

                try
                {
                    await BrowseNodeHierarchy(root, GetSimpleListWriterCallback(nodes), token);
                    result.MaxLevel = CountWidestLevel(root, nodes);
                }
                catch (Exception ex)
                {
                    Log.Warning("Failed to browse node hierarchy");
                    Log.Debug(ex, "Failed to browse nodes");
                    if (ex is ServiceResultException exc && exc.StatusCode == StatusCodes.BadServiceUnsupported)
                    {
                        throw new Exception(
                            "Browse unsupported by server, the extractor does not support servers without support for" +
                            " the \"Browse\" service");
                    }
                    result.failed = true;
                }

                result.NumNodes = nodes.Count;
                results.Add(result);
            }

            var scResults = results.Where(res => !res.failed);
            if (!scResults.Any())
            {
                throw new Exception("No configuration resulted in successful browse, manual configuration may work.");
            }

            var best = scResults.Aggregate((agg, next) =>
                next.NumNodes > agg.NumNodes
                || next.NumNodes == agg.NumNodes && next.BrowseChunk == 0 && agg.BrowseChunk != 0
                || next.NumNodes == agg.NumNodes && next.BrowseChunk == agg.BrowseChunk && next.BrowseNodesChunk > agg.BrowseNodesChunk
                ? next : agg);

            if (best.MaxLevel < best.BrowseNodesChunk)
            {
                Log.Warning("Widest level is smaller than BrowseNodesChunk, so it is not completely safe, the " +
                            "largest known safe value of BrowseNodesChunk is {max}", best.MaxLevel);
                if (best.MaxLevel < serverWidest && !useServer)
                {
                    Log.Information("The server hierarchy is generally wider than this, so retry browse mapping using the " +
                                    "server hierarchy");
                    useServer = true;
                    await GetBrowseChunkSizes(token);
                    return;
                }

                if (best.NumNodes < serverSizeBase && !useServer)
                {
                    Log.Information("The server hierarchy is larger than the main hierarchy, so use the server to identify " +
                                    "attribute chunk later");
                    useServer = true;
                }
            }
            Log.Information("Successfully determined BrowseNodesChunk: {bnc}, BrowseChunk: {bc}", 
                best.BrowseNodesChunk, best.BrowseChunk);
            config.Source.BrowseNodesChunk = best.BrowseNodesChunk;
            config.Source.BrowseChunk = best.BrowseChunk;
            baseConfig.Source.BrowseNodesChunk = best.BrowseNodesChunk;
            baseConfig.Source.BrowseChunk = best.BrowseChunk;

            if (useServer && best.NumNodes < serverSizeBase)
            {
                Log.Warning("The server is smaller than the known number of nodes in the specification defined base hierarchy. " +
                            "This may cause issues later, depending on what parts are missing.");
            }
        }

        public ProtoNodeId NodeIdToProto(NodeId id)
        {
            string nodeidstr = id.ToString();
            string nsstr = $"ns={id.NamespaceIndex};";
            int pos = nodeidstr.IndexOf(nsstr, StringComparison.CurrentCulture);
            if (pos == 0)
            {
                nodeidstr = nodeidstr.Substring(0, pos) + nodeidstr.Substring(pos + nsstr.Length);
            }
            return new ProtoNodeId
            {
                NamespaceUri = session.NamespaceUris.GetString(id.NamespaceIndex),
                NodeId = nodeidstr
            };
        }

        private bool IsChildOf(IEnumerable<BufferedNode> nodes, BufferedNode child, NodeId parent)
        {
            var next = child;

            do
            {
                if (next.ParentId == parent)
                {
                    return true;
                }
                if (next.ParentId == null)
                {
                    break;
                }

                next = nodes.FirstOrDefault(node => node.Id == next.ParentId);
            } while (next != null);

            return false;
        }

        private bool IsCustomObject(NodeId id)
        {
            return id.NamespaceIndex != 0 || id.IdType != IdType.Numeric;
        }

        public void ReadCustomTypes(CancellationToken token)
        {
            dataTypes = new List<BufferedNode>();

            Log.Information("Browsing data type hierarchy for custom datatypes");

            visitedNodes.Clear();

            try
            {
                BrowseDirectory(new List<NodeId> {DataTypes.BaseDataType}, GetSimpleListWriterCallback(dataTypes),
                    token,
                    ReferenceTypeIds.HasSubtype, (uint) NodeClass.DataType);
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed to browse data types");
                throw;
            }

            customNumericTypes = new List<ProtoDataType>();
            foreach (var type in dataTypes)
            {
                string identifier = type.Id.IdType == IdType.String ? (string) type.Id.Identifier : null;
                if (IsCustomObject(type.Id)
                    && identifier != null && ((
                        identifier.Contains("real", StringComparison.InvariantCultureIgnoreCase)
                        || identifier.Contains("integer", StringComparison.InvariantCultureIgnoreCase)
                        || identifier.StartsWith("int", StringComparison.InvariantCultureIgnoreCase)
                        || identifier.Contains("number", StringComparison.InvariantCultureIgnoreCase)
                    )
                    || type.DisplayName != null && (
                        type.DisplayName.Contains("real", StringComparison.InvariantCultureIgnoreCase)
                        || type.DisplayName.Contains("integer", StringComparison.InvariantCultureIgnoreCase)
                        || type.DisplayName.StartsWith("int", StringComparison.InvariantCultureIgnoreCase)
                        || type.DisplayName.Contains("number", StringComparison.InvariantCultureIgnoreCase)
                    )
                    || IsChildOf(dataTypes, type, DataTypes.Number)
                    ))
                {
                    Log.Information("Found potential custom numeric datatype: {id}", type.Id);
                    customNumericTypes.Add(new ProtoDataType
                    {
                        IsStep = identifier.Contains("bool", StringComparison.InvariantCultureIgnoreCase)
                                 || IsChildOf(dataTypes, type, DataTypes.Boolean),
                        NodeId = NodeIdToProto(type.Id)
                    });
                }
            }
            Log.Information("Found {count} custom numeric datatypes", customNumericTypes.Count);
        }

        public async Task GetVariableChunkSizes(CancellationToken token)
        {
            var root = useServer
                ? ObjectIds.Server
                : config.Extraction.RootNode.ToNodeId(this, ObjectIds.ObjectsFolder);

            Log.Information("Reading variable chunk sizes to determine the AttributeChunk property");

            visitedNodes.Clear();

            nodeList = new List<BufferedNode>();

            try
            {
                await BrowseNodeHierarchy(root, GetSimpleListWriterCallback(nodeList), token);
            }
            catch
            {
                Log.Error("Failed to browse node hierarchy");
                throw;
            }

            int oldArraySize = config.Extraction.MaxArraySize;
            var expectedAttributeReads = nodeList.Aggregate(0, (acc, node) => acc + (node.IsVariable ? 5 : 1));
            config.Extraction.MaxArraySize = 10;

            var testChunks = testAttributeChunkSizes.Where(chunkSize =>
                chunkSize <= expectedAttributeReads || chunkSize <= 1000);

            if (expectedAttributeReads < 1000)
            {
                Log.Warning("Reading less than 1000 attributes maximum. Most servers should support more, but" +
                            " this server only has enough variables to read {reads}", expectedAttributeReads);
            }

            bool succeeded = false;

            foreach (var chunkSize in testChunks)
            {
                Log.Information("Attempting to read attributes with ChunkSize {chunkSize}", chunkSize);
                config.Source.AttributesChunk = chunkSize;
                try
                {
                    ReadNodeData(nodeList, token);
                }
                catch (Exception e)
                {
                    Log.Information(e, "Failed to read node attributes");

                    if (e is ServiceResultException exc && exc.StatusCode == StatusCodes.BadServiceUnsupported)
                    {
                        throw new Exception(
                            "Attribute read is unsupported, the extractor does not support servers which do not " +
                            "support the \"Read\" service");
                    }

                    continue;
                }

                Log.Information("Settled on AttributesChunk: {size}", chunkSize);
                succeeded = true;
                baseConfig.Source.AttributesChunk = chunkSize;
                break;
            }

            config.Extraction.MaxArraySize = oldArraySize;

            if (!succeeded)
            {
                throw new Exception("Failed to read node attributes for any chunk size");
            }
        }

        public async Task IdentifyDataTypeSettings(CancellationToken token)
        {
            var root = config.Extraction.RootNode.ToNodeId(this, ObjectIds.ObjectsFolder);

            int arrayLimit = config.Extraction.MaxArraySize == 0 ? 10 : config.Extraction.MaxArraySize;

            config.Extraction.MaxArraySize = 10;

            if (useServer)
            {
                visitedNodes.Clear();
                Log.Information("Filling common node information since this has not been done when identifying attribute chunk size");
                nodeList = new List<BufferedNode>();
                try
                {
                    await BrowseNodeHierarchy(root, GetSimpleListWriterCallback(nodeList), token);
                    ReadNodeData(nodeList, token);
                }
                catch (Exception e)
                {
                    Log.Information(e, "Failed to map out hierarchy");
                    throw;
                }
            }


            Log.Information("Mapping out variable datatypes");

            var variables = nodeList.Where(node =>
                node.IsVariable && (node is BufferedVariable variable) && !variable.IsProperty)
                .Select(node => node as BufferedVariable)
                .Where(node => node != null);



            history = false;
            bool stringVariables = false;
            int maxLimitedArrayLength = 1;

            var identifiedTypes = new List<BufferedNode>();
            foreach (var variable in variables)
            {
                if (variable.ArrayDimensions != null
                    && variable.ArrayDimensions.Length == 1
                    && variable.ArrayDimensions[0] <= arrayLimit
                    && variable.ArrayDimensions[0] > maxLimitedArrayLength)
                {
                    maxLimitedArrayLength = variable.ArrayDimensions[0];
                } else if (variable.ArrayDimensions != null
                           && (variable.ArrayDimensions.Length > 1
                               || variable.ArrayDimensions.Length == 1 &&
                               variable.ArrayDimensions[0] > arrayLimit)
                           || variable.ValueRank >= ValueRanks.TwoDimensions)
                {
                    continue;
                }

                if (variable.Historizing)
                {
                    history = true;
                }

                var dataType = dataTypes.FirstOrDefault(type => type.Id == variable.DataType.raw);
                if (dataType == null)
                {
                    Log.Warning("DataType found on node but not in hierarchy, " +
                                "this may mean that some datatypes are defined outside of the main datatype hierarchy.");
                    continue;
                }

                if (identifiedTypes.Contains(dataType)) continue;
                identifiedTypes.Add(dataType);
            }

            foreach (var dataType in identifiedTypes)
            {
                var identifier = dataType.Id.IdType == IdType.String ? (string)dataType.Id.Identifier : null;
                if (dataType.DisplayName != null
                    && !dataType.DisplayName.Contains("picture", StringComparison.InvariantCultureIgnoreCase)
                    && !dataType.DisplayName.Contains("image", StringComparison.InvariantCultureIgnoreCase)
                    && (identifier == null
                        || !identifier.Contains("picture", StringComparison.InvariantCultureIgnoreCase)
                        && !identifier.Contains("image", StringComparison.InvariantCultureIgnoreCase)
                        )
                    )
                {
                    stringVariables = true;
                }
            }

            if (stringVariables)
            {
                Log.Information("Variables with string datatype were discovered, and the AllowStringVariables config option " +
                                "will be set to true");
            } else if (!baseConfig.Extraction.AllowStringVariables)
            {
                Log.Information("No string variables found and the AllowStringVariables option will be set to false");
            }

            if (maxLimitedArrayLength > 1)
            {
                Log.Information("Arrays of length {max} were found, which will be used to set the MaxArraySize option", maxLimitedArrayLength);
            }
            else
            {
                Log.Information("No arrays were found, MaxArraySize remains at its current setting, or 0 if unset");
            }

            Log.Information(history
                ? "Historizing variables were found, tests on history chunkSizes will be performed later"
                : "No historizing variables were found, tests on history chunkSizes will be skipped");

            baseConfig.Extraction.AllowStringVariables = baseConfig.Extraction.AllowStringVariables || stringVariables;
            baseConfig.Extraction.MaxArraySize = maxLimitedArrayLength > 1 ? maxLimitedArrayLength : arrayLimit;
        }

        private struct BrowseMapResult
        {
            public int MaxLevel;
            public int BrowseNodesChunk;
            public int BrowseChunk;
            public int NumNodes;
            public bool failed;
        }

        private IEnumerable<BufferedDataPoint> ToDataPoint(DataValue value, NodeExtractionState variable, string uniqueId)
        {
            if (variable.ArrayDimensions != null && variable.ArrayDimensions.Length > 0 && variable.ArrayDimensions[0] > 0)
            {
                var ret = new List<BufferedDataPoint>();
                if (!(value.Value is Array))
                {
                    Log.Debug("Bad array datapoint: {BadPointName} {BadPointValue}", uniqueId, value.Value.ToString());
                    return Enumerable.Empty<BufferedDataPoint>();
                }
                var values = (Array)value.Value;
                for (int i = 0; i < Math.Min(variable.ArrayDimensions[0], values.Length); i++)
                {
                    var dp = variable.DataType.IsString
                        ? new BufferedDataPoint(
                            value.SourceTimestamp,
                            $"{uniqueId}[{i}]",
                            ConvertToString(values.GetValue(i)))
                        : new BufferedDataPoint(
                            value.SourceTimestamp,
                            $"{uniqueId}[{i}]",
                            ConvertToDouble(values.GetValue(i)));
                    ret.Add(dp);
                }
                return ret;
            }
            var sdp = variable.DataType.IsString
                ? new BufferedDataPoint(
                    value.SourceTimestamp,
                    uniqueId,
                    ConvertToString(value.Value))
                : new BufferedDataPoint(
                    value.SourceTimestamp,
                    uniqueId,
                    ConvertToDouble(value.Value));
            return new[] { sdp };
        }

        private MonitoredItemNotificationEventHandler GetSimpleListWriterHandler(
            List<BufferedDataPoint> points,
            IDictionary<NodeId, NodeExtractionState> states)
        {
            return (item, args) =>
            {
                string uniqueId = GetUniqueId(item.ResolvedNodeId);
                var state = states[item.ResolvedNodeId];
                foreach (var datapoint in item.DequeueValues())
                {
                    if (StatusCode.IsNotGood(datapoint.StatusCode))
                    {
                        Log.Debug("Bad streaming datapoint: {BadDatapointExternalId} {SourceTimestamp}", uniqueId, datapoint.SourceTimestamp);
                        continue;
                    }
                    var buffDps = ToDataPoint(datapoint, state, uniqueId);
                    state.UpdateFromStream(buffDps);
                    if (!state.IsStreaming) return;
                    foreach (var buffDp in buffDps)
                    {
                        Log.Verbose("Subscription DataPoint {dp}", buffDp.ToDebugDescription());
                    }
                    points.AddRange(buffDps);
                }
            };
        }

        public async Task GetSubscriptionChunkSizes(CancellationToken token)
        {
            bool failed = true;
            var states = nodeList.Where(node =>
                    node.IsVariable && (node is BufferedVariable variable) && !variable.IsProperty)
                .Select(node => new NodeExtractionState(node as BufferedVariable)).ToList();

            Log.Information("Get chunkSizes for subscribing to variables");

            if (states.Count < 1000)
            {
                Log.Warning("There are only {count} extractable variables, so expected chunksizes may not be accurate. " +
                            "The default is 1000, which generally works.", states.Count);
            }

            var dps = new List<BufferedDataPoint>();

            foreach (var chunkSize in testSubscriptionChunkSizes)
            {
                config.Source.SubscriptionChunk = chunkSize;
                try
                {
                    SubscribeToNodes(states, GetSimpleListWriterHandler(dps, states.ToDictionary(state => state.Id)), token);
                    baseConfig.Source.SubscriptionChunk = chunkSize;
                    failed = false;
                    break;
                }
                catch (Exception e)
                {
                    Log.Error(e, "Failed to subscribe to nodes, retrying with different chunkSize");
                    bool critical = false;
                    try
                    {
                        session.RemoveSubscriptions(session.Subscriptions.ToList());
                    }
                    catch (Exception ex)
                    {
                        critical = true;
                        Log.Warning(ex, "Unable to remove subscriptions, further analysis is not possible");
                    }

                    if (e is ServiceResultException exc && exc.StatusCode == StatusCodes.BadServiceUnsupported)
                    {
                        critical = true;
                        Log.Warning("CreateMonitoredItems or CreateSubscriptions services unsupported, the extractor " +
                                    "will not be able to properly read datapoints live from this server");
                    }

                    if (critical) break;
                }
            }

            if (failed)
            {
                Log.Warning("Unable to subscribe to nodes");
                return;
            }
            Log.Information("Settled on chunkSize: {size}", baseConfig.Source.SubscriptionChunk);
            Log.Information("Waiting for datapoints to arrive...");

            for (int i = 0; i < 50; i++)
            {
                if (dps.Any()) break;
                await Task.Delay(200);
            }

            if (dps.Any())
            {
                Log.Information("Datapoints arrived, subscriptions confirmed to be working properly");
            }
            else
            {
                Log.Warning("No datapoints arrived, subscriptions may not be working properly, " +
                            "or there may be no updates on the server");
            }

            session.RemoveSubscriptions(session.Subscriptions.ToList());
        }

        private BufferedDataPoint[] ReadResultToDataPoints(IEncodeable rawData, NodeExtractionState state)
        {
            if (rawData == null) return Array.Empty<BufferedDataPoint>();
            if (!(rawData is HistoryData data))
            {
                Log.Warning("Incorrect result type of history read data");
                return Array.Empty<BufferedDataPoint>();
            }

            if (data.DataValues == null) return Array.Empty<BufferedDataPoint>();
            string uniqueId = GetUniqueId(state.Id);

            var result = new List<BufferedDataPoint>();

            foreach (var datapoint in data.DataValues)
            {
                if (StatusCode.IsNotGood(datapoint.StatusCode))
                {
                    Log.Debug("Bad history datapoint: {BadDatapointExternalId} {SourceTimestamp}", uniqueId,
                        datapoint.SourceTimestamp);
                    continue;
                }

                var buffDps = ToDataPoint(datapoint, state, uniqueId);
                foreach (var buffDp in buffDps)
                {
                    Log.Verbose("History DataPoint {dp}", buffDp.ToDebugDescription());
                    result.Add(buffDp);
                }
            }

            return result.ToArray();
        }

        public void GetHistoryReadConfig()
        {
            var historizingStates = nodeList.Where(node =>
                    node.IsVariable && (node is BufferedVariable variable) && !variable.IsProperty && variable.Historizing)
                .Select(node => new NodeExtractionState(node as BufferedVariable)).ToList();

            var stateMap = historizingStates.ToDictionary(state => state.Id);

            Log.Information("Read history to decide on decent history settings");

            if (!historizingStates.Any())
            {
                Log.Warning("No historizing variables detected, unable analyze history");
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

            foreach (var chunkSize in testHistoryChunkSizes)
            {
                foreach (var chunk in Utils.ChunkBy(historizingStates, chunkSize))
                {
                    var historyParams = new HistoryReadParams(chunk.Select(state => state.Id), details);
                    try
                    {
                        var result = DoHistoryRead(historyParams);

                        foreach (var res in result)
                        {
                            var data = ReadResultToDataPoints(res.Item2, stateMap[res.Item1]);
                            if (data.Length > 10 && nodeWithData == null)
                            {
                                nodeWithData = res.Item1;
                            }


                            if (data.Length < 2) continue;
                            count++;
                            long avgTicks = (data.Last().Timestamp.Ticks - data.First().Timestamp.Ticks) / (data.Length - 1);
                            sumDistance += avgTicks;

                            if (historyParams.Completed[res.Item1]) continue;
                            if (avgTicks == 0) continue;
                            long estimate = (DateTime.UtcNow.Ticks - data.First().Timestamp.Ticks) / avgTicks;
                            if (estimate > largestEstimate)
                            {
                                nodeWithData = res.Item1;
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
                        Log.Warning(e, "Failed to read history");
                        if (e is ServiceResultException exc && (
                                exc.StatusCode == StatusCodes.BadHistoryOperationUnsupported
                                || exc.StatusCode == StatusCodes.BadServiceUnsupported))
                        {
                            Log.Warning("History read unsupported, despite Historizing being set to true. " +
                                        "The history config option must be set to false, or this will cause issues");
                            done = true;
                            break;
                        }
                    }
                }

                if (done) break;
            }


            if (failed)
            {
                Log.Warning("Unable to read data history");
                return;
            }

            Log.Information("Settled on chunkSize: {size}", baseConfig.History.DataNodesChunk);
            Log.Information("Largest estimated number of datapoints in a single nodes history is {largestEstimate}, " +
                            "this is found by looking at the first datapoints, then assuming the average frequency holds until now", largestEstimate);

            if (nodeWithData == null)
            {
                Log.Warning("No nodes found with more than 10 datapoints in history, further history analysis is not possible");
                return;
            }

            var totalAvgDistance = sumDistance / count;

            Log.Information("Average distance between timestamps across all nodes with history: {dist}",
                TimeSpan.FromTicks(totalAvgDistance));
            var granularity = Math.Max(TimeSpan.FromTicks(totalAvgDistance).Seconds, 1) * 10;
            Log.Information("Suggested granularity is: {gran} seconds", granularity);
            config.History.Granularity = granularity;

            bool backfillCapable = false;

            var backfillDetails = new ReadRawModifiedDetails
            {
                IsReadModified = false,
                StartTime = DateTime.UtcNow,
                EndTime = earliestTime,
                NumValuesPerNode = (uint) config.History.DataChunk
            };

            var backfillParams = new HistoryReadParams(new[] {nodeWithData}, backfillDetails);

            try
            {
                var result = DoHistoryRead(backfillParams);

                var data = ReadResultToDataPoints(result.First().Item2, stateMap[result.First().Item1]);

                Log.Information("Last ts: {ts}, {now}", data.First().Timestamp, DateTime.UtcNow);

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
                    Log.Warning("Backfill does not result in properly ordered results");
                }
                else
                {
                    Log.Information("Backfill config results in properly ordered results");
                    backfillCapable = true;
                }
            }
            catch (Exception e)
            {
                Log.Information(e, "Failed to perform backfill");
            }

            if ((largestEstimate > 100000 || config.History.Backfill) && backfillCapable)
            {
                Log.Information("Backfill is recommended or manually enabled, and the server is capable");
                baseConfig.History.Backfill = true;
            }
            else
            {
                Log.Information("Backfill is not recommended, or the server is incapable");
                baseConfig.History.Backfill = false;
            }

        }

        private BufferedEvent ConstructEvent(EventFilter filter, VariantCollection eventFields, NodeId emitter)
        {
            int eventTypeIndex = filter.SelectClauses.FindIndex(atr => atr.TypeDefinitionId == ObjectTypeIds.BaseEventType
                                                                       && atr.BrowsePath[0] == BrowseNames.EventType);
            if (eventTypeIndex < 0)
            {
                Log.Warning("Triggered event has no type, ignoring.");
                return null;
            }
            var eventType = eventFields[eventTypeIndex].Value as NodeId;
            // Many servers don't handle filtering on history data.
            if (eventType == null || !activeEventFields.ContainsKey(eventType))
            {
                Log.Verbose("Invalid event type: {eventType}", eventType);
                return null;
            }
            var targetEventFields = activeEventFields[eventType];

            var extractedProperties = new Dictionary<string, object>();

            for (int i = 0; i < filter.SelectClauses.Count; i++)
            {
                var clause = filter.SelectClauses[i];
                if (!targetEventFields.Any(field =>
                    field.Item1 == clause.TypeDefinitionId
                    && field.Item2 == clause.BrowsePath[0]
                    && clause.BrowsePath.Count == 1)) continue;

                string name = clause.BrowsePath[0].Name;
                if (config.Events.ExcludeProperties.Contains(name) || config.Events.BaseExcludeProperties.Contains(name)) continue;
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
                var buffEvent = new BufferedEvent
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
                    EmittingNode = emitter,
                    ReceivedTime = DateTime.UtcNow,
                };
                return buffEvent;
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed to construct bufferedEvent from raw fields");
                return null;
            }
        }

        private BufferedEvent[] ReadResultToEvents(IEncodeable rawEvts, NodeId emitterId, ReadEventDetails details)
        {
            if (rawEvts == null) return Array.Empty<BufferedEvent>();
            if (!(rawEvts is HistoryEvent evts))
            {
                Log.Warning("Incorrect return type of history read events");
                return Array.Empty<BufferedEvent>();
            }

            var filter = details.Filter;
            if (filter == null)
            {
                Log.Warning("No event filter, ignoring");
                return Array.Empty<BufferedEvent>();
            }

            if (evts.Events == null) return Array.Empty<BufferedEvent>();

            return evts.Events.Select(evt => ConstructEvent(filter, evt.EventFields, emitterId)).ToArray();
        }

        public async Task GetEventConfig(CancellationToken token)
        {
            Log.Information("Test for event configuration");
            var eventTypes = new List<BufferedNode>();

            try
            {
                visitedNodes.Clear();
                BrowseDirectory(new List<NodeId> {ObjectTypeIds.BaseEventType}, GetSimpleListWriterCallback(eventTypes),
                    token,
                    ReferenceTypeIds.HasSubtype, (uint) NodeClass.ObjectType);
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Failed to read event types, the extractor will not be able to support events");
                return;
            }

            var emitterReferences = new List<BufferedNode>();

            Log.Information("Scan hierarchy for GeneratesEvent references");

            try
            {
                visitedNodes.Clear();
                BrowseDirectory(nodeList.Select(node => node.Id).ToList(),
                    GetSimpleListWriterCallback(emitterReferences),
                    token,
                    ReferenceTypeIds.GeneratesEvent, (uint)NodeClass.ObjectType, false);
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "Failed to look for GeneratesEvent references, this tool will not be able to identify non-server emitters");
            }

            visitedNodes.Clear();

            var referencedEvents = emitterReferences.Select(evt => evt.Id)
                .Distinct()
                .Where(id => id.IdType != IdType.Numeric
                             || id.NamespaceIndex > 0
                              || id == ObjectTypeIds.BaseEventType).ToHashSet();

            Log.Information("Referenced emitters: {cnt}", emitterReferences.Count);

            emittedEvents = referencedEvents.ToList();

            emitterIds = emitterReferences
                .Where(evt => referencedEvents.Contains(evt.Id))
                .Select(evt => evt.ParentId).Distinct().ToList();

            Log.Information("Identified {cnt} emitters and {cnt2} events by looking at GeneratesEvent references", 
                emitterIds.Count, emittedEvents.Count);

            bool auditReferences = emitterReferences.Any(evt => evt.ParentId == ObjectIds.Server && (
                    evt.Id == ObjectTypeIds.AuditAddNodesEventType || evt.Id == ObjectTypeIds.AuditAddReferencesEventType));

            baseConfig.Extraction.EnableAuditDiscovery |= auditReferences;
            if (auditReferences)
            {
                Log.Information("Audit events on the server node detected, auditing can be enabled");
            }

            Log.Information("Listening to events on the server node a little while in order to find further events");

            var eventsToListenFor = eventTypes.Where(evt =>
                evt.Id.IdType != IdType.Numeric
                || evt.Id.NamespaceIndex > 0
                || evt.Id == ObjectTypeIds.BaseEventType
                || IsChildOf(eventTypes, evt, ObjectTypeIds.AuditEventType))
                .Select(evt => evt.Id);

            activeEventFields = GetEventFields(eventsToListenFor, token);

            var events = new List<BufferedEvent>();

            SubscribeToEvents(new [] {ObjectIds.Server}, nodeList.Select(node => node.Id).ToList(),
                (item, args) =>
                {
                    if (!(args.NotificationValue is EventFieldList triggeredEvent))
                    {
                        Log.Warning("No event in event subscription notification: {}", item.StartNodeId);
                        return;
                    }
                    var eventFields = triggeredEvent.EventFields;
                    if (!(item.Filter is EventFilter filter))
                    {
                        Log.Warning("Triggered event without filter");
                        return;
                    }
                    var buffEvent = ConstructEvent(filter, eventFields, item.ResolvedNodeId);
                    if (buffEvent == null) return;

                    events.Add(buffEvent);

                    Log.Verbose(buffEvent.ToDebugDescription());

                }, token);

            await Task.Delay(5000);

            session.RemoveSubscriptions(session.Subscriptions.ToList());

            if (!events.Any())
            {
                Log.Information("No events detected after 5 seconds");
            }
            else
            {
                Log.Information("Detected {cnt} events in 5 seconds", events.Count);
                var discoveredEvents = events.Select(evt => evt.EventType).Distinct();


                if (discoveredEvents.Any(id =>
                    IsChildOf(eventTypes, eventTypes.Find(type => type.Id == id), ObjectTypeIds.AuditEventType)))
                {
                    Log.Information("Audit events detected on server node, auditing can be enabled");
                    baseConfig.Extraction.EnableAuditDiscovery = true;
                }

                emittedEvents = emittedEvents.Concat(discoveredEvents).Distinct().ToList();

                if (!emitterIds.Contains(ObjectIds.Server))
                {
                    emitterIds.Add(ObjectIds.Server);
                }
            }

            if (emittedEvents.Any(id => id == ObjectTypeIds.BaseEventType))
            {
                Log.Warning("Using BaseEventType directly is not recommended, consider switching to a custom event type instead.");
            }

            Log.Information("Detected a total of {cnt} emitters and {cnt2} event types",
                emitterIds.Count, emittedEvents.Count);

            Log.Information("Attempt to read historical events for each emitter. Chunk size analysis is not performed here, " +
                            "the results from normal history read is used if available.");

            var earliestTime = DateTimeOffset.FromUnixTimeMilliseconds(config.History.StartTime).DateTime;

            var details = new ReadEventDetails
            {
                EndTime = DateTime.UtcNow.AddDays(10),
                StartTime = earliestTime,
                NumValuesPerNode = 100,
                Filter = BuildEventFilter(nodeList.Select(node => node.Id).ToList())
            };

            historizingEmitters = new List<NodeId>();

            foreach (var emitter in emitterIds)
            {
                var historyParams = new HistoryReadParams(new[] {emitter}, details);

                try
                {
                    // Add timeout failure
                    var resultTask = Task.Run(() => DoHistoryRead(historyParams));
                    var timeoutTask = Task.Delay(5000);

                    await Task.WhenAny(resultTask, timeoutTask);

                    if (!resultTask.IsCompleted) throw new Exception("Timeout");

                    var result = resultTask.Result;

                    var eventResult = ReadResultToEvents(result.First().Item2, emitter, details);

                    if (eventResult.Any())
                    {
                        historizingEmitters.Add(emitter);
                    }
                }
                catch (Exception ex)
                {
                    Log.Information("Unable to read history for {id}", emitter);
                    Log.Debug(ex, "Failed to read history for {id}", emitter);
                }
            }

            Log.Information("Detected {cnt} historizing emitters", historizingEmitters.Count);
        }

        public void GetNamespaceMap()
        {
            var indices = nodeList.Select(node => node.Id.NamespaceIndex).Distinct();

            var namespaces = indices.Select(idx => session.NamespaceUris.GetString(idx));

            var startRegex = new Regex("^.*://");
            var splitRegex = new Regex("[^a-zA-Z\\d]");

            var map = namespaces.ToDictionary(ns => ns, ns =>
                string.Concat(splitRegex.Split(startRegex.Replace(ns, ""))
                    .Where(sub => !string.IsNullOrEmpty(sub))
                    .Select(sub => sub.First()))
            );

            NamespaceMap = new Dictionary<string, string>();

            foreach (var mapped in map)
            {
                var baseValue = mapped.Value;

                var nextValue = baseValue;

                int index = 1;

                while (NamespaceMap.Any(kvp => nextValue == kvp.Value && mapped.Key != kvp.Key))
                {
                    nextValue = baseValue + index;
                    index++;
                }

                NamespaceMap.Add(mapped.Key, nextValue.ToLower());
            }

            Log.Information("Suggested namespaceMap: ");
            foreach (var kvp in NamespaceMap)
            {
                Log.Information("    {key}: {value}", kvp.Key, kvp.Value);
            }
        }
    }
}
