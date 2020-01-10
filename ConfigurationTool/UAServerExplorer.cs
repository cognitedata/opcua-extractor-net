using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Opc.Ua;
using Serilog;

namespace Cognite.OpcUa.Config
{
    class UAServerExplorer : UAClient
    {
        private readonly FullConfig baseConfig;
        private readonly FullConfig config;
        private List<BufferedNode> dataTypes;
        private List<ProtoDataType> customNumericTypes;
        private List<BufferedNode> nodeList;
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
            100000,
            10000,
            1000,
            100
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
                if (node.NodeClass == NodeClass.Object || node.NodeClass == NodeClass.DataType)
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
        private NodeId GetTopLevelDataType(IEnumerable<BufferedNode> nodes, BufferedNode child)
        {
            var next = child;

            do
            {
                if (next.ParentId == DataTypes.BaseDataType)
                {
                    return next.Id;
                }

                next = nodes.FirstOrDefault(node => node.Id == next.ParentId);
            } while (next != null);
            
            return null;
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
    }
}
