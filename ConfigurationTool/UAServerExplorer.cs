/* Cognite Extractor for OPC-UA
Copyright (C) 2019 Cognite AS

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
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Opc.Ua;
using Serilog;

namespace Cognite.OpcUa.Config
{
    public class UAServerExplorer : UAClient
    {
        private readonly FullConfig baseConfig;
        private readonly FullConfig config;
        private List<BufferedNode> dataTypes;
        private List<ProtoDataType> customNumericTypes;
        private List<BufferedNode> nodeList;
        private List<NodeId> emitterIds;
        private List<NodeId> emittedEvents;
        private List<BufferedNode> eventTypes;
        private Dictionary<string, string> namespaceMap;
        private Dictionary<NodeId, IEnumerable<(NodeId Root, QualifiedName BrowseName)>> activeEventFields;
        private bool history;
        private bool useServer;

        private const int ServerSizeBase = 125;
        private const int ServerWidest = 47;

        private readonly ILogger log = Log.Logger.ForContext(typeof(UAServerExplorer));

        private readonly List<(int, int)> testBrowseChunkSizes = new List<(int, int)>
        {
            (1000, 0),
            (1000, 1000),
            (1000, 100),
            (100, 1000),
            (100, 100),
            (1, 1000),
            (1, 0),
        };

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

        private struct Summary
        {
            public List<string> Endpoints;
            public bool Secure;
            public int BrowseNodesChunk;
            public int BrowseChunk;
            public bool BrowseLimitWarning;
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
            public int NumEventTypes;
            public int NumEmitters;
            public bool BaseEventWarning;
            public int NumHistorizingEmitters;
            public List<string> NamespaceMap;
            public TimeSpan HistoryGranularity;

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
        /// <summary>
        /// Try connecting to the server, and treating it as a discovery server, to list other endpoints on the same server.
        /// </summary>
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
                log.Error("Failed to connect to server using initial options");
                log.Debug(ex, "Failed to connect to endpoint");
            }
            Session.KeepAliveInterval = Math.Max(config.Source.KeepAliveInterval, 30000);
            log.Information("Attempting to list endpoints using given url as discovery server");

            var context = Appconfig.CreateMessageContext();
            var endpointConfig = EndpointConfiguration.Create(Appconfig);
            using var channel = DiscoveryChannel.Create(new Uri(config.Source.EndpointUrl), endpointConfig, context);
            using var disc = new DiscoveryClient(channel);
            var endpoints = new EndpointDescriptionCollection();
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

            bool openExists = false;
            bool secureExists = false;

            foreach (var ep in endpoints)
            {
                log.Information("Endpoint: {url}, Security: {security}", ep.EndpointUrl, ep.SecurityPolicyUri);
                openExists |= ep.SecurityPolicyUri == SecurityPolicies.None;
                secureExists |= ep.SecurityPolicyUri != SecurityPolicies.None;
                summary.Secure = secureExists;
            }

            if (failed)
            {
                if (!secureExists && !openExists)
                {
                    log.Information("No endpoint found, make sure the given discovery url is correct");
                } else if (!secureExists && config.Source.Secure)
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
        }
        /// <summary>
        /// Browse multiple times with different combination of BrowseChunk and BrowseNodesChunk, to determine them.
        /// Uses the result with the greatest successfull values for BrowseChunk and BrowseNodesChunk, with the greatest number of received nodes.
        /// Will read from the server hierarchy if the number of nodes in the main hierarchy is too small.
        /// </summary>
        public async Task GetBrowseChunkSizes(CancellationToken token)
        {
            var results = new List<BrowseMapResult>();

            var root = useServer
                ? ObjectIds.Server
                : config.Extraction.RootNode.ToNodeId(this, ObjectIds.ObjectsFolder);

            foreach ((int lbrowseNodesChunk, int lbrowseChunk) in testBrowseChunkSizes)
            {
                var nodes = new List<BufferedNode>();

                VisitedNodes.Clear();

                int browseNodesChunk = Math.Min(lbrowseNodesChunk, baseConfig.Source.BrowseNodesChunk);
                int browseChunk = Math.Min(lbrowseChunk, baseConfig.Source.BrowseChunk);

                if (results.Any(res => res.BrowseNodesChunk == browseNodesChunk && res.BrowseChunk == browseChunk))
                {
                    log.Information("Skipping {bnc}, {bc} due to having been browsed", browseNodesChunk, browseChunk);
                    continue;
                }
                if (results.Any())
                {
                    var maxSize = results.Select(res => res.NumNodes).Max();
                    if (maxSize > 100 * browseNodesChunk) continue;
                }


                config.Source.BrowseChunk = browseChunk;
                config.Source.BrowseNodesChunk = browseNodesChunk;

                log.Information("Browse with BrowseNodesChunk: {bnc}, BrowseChunk: {bc}", browseNodesChunk,
                    browseChunk);

                var result = new BrowseMapResult {BrowseNodesChunk = browseNodesChunk, BrowseChunk = browseChunk};

                try
                {
                    await ToolUtil.RunWithTimeout(BrowseNodeHierarchy(root, ToolUtil.GetSimpleListWriterCallback(nodes, this), token), 120);
                    log.Information("Browse succeeded, attempting to read children of all nodes, to further test operation limit");
                    await ToolUtil.RunWithTimeout(() => BrowseDirectory(nodes.Select(node => node.Id).Take(browseNodesChunk),
                        (_, __) => { }, token), 120);
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
                    result.failed = true;
                }

                result.NumNodes = nodes.Count;
                results.Add(result);
            }

            var scResults = results.Where(res => !res.failed);
            if (!scResults.Any())
            {
                throw new FatalException("No configuration resulted in successful browse, manual configuration may work.");
            }

            var best = scResults.Aggregate((agg, next) =>
                next.NumNodes > agg.NumNodes
                || next.NumNodes == agg.NumNodes && next.BrowseChunk > agg.BrowseChunk
                || next.NumNodes == agg.NumNodes && next.BrowseChunk == agg.BrowseChunk && next.BrowseNodesChunk > agg.BrowseNodesChunk
                ? next : agg);

            if (best.NumNodes < best.BrowseNodesChunk)
            {
                log.Warning("Size is smaller than BrowseNodesChunk, so it is not completely safe, the " +
                            "largest known safe value of BrowseNodesChunk is {max}", best.NumNodes);
                if (best.NumNodes < ServerWidest && !useServer)
                {
                    log.Information("The server hierarchy is generally wider than this, so retry browse mapping using the " +
                                    "server hierarchy");
                    useServer = true;
                    await GetBrowseChunkSizes(token);
                    return;
                }
                summary.BrowseLimitWarning = true;

                if (best.NumNodes < ServerSizeBase && !useServer)
                {
                    log.Information("The server hierarchy is larger than the main hierarchy, so use the server to identify " +
                                    "attribute chunk later");
                    useServer = true;
                }
            }
            log.Information("Successfully determined BrowseNodesChunk: {bnc}, BrowseChunk: {bc}", 
                best.BrowseNodesChunk, best.BrowseChunk);
            config.Source.BrowseNodesChunk = best.BrowseNodesChunk;
            config.Source.BrowseChunk = best.BrowseChunk;
            baseConfig.Source.BrowseNodesChunk = best.BrowseNodesChunk;
            baseConfig.Source.BrowseChunk = best.BrowseChunk;
            summary.BrowseNodesChunk = best.BrowseNodesChunk;
            summary.BrowseChunk = best.BrowseChunk;
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
        /// <summary>
        /// Browse the datatype hierarchy, checking for custom numeric datatypes.
        /// </summary>
        public void ReadCustomTypes(CancellationToken token)
        {
            dataTypes = new List<BufferedNode>();

            log.Information("Browsing data type hierarchy for custom datatypes");

            VisitedNodes.Clear();

            try
            {
                BrowseDirectory(new List<NodeId> {DataTypes.BaseDataType}, ToolUtil.GetSimpleListWriterCallback(dataTypes, this),
                    token,
                    ReferenceTypeIds.HasSubtype, (uint) NodeClass.DataType | (uint) NodeClass.ObjectType);
            }
            catch (Exception e)
            {
                log.Error(e, "Failed to browse data types");
                throw;
            }

            customNumericTypes = new List<ProtoDataType>();
            foreach (var type in dataTypes)
            {
                string identifier = type.Id.IdType == IdType.String ? (string) type.Id.Identifier : null;
                if (IsCustomObject(type.Id)
                    && (identifier != null && (
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
                    || ToolUtil.IsChildOf(dataTypes, type, DataTypes.Number)
                    || ToolUtil.IsChildOf(dataTypes, type, DataTypes.Boolean)
                    ))
                {
                    log.Information("Found potential custom numeric datatype: {id}", type.Id);
                    customNumericTypes.Add(new ProtoDataType
                    {
                        IsStep = identifier != null && identifier.Contains("bool", StringComparison.InvariantCultureIgnoreCase)
                                 || type.DisplayName != null && type.DisplayName.Contains("bool", StringComparison.InvariantCultureIgnoreCase)
                                 || ToolUtil.IsChildOf(dataTypes, type, DataTypes.Boolean),
                        NodeId = NodeIdToProto(type.Id)
                    });
                }
            }
            log.Information("Found {count} custom numeric datatypes", customNumericTypes.Count);
            summary.CustomNumTypesCount = customNumericTypes.Count;
            baseConfig.Extraction.DataTypes.CustomNumericTypes = customNumericTypes;
        }
        /// <summary>
        /// Get AttributeChunk config value, by attempting to read for various chunk sizes.
        /// </summary>
        public async Task GetAttributeChunkSizes(CancellationToken token)
        {
            var root = useServer
                ? ObjectIds.Server
                : config.Extraction.RootNode.ToNodeId(this, ObjectIds.ObjectsFolder);

            log.Information("Reading variable chunk sizes to determine the AttributeChunk property");

            config.History.Enabled = true;

            VisitedNodes.Clear();

            nodeList = new List<BufferedNode>();

            try
            {
                await BrowseNodeHierarchy(root, ToolUtil.GetSimpleListWriterCallback(nodeList, this), token);
            }
            catch
            {
                log.Error("Failed to browse node hierarchy");
                throw;
            }

            int oldArraySize = config.Extraction.DataTypes.MaxArraySize;
            int expectedAttributeReads = nodeList.Aggregate(0, (acc, node) => acc + (node.IsVariable ? 5 : 1));
            config.Extraction.DataTypes.MaxArraySize = 10;

            var testChunks = testAttributeChunkSizes.Where(chunkSize =>
                chunkSize <= expectedAttributeReads || chunkSize <= 1000);

            if (expectedAttributeReads < 1000)
            {
                log.Warning("Reading less than 1000 attributes maximum. Most servers should support more, but" +
                            " this server only has enough variables to read {reads}", expectedAttributeReads);
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

            config.Extraction.DataTypes.MaxArraySize = 10;

            if (useServer)
            {
                VisitedNodes.Clear();
                log.Information("Filling common node information since this has not been done when identifying attribute chunk size");
                nodeList = new List<BufferedNode>();
                try
                {
                    await BrowseNodeHierarchy(root, ToolUtil.GetSimpleListWriterCallback(nodeList, this), token);
                }
                catch (Exception e)
                {
                    log.Information(e, "Failed to map out hierarchy");
                    throw;
                }
            }


            log.Information("Mapping out variable datatypes");

            var variables = nodeList.Where(node =>
                node.IsVariable && (node is BufferedVariable variable) && !variable.IsProperty)
                .Select(node => node as BufferedVariable)
                .Where(node => node != null);

            ReadNodeData(variables, token);

            history = false;
            bool stringVariables = false;
            int maxLimitedArrayLength = 1;

            var identifiedTypes = new List<BufferedNode>();
            foreach (var variable in variables)
            {
                if (variable.ArrayDimensions != null
                    && variable.ArrayDimensions.Count == 1
                    && variable.ArrayDimensions[0] <= arrayLimit
                    && variable.ArrayDimensions[0] > maxLimitedArrayLength)
                {
                    maxLimitedArrayLength = variable.ArrayDimensions[0];
                } else if (variable.ArrayDimensions != null
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

                if (variable.DataTypeId == null || variable.DataTypeId.IsNullNodeId)
                {
                    Log.Warning("Variable datatype is null on id: {id}", variable.Id);
                    continue;
                }

                var dataType = dataTypes.FirstOrDefault(type => type.Id == variable.DataTypeId);

                if (dataType == null)
                {
                    log.Warning("DataType found on node but not in hierarchy, " +
                                "this may mean that some datatypes are defined outside of the main datatype hierarchy: {type}", variable.DataTypeId);
                    continue;
                }

                if (identifiedTypes.Contains(dataType)) continue;
                identifiedTypes.Add(dataType);
            }

            foreach (var dataType in identifiedTypes)
            {
                string identifier = dataType.Id.IdType == IdType.String ? (string)dataType.Id.Identifier : null;
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
                log.Information("Variables with string datatype were discovered, and the AllowStringVariables config option " +
                                "will be set to true");
            } else if (!baseConfig.Extraction.DataTypes.AllowStringVariables)
            {
                log.Information("No string variables found and the AllowStringVariables option will be set to false");
            }

            if (maxLimitedArrayLength > 1)
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
            baseConfig.Extraction.DataTypes.MaxArraySize = maxLimitedArrayLength > 1 ? maxLimitedArrayLength : oldArraySize;

            summary.StringVariables = stringVariables;
            summary.MaxArraySize = maxLimitedArrayLength;
        }

        private struct BrowseMapResult
        {
            public int BrowseNodesChunk;
            public int BrowseChunk;
            public int NumNodes;
            public bool failed;
        }
        private bool AllowTSMap(BufferedVariable node)
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
            foreach (var node in nodeList)
            {
                if (node is BufferedVariable variable)
                {
                    variable.DataType = new BufferedDataType(variable.DataTypeId ?? NodeId.Null);
                }
            }
            var states = nodeList.Where(node =>
                    node.IsVariable && (node is BufferedVariable variable) && !variable.IsProperty
                    && AllowTSMap(variable))
                .Select(node => new NodeExtractionState(this, node as BufferedVariable, false, false, false)).ToList();

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

            var dps = new List<BufferedDataPoint>();

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
                await Task.Delay(200);
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
                    node.IsVariable && (node is BufferedVariable variable) && !variable.IsProperty && variable.Historizing)
                .Select(node => new NodeExtractionState(this, node as BufferedVariable, true, true, false)).ToList();

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
                NumValuesPerNode = (uint) config.History.DataChunk
            };

            var backfillParams = new HistoryReadParams(new[] {nodeWithData}, backfillDetails);

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
        private BufferedEvent ConstructEvent(EventFilter filter, VariantCollection eventFields, NodeId emitter)
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
                if (!targetEventFields.Any(field =>
                    field.Root == clause.TypeDefinitionId
                    && field.BrowseName == clause.BrowsePath[0]
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
                log.Error(e, "Failed to construct bufferedEvent from Raw fields");
                return null;
            }
        }

        private IEnumerable<BufferedEvent> ReadResultToEvents(IEncodeable rawEvts, NodeId emitterId, ReadEventDetails details)
        {
            if (rawEvts == null) return Array.Empty<BufferedEvent>();
            if (!(rawEvts is HistoryEvent evts))
            {
                log.Warning("Incorrect return type of history read events");
                return Array.Empty<BufferedEvent>();
            }

            var filter = details.Filter;
            if (filter == null)
            {
                log.Warning("No event filter, ignoring");
                return Array.Empty<BufferedEvent>();
            }

            if (evts.Events == null) return Array.Empty<BufferedEvent>();

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
            eventTypes = new List<BufferedNode>();

            try
            {
                VisitedNodes.Clear();
                BrowseDirectory(new List<NodeId> {ObjectTypeIds.BaseEventType}, ToolUtil.GetSimpleListWriterCallback(eventTypes, this),
                    token,
                    ReferenceTypeIds.HasSubtype, (uint) NodeClass.ObjectType);
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
                log.Warning(ex, "Failed to read EventNotifier property, this tool will not be able to identify non-server emitters this way");
            }

            var emitters = notifierPairs.Where(pair => (pair.notifier & EventNotifiers.SubscribeToEvents) != 0);
            var historizingEmitters = emitters.Where(pair => (pair.notifier & EventNotifiers.HistoryRead) != 0);

            if (emitters.Any())
            {
                log.Information("Discovered {cnt} emitters, of which {cnt2} are historizing", emitters.Count(), historizingEmitters.Count());
            }

            log.Information("Scan hierarchy for GeneratesEvent references");

            var emitterReferences = new List<BufferedNode>();

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
                log.Warning(ex, "Failed to look for GeneratesEvent references, this tool will not be able to identify non-server emitters this way");
            }

            VisitedNodes.Clear();

            var referencedEvents = emitterReferences.Select(evt => evt.Id)
                .Distinct()
                .Where(id => IsCustomObject(id) || id == ObjectTypeIds.BaseEventType).ToHashSet();

            emittedEvents = referencedEvents.ToList();


            log.Information("Identified {cnt} emitters and {cnt2} events by looking at GeneratesEvent references",
                emitterReferences.DistinctBy(reference => reference.ParentId).Count(), emittedEvents.Count);

            if (emitterReferences.Any() && emitters.Any())
            {
                emitterIds = emitters.Select(pair => pair.id).Append(ObjectIds.Server)
                    .Intersect(emitterReferences.Where(evt => referencedEvents.Contains(evt.Id)).Select(evt => evt.ParentId))
                    .Distinct()
                    .ToList();
                log.Information("{cnt} emitters were detected that generate relevant events and can be subscribed to", emitterIds.Count);
            }
            else if (emitterReferences.Any() && !emitters.Any())
            {
                emitterIds = emitterReferences
                    .Where(evt => referencedEvents.Contains(evt.Id))
                    .Select(evt => evt.ParentId)
                    .Distinct()
                    .ToList();

                log.Information("{cnt} emitters were detected that generate relevant events", emitterIds.Count);
            }
            else if (!emitterReferences.Any() && emitters.Any())
            {
                emitterIds = emitters.Select(pair => pair.id).Distinct().ToList();

                log.Information("{cnt} subscribable emitters were detected", emitterIds.Count);
            }
            else
            {
                emitterIds = new List<NodeId>();
                log.Information("No emitters were found");
            }

            bool auditReferences = emitterReferences.Any(evt => evt.ParentId == ObjectIds.Server && (
                    evt.Id == ObjectTypeIds.AuditAddNodesEventType || evt.Id == ObjectTypeIds.AuditAddReferencesEventType));

            baseConfig.Extraction.EnableAuditDiscovery |= auditReferences;
            summary.Auditing = auditReferences;

            if (auditReferences)
            {
                log.Information("Audit events on the server node detected, auditing can be enabled");
            }

            log.Information("Listening to events on the server node a little while in order to find further events");

            var eventsToListenFor = eventTypes.Where(evt =>
                IsCustomObject(evt.Id)
                || evt.Id == ObjectTypeIds.BaseEventType)
                .Select(evt => evt.Id);

            activeEventFields = GetEventFields(eventsToListenFor, token);

            var events = new List<BufferedEvent>();

            object listLock = new object();

            await ToolUtil.RunWithTimeout(() => SubscribeToEvents(new [] {ObjectIds.Server},
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

                    log.Verbose(buffEvent.ToDebugDescription());

                }, token), 120);

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

                    var buffEvent = new BufferedEvent
                    {
                        EventType = eventType
                    };
                    lock (listLock)
                    {
                        events.Add(buffEvent);
                    }

                    log.Verbose(buffEvent.ToDebugDescription());

                }), 120);

            await Task.Delay(5000);

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

                if (!emitterIds.Contains(ObjectIds.Server))
                {
                    emitterIds.Add(ObjectIds.Server);
                }
            }

            if (emittedEvents.Any(id => id == ObjectTypeIds.BaseEventType))
            {
                log.Warning("Using BaseEventType directly is not recommended, consider switching to a custom event type instead.");
                summary.BaseEventWarning = true;
            }

            log.Information("Detected a total of {cnt} event emitters and {cnt2} event types",
                emitterIds.Count, emittedEvents.Count);

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




            summary.NumEmitters = emitterIds.Count;
            summary.NumEventTypes = emittedEvents.Count;

            if (!emitterIds.Any()) return;

            historizingEmitters = emitters.Where(emitter => emitterIds.Contains(emitter.id));

            log.Information("Detected {cnt} historizing emitters", historizingEmitters.Count());
            summary.NumHistorizingEmitters = historizingEmitters.Count();

            baseConfig.History.Enabled = true;
            baseConfig.Events.EventIds = emittedEvents.Distinct().Select(NodeIdToProto).ToList();
            baseConfig.Events.EmitterIds = emitterIds.Distinct().Select(NodeIdToProto).ToList();
            baseConfig.Events.HistorizingEmitterIds = historizingEmitters.Distinct().Select(pair => NodeIdToProto(pair.id)).ToList();

            if (!baseConfig.Events.EventIds.Any())
            {
                baseConfig.Events.EmitterIds = Enumerable.Empty<ProtoNodeId>();
            }
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
            if (summary.BrowseLimitWarning)
            {
                log.Information("This is not a completely safe option, as the actual number of nodes is lower than the limit, so if " +
                                "the number of nodes increases in the future, it may fail.");
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
            if (summary.CustomNumTypesCount > 0 || summary.MaxArraySize > 0 || summary.StringVariables)
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
                log.Information("{types} different event types were found, emitted from {em} nodes acting as emitters",
                    summary.NumEventTypes, summary.NumEmitters);
                if (summary.NumHistorizingEmitters > 0)
                {
                    log.Information("{cnt} historizing event emitters were found and successfully read from", summary.NumHistorizingEmitters);
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
