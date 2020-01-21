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
        private List<NodeId> historizingEmitters;
        private List<BufferedNode> eventTypes;
        private Dictionary<string, string> namespaceMap;
        private Dictionary<NodeId, IEnumerable<(NodeId, QualifiedName)>> activeEventFields;
        private bool history;
        private bool useServer;

        private static readonly int serverSizeBase = 125;
        private static readonly int serverWidest = 47;

        private readonly List<(int, int)> testBrowseChunkSizes = new List<(int, int)>
        {
            (100, 1000),
            (1000, 100),
            (100, 100),
            (1, 1000),
            (1000, 0),
            (1, 0),
            (1000, 1000)
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

        private struct Summary
        {
            public List<string> Endpoints;
            public bool Secure;
            public int BrowseNodesChunk;
            public int BrowseChunk;
            public bool BrowseLimitWarning;
            public bool ServerSizeWarning;
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
                summary.Endpoints = endpoints.Select(ep => $"{ep.EndpointUrl}: {ep.SecurityPolicyUri}").ToList();
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
                summary.Secure = secureExists;
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

        public async Task GetBrowseChunkSizes(CancellationToken token)
        {
            var results = new List<BrowseMapResult>();

            var root = useServer
                ? ObjectIds.Server
                : config.Extraction.RootNode.ToNodeId(this, ObjectIds.ObjectsFolder);

            foreach ((int lbrowseNodesChunk, int lbrowseChunk) in testBrowseChunkSizes)
            {
                var nodes = new List<BufferedNode>();

                visitedNodes.Clear();

                var browseNodesChunk = Math.Min(lbrowseNodesChunk, baseConfig.Source.BrowseNodesChunk);
                var browseChunk = Math.Min(lbrowseChunk, baseConfig.Source.BrowseChunk);

                if (results.Any(res => res.BrowseNodesChunk == browseNodesChunk && res.BrowseChunk == browseChunk))
                {
                    continue;
                }

                Log.Information("Browse with BrowseNodesChunk: {bnc}, BrowseChunk: {bc}", browseNodesChunk,
                    browseChunk);

                var result = new BrowseMapResult {BrowseNodesChunk = browseNodesChunk, BrowseChunk = browseChunk};

                try
                {
                    await ToolUtil.RunWithTimeout(BrowseNodeHierarchy(root, ToolUtil.GetSimpleListWriterCallback(nodes, this), token), 120);
                    Log.Information("Browse succeeded, attempting to read children of all nodes, to further test operation limit");
                    await ToolUtil.RunWithTimeout(() => BrowseDirectory(nodes.Select(node => node.Id), (_, __) => { }, token), 120);
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
                || next.NumNodes == agg.NumNodes && next.BrowseChunk > agg.BrowseChunk
                || next.NumNodes == agg.NumNodes && next.BrowseChunk == agg.BrowseChunk && next.BrowseNodesChunk > agg.BrowseNodesChunk
                ? next : agg);

            if (best.NumNodes < best.BrowseNodesChunk)
            {
                Log.Warning("Size is smaller than BrowseNodesChunk, so it is not completely safe, the " +
                            "largest known safe value of BrowseNodesChunk is {max}", best.NumNodes);
                if (best.NumNodes < serverWidest && !useServer)
                {
                    Log.Information("The server hierarchy is generally wider than this, so retry browse mapping using the " +
                                    "server hierarchy");
                    useServer = true;
                    await GetBrowseChunkSizes(token);
                    return;
                }
                summary.BrowseLimitWarning = true;

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
            summary.BrowseNodesChunk = best.BrowseNodesChunk;
            summary.BrowseChunk = best.BrowseChunk;

            if (useServer && best.NumNodes < serverSizeBase)
            {
                Log.Warning("The server is smaller than the known number of nodes in the specification defined base hierarchy. " +
                            "This may cause issues later, depending on what parts are missing.");
                summary.ServerSizeWarning = true;
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
                BrowseDirectory(new List<NodeId> {DataTypes.BaseDataType}, ToolUtil.GetSimpleListWriterCallback(dataTypes, this),
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
                    || ToolUtil.IsChildOf(dataTypes, type, DataTypes.Number)
                    ))
                {
                    Log.Information("Found potential custom numeric datatype: {id}", type.Id);
                    customNumericTypes.Add(new ProtoDataType
                    {
                        IsStep = identifier.Contains("bool", StringComparison.InvariantCultureIgnoreCase)
                                 || ToolUtil.IsChildOf(dataTypes, type, DataTypes.Boolean),
                        NodeId = NodeIdToProto(type.Id)
                    });
                }
            }
            Log.Information("Found {count} custom numeric datatypes", customNumericTypes.Count);
            summary.CustomNumTypesCount = customNumericTypes.Count;
        }

        public async Task GetAttributeChunkSizes(CancellationToken token)
        {
            var root = useServer
                ? ObjectIds.Server
                : config.Extraction.RootNode.ToNodeId(this, ObjectIds.ObjectsFolder);

            Log.Information("Reading variable chunk sizes to determine the AttributeChunk property");

            visitedNodes.Clear();

            nodeList = new List<BufferedNode>();

            try
            {
                await BrowseNodeHierarchy(root, ToolUtil.GetSimpleListWriterCallback(nodeList, this), token);
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
                summary.VariableLimitWarning = true;
            }

            bool succeeded = false;

            foreach (var chunkSize in testChunks)
            {
                Log.Information("Attempting to read attributes with ChunkSize {chunkSize}", chunkSize);
                config.Source.AttributesChunk = chunkSize;
                try
                {
                    await ToolUtil.RunWithTimeout(() => ReadNodeData(nodeList, token), 120);
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

            summary.AttributeChunkSize = baseConfig.Source.AttributesChunk;

            config.Extraction.MaxArraySize = oldArraySize;

            if (!succeeded)
            {
                throw new Exception("Failed to read node attributes for any chunk size");
            }
        }

        public async Task IdentifyDataTypeSettings(CancellationToken token)
        {
            var root = config.Extraction.RootNode.ToNodeId(this, ObjectIds.ObjectsFolder);

            int oldArraySize = config.Extraction.MaxArraySize;

            int arrayLimit = config.Extraction.MaxArraySize == 0 ? 10 : config.Extraction.MaxArraySize;

            config.Extraction.MaxArraySize = 10;

            if (useServer)
            {
                visitedNodes.Clear();
                Log.Information("Filling common node information since this has not been done when identifying attribute chunk size");
                nodeList = new List<BufferedNode>();
                try
                {
                    await BrowseNodeHierarchy(root, ToolUtil.GetSimpleListWriterCallback(nodeList, this), token);
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

            config.Extraction.MaxArraySize = oldArraySize;

            baseConfig.Extraction.AllowStringVariables = baseConfig.Extraction.AllowStringVariables || stringVariables;
            baseConfig.Extraction.MaxArraySize = maxLimitedArrayLength > 1 ? maxLimitedArrayLength : oldArraySize;

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

        public async Task GetSubscriptionChunkSizes(CancellationToken token)
        {
            bool failed = true;
            var states = nodeList.Where(node =>
                    node.IsVariable && (node is BufferedVariable variable) && !variable.IsProperty)
                .Select(node => new NodeExtractionState(node as BufferedVariable)).ToList();

            Log.Information("Get chunkSizes for subscribing to variables");

            if (states.Count == 0)
            {
                Log.Warning("There are no extractable states, subscriptions will not be tested");
                return;
            }

            if (states.Count < 1000)
            {
                Log.Warning("There are only {count} extractable variables, so expected chunksizes may not be accurate. " +
                            "The default is 1000, which generally works.", states.Count);
                summary.SubscriptionLimitWarning = true;
            }

            var dps = new List<BufferedDataPoint>();

            foreach (var chunkSize in testSubscriptionChunkSizes)
            {
                config.Source.SubscriptionChunk = chunkSize;
                try
                {
                    await ToolUtil.RunWithTimeout(() =>
                        SubscribeToNodes(states,
                            ToolUtil.GetSimpleListWriterHandler(dps, states.ToDictionary(state => state.Id), this), token), 120);
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
                        await ToolUtil.RunWithTimeout(() => session.RemoveSubscriptions(session.Subscriptions.ToList()), 120);
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

            summary.Subscriptions = true;
            Log.Information("Settled on chunkSize: {size}", baseConfig.Source.SubscriptionChunk);
            Log.Information("Waiting for datapoints to arrive...");
            summary.SubscriptionChunkSize = baseConfig.Source.SubscriptionChunk;

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
                summary.SilentSubscriptionsWarning = true;
            }

            session.RemoveSubscriptions(session.Subscriptions.ToList());
        }

        public async Task GetHistoryReadConfig()
        {
            var historizingStates = nodeList.Where(node =>
                    node.IsVariable && (node is BufferedVariable variable) && !variable.IsProperty && variable.Historizing)
                .Select(node => new NodeExtractionState(node as BufferedVariable)).ToList();

            var stateMap = historizingStates.ToDictionary(state => state.Id);

            Log.Information("Read history to decide on decent history settings");

            if (!historizingStates.Any())
            {
                Log.Warning("No historizing variables detected, unable analyze history");
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

            foreach (var chunkSize in testHistoryChunkSizes)
            {
                foreach (var chunk in Utils.ChunkBy(historizingStates, chunkSize))
                {
                    var historyParams = new HistoryReadParams(chunk.Select(state => state.Id), details);
                    try
                    {
                        var result = await ToolUtil.RunWithTimeout(() => DoHistoryRead(historyParams), 10);

                        foreach (var res in result)
                        {
                            var data = ToolUtil.ReadResultToDataPoints(res.Item2, stateMap[res.Item1], this);
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

            summary.History = true;
            Log.Information("Settled on chunkSize: {size}", baseConfig.History.DataNodesChunk);
            summary.HistoryChunkSize = baseConfig.History.DataNodesChunk;
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

                var data = ToolUtil.ReadResultToDataPoints(result.First().Item2, stateMap[result.First().Item1], this);

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

            summary.BackfillRecommended = largestEstimate > 100000 && backfillCapable;

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
            eventTypes = new List<BufferedNode>();

            try
            {
                visitedNodes.Clear();
                BrowseDirectory(new List<NodeId> {ObjectTypeIds.BaseEventType}, ToolUtil.GetSimpleListWriterCallback(eventTypes, this),
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
                BrowseDirectory(nodeList.Select(node => node.Id).Append(ObjectIds.Server).ToList(),
                    ToolUtil.GetSimpleListWriterCallback(emitterReferences, this),
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
                .Where(id => IsCustomObject(id)
                              || id == ObjectTypeIds.BaseEventType).ToHashSet();


            emittedEvents = referencedEvents.ToList();

            emitterIds = emitterReferences
                .Where(evt => referencedEvents.Contains(evt.Id))
                .Select(evt => evt.ParentId).Distinct().ToList();

            Log.Information("Identified {cnt} emitters and {cnt2} events by looking at GeneratesEvent references", 
                emitterIds.Count, emittedEvents.Count);

            bool auditReferences = emitterReferences.Any(evt => evt.ParentId == ObjectIds.Server && (
                    evt.Id == ObjectTypeIds.AuditAddNodesEventType || evt.Id == ObjectTypeIds.AuditAddReferencesEventType));

            baseConfig.Extraction.EnableAuditDiscovery |= auditReferences;
            summary.Auditing = auditReferences;
            if (auditReferences)
            {
                Log.Information("Audit events on the server node detected, auditing can be enabled");
            }

            Log.Information("Listening to events on the server node a little while in order to find further events");

            var eventsToListenFor = eventTypes.Where(evt =>
                IsCustomObject(evt.Id)
                || evt.Id == ObjectTypeIds.BaseEventType)
                .Select(evt => evt.Id);

            activeEventFields = GetEventFields(eventsToListenFor, token);

            var events = new List<BufferedEvent>();

            object listLock = new object();

            await ToolUtil.RunWithTimeout(() => SubscribeToEvents(new [] {ObjectIds.Server}, nodeList.Select(node => node.Id).ToList(),
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

                    lock (listLock)
                    {
                        events.Add(buffEvent);
                    }

                    Log.Verbose(buffEvent.ToDebugDescription());

                }, token), 120);

            await ToolUtil.RunWithTimeout(() => SubscribeToAuditEvents(
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
                    int eventTypeIndex = filter.SelectClauses.FindIndex(atr => atr.TypeDefinitionId == ObjectTypeIds.BaseEventType
                                                                               && atr.BrowsePath[0] == BrowseNames.EventType);
                    if (eventTypeIndex < 0)
                    {
                        Log.Warning("Triggered event has no type, ignoring");
                        return;
                    }
                    var eventType = eventFields[eventTypeIndex].Value as NodeId;
                    if (eventType == null || eventType != ObjectTypeIds.AuditAddNodesEventType && eventType != ObjectTypeIds.AuditAddReferencesEventType)
                    {
                        Log.Warning("Non-audit event triggered on audit event listener");
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

                    Log.Verbose(buffEvent.ToDebugDescription());

                }), 120);

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
                    ToolUtil.IsChildOf(eventTypes, eventTypes.Find(type => type.Id == id), ObjectTypeIds.AuditEventType)))
                {
                    Log.Information("Audit events detected on server node, auditing can be enabled");
                    baseConfig.Extraction.EnableAuditDiscovery = true;
                    summary.Auditing = true;
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
                summary.BaseEventWarning = true;
            }

            Log.Information("Detected a total of {cnt} event emitters and {cnt2} event types",
                emitterIds.Count, emittedEvents.Count);

            summary.NumEmitters = emitterIds.Count;
            summary.NumEventTypes = emittedEvents.Count;

            if (!emitterIds.Any()) return;

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
                    var result = await ToolUtil.RunWithTimeout(() => DoHistoryRead(historyParams), 10);

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
            summary.NumHistorizingEmitters = historizingEmitters.Count;

            baseConfig.Events.EventIds = emittedEvents.Distinct().Select(NodeIdToProto).ToList();
            baseConfig.Events.EmitterIds = emitterIds.Distinct().Select(NodeIdToProto).ToList();
            baseConfig.Events.HistorizingEmitterIds = historizingEmitters.Distinct().Select(NodeIdToProto).ToList();
        }

        public void GetNamespaceMap()
        {
            var indices = nodeList.Concat(dataTypes).Concat(eventTypes).Select(node => node.Id.NamespaceIndex).Distinct();

            var namespaces = indices.Select(idx => session.NamespaceUris.GetString(idx));

            var startRegex = new Regex("^.*://");
            var splitRegex = new Regex("[^a-zA-Z\\d]");

            var map = namespaces.ToDictionary(ns => ns, ns =>
                ns == "http://opcfoundation.org/UA/" ? "base" :
                string.Concat(splitRegex.Split(startRegex.Replace(ns, ""))
                    .Where(sub => !string.IsNullOrEmpty(sub) && sub.Length > 3)
                    .Select(sub => sub.First()))
            );

            namespaceMap = new Dictionary<string, string>();

            foreach (var mapped in map)
            {
                var baseValue = mapped.Value;

                var nextValue = baseValue;

                int index = 1;

                while (namespaceMap.Any(kvp => nextValue == kvp.Value && mapped.Key != kvp.Key))
                {
                    nextValue = baseValue + index;
                    index++;
                }

                namespaceMap.Add(mapped.Key, nextValue.ToLower());
            }

            Log.Information("Suggested namespaceMap: ");
            foreach (var kvp in namespaceMap)
            {
                Log.Information("    {key}: {value}", kvp.Key, kvp.Value);
            }

            summary.NamespaceMap = namespaceMap.Select(kvp => $"{kvp.Key}: {kvp.Value}").ToList();

            baseConfig.Extraction.NamespaceMap = namespaceMap;
        }

        public void LogSummary()
        {
            Log.Information("");
            Log.Information("Server analysis successfully completed, no critical issues were found");
            Log.Information("==== SUMMARY ====");
            Log.Information("");

            if (summary.Endpoints.Any())
            {
                Log.Information("{cnt} endpoints were found: ", summary.Endpoints.Count);
                foreach (var endpoint in summary.Endpoints)
                {
                    Log.Information("    {ep}", endpoint);
                }

                if (summary.Secure)
                {
                    Log.Information("At least one of these are secure, meaning that the Secure config option can and should be enabled");
                }
                else
                {
                    Log.Information("None of these are secure, so enabling the Secure config option will probably not work.");
                }
            }
            else
            {
                Log.Information("No endpoints were found, but the client was able to connect. This is not necessarily an issue, " +
                                "but there may be a different discovery URL connected to the server that exposes further endpoints.");
            }
            Log.Information("");

            if (summary.BrowseChunk == 0)
            {
                Log.Information("Settled on browsing the children of {bnc} nodes at a time and letting the server decide how many results " +
                                "to return for each request", summary.BrowseNodesChunk);
            }
            else
            {
                Log.Information("Settled on browsing the children of {bnc} nodes at a time and expecting {bc} results maximum for each request",
                    summary.BrowseNodesChunk, summary.BrowseChunk);
            }
            if (summary.BrowseLimitWarning)
            {
                Log.Information("This is not a completely safe option, as the actual number of nodes is lower than the limit, so if " +
                                "the number of nodes increases in the future, it may fail.");
            }
            if (summary.ServerSizeWarning)
            {
                Log.Information("During browse, it was discovered that the server is smaller than the OPC-UA base server hierarchy, " +
                                "depending on the parts that are missing, this may cause issues for the extractor");
            }
            Log.Information("");

            if (summary.CustomNumTypesCount > 0)
            {
                Log.Information("{cnt} custom numeric types were discovered", summary.CustomNumTypesCount);
            }
            if (summary.MaxArraySize > 1)
            {
                Log.Information("Arrays of size {size} were discovered", summary.MaxArraySize);
            }
            if (summary.StringVariables)
            {
                Log.Information("There are variables that would be mapped to strings in CDF, if this is not correct " +
                                "they may be numeric types that the auto detection did not catch, or they may need to be filtered out");
            }
            if (summary.CustomNumTypesCount > 0 || summary.MaxArraySize > 0 || summary.StringVariables)
            {
                Log.Information("");
            }

            Log.Information("Settled on reading {cnt} attributes per Read call", summary.AttributeChunkSize);
            if (summary.VariableLimitWarning)
            {
                Log.Information("This is not a completely safe option, as the actual number of attributes is lower than the limit, so if " +
                                "the number of nodes increases in the future, it may fail");
            }
            Log.Information("");

            if (summary.Subscriptions)
            {
                Log.Information("Successfully subscribed to data variables");
                Log.Information("Settled on subscription chunk size: {chunk}", summary.SubscriptionChunkSize);
                if (summary.SubscriptionLimitWarning)
                {
                    Log.Information("This is not a completely safe option, as the actual number of extractable nodes is lower than the limit, " +
                                    "so if the number of variables increases in the future, it may fail");
                }

                if (summary.SilentSubscriptionsWarning)
                {
                    Log.Information("Though subscriptions were successfully created, no data was received. This may be an issue if " +
                                    "data is expected to appear within a five second window");
                }
            }
            else
            {
                Log.Information("The explorer was unable to subscribe to data variables, because none exist or due to a server issue");
            }
            Log.Information("");

            if (summary.History)
            {
                Log.Information("Successfully read datapoint history");
                Log.Information("Settled on history chunk size {chunk} with granularity {g}", 
                    summary.HistoryChunkSize, summary.HistoryGranularity);
                if (summary.BackfillRecommended)
                {
                    Log.Information("There are large enough amounts of datapoints for certain variables that " +
                                    "enabling backfill is recommended. This increases startup time a bit, but makes the extractor capable of " +
                                    "reading live data and historical data at the same time");
                }
            }
            else if (summary.NoHistorizingNodes)
            {
                Log.Information("No historizing nodes detected, the server may support history, but the extractor will only read " +
                                "history from nodes with the Historizing attribute set to true");
            }
            else
            {
                Log.Information("The explorer was unable to read history");
            }
            Log.Information("");

            if (summary.NumEventTypes > 0)
            {
                Log.Information("Successfully found support for events on the server");
                Log.Information("{types} different event types were found, emitted from {em} nodes acting as emitters",
                    summary.NumEventTypes, summary.NumEmitters);
                if (summary.NumHistorizingEmitters > 0)
                {
                    Log.Information("{cnt} historizing event emitters were found and successfully read from", summary.NumHistorizingEmitters);
                }

                if (summary.BaseEventWarning)
                {
                    Log.Information("BaseEventType events were observed to be emitted directly. This is not ideal, it is better to " +
                                    "only use custom or predefined event types.");
                }
            }
            else
            {
                Log.Information("No regular relevant events were able to be read from the server");
            }

            if (summary.Auditing)
            {
                Log.Information("The server likely supports auditing, which may be used to detect addition of nodes and references");
            }
            Log.Information("");

            Log.Information("The following NamespaceMap was suggested: ");
            foreach (string ns in summary.NamespaceMap)
            {
                Log.Information("    {ns}", ns);
            }
        }

        public FullConfig GetFinalConfig()
        {
            return baseConfig;
        }
    }
}
