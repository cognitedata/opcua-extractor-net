﻿/* Cognite Extractor for OPC-UA
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Opc.Ua;
using Opc.Ua.Client;
using Prometheus.Client;
using Serilog;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Main extractor class, tying together the <see cref="UAClient"/> and CDF client.
    /// </summary>
    public class Extractor
    {
        private readonly UAClient UAClient;
        private readonly FullConfig config;
        public NodeId RootNode { get; private set; }
        private readonly IEnumerable<IPusher> pushers;
        private readonly ConcurrentQueue<BufferedNode> commonQueue = new ConcurrentQueue<BufferedNode>();

        // Concurrent reading of properties
        private readonly HashSet<NodeId> pendingProperties = new HashSet<NodeId>();
        private readonly object propertySetLock = new object();
        private readonly List<Task> propertyReadTasks = new List<Task>();

        public ConcurrentDictionary<NodeId, NodeExtractionState> NodeStates { get; } = new ConcurrentDictionary<NodeId, NodeExtractionState>();
        public ConcurrentDictionary<NodeId, EventExtractionState> EventEmitterStates { get; } = new ConcurrentDictionary<NodeId, EventExtractionState>();

        private HashSet<NodeId> managedNodes;

        public ConcurrentDictionary<NodeId, IEnumerable<(NodeId, QualifiedName)>> ActiveEvents { get; } = new ConcurrentDictionary<NodeId, IEnumerable<(NodeId, QualifiedName)>>();

        private bool pushEvents = false;
        private bool pushData = false;

        public bool Started { get; private set; }


        private static readonly Gauge startTime = Metrics
            .CreateGauge("opcua_start_time", "Start time for the extractor");

        public Extractor(FullConfig config, IEnumerable<IPusher> pushers, UAClient UAClient)
        {
            this.pushers = pushers;
            this.UAClient = UAClient;
            this.config = config;
            UAClient.Extractor = this;
            Log.Information("Building extractor with {NumPushers} pushers", pushers.Count());
            foreach (var pusher in pushers)
            {
                pusher.Extractor = this;
                pusher.UAClient = UAClient;
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="config">Full config object</param>
        /// <param name="pusher">Pusher to be used</param>
        /// <param name="UAClient">UAClient to use</param>
        public Extractor(FullConfig config, IPusher pusher, UAClient UAClient) : this(config, new List<IPusher> { pusher }, UAClient)
        {
        }
        #region Interface

        /// <summary>
        /// Start the extractor, starting the data-push and the UAClient
        /// </summary>
        /// <returns>True on success</returns>
        public async Task RunExtractor(CancellationToken token, bool quitAfterMap = false)
        {
            if (!UAClient.Started)
            {
                Log.Information("Start UAClient");
                try
                {
                    UAClient.Run(token).Wait();
                }
                catch (Exception e)
                {
                    Log.Error(e, "Failed to start UAClient");
                    throw;
                }
                if (!UAClient.Started)
                {
                    throw new Exception("UAClient failed to start");
                }
            }
            Started = true;
            startTime.Set(new DateTimeOffset(DateTime.Now).ToUnixTimeMilliseconds());
            RootNode = config.Extraction.RootNode.ToNodeId(UAClient);
            if (config.Extraction.NodeMap != null)
            {
                foreach (var kvp in config.Extraction.NodeMap)
                {
                    UAClient.AddNodeOverride(kvp.Value.ToNodeId(UAClient), kvp.Key);
                }
            }

            IEnumerable<Task> tasks = pushers.Select(pusher =>
            {
                return Task.Run(async () =>
                {
                    Log.Information("Start push loop");
                    while (!token.IsCancellationRequested && !UAClient.Failed)
                    {
                        try
                        {
                            if (pushData)
                            {
                                await pusher.PushDataPoints(token);
                            }
                            if (pushEvents)
                            {
                                await pusher.PushEvents(token);
                            }
                            Utils.WriteDateToFile(DateTime.Now);
                            await Task.Delay(pusher.BaseConfig.DataPushDelay, token);
                        }
                        catch (TaskCanceledException)
                        {
                            break;
                        }
                        catch (Exception e)
                        {
                            Log.Error(e, "Failed to push datapoints on pusher of type {FailedPusherName}", pusher.GetType().Name);
                        }
                    }
                });
            }).Append(MapUAToCDF(token)).ToList();

            Task failedTask = tasks.FirstOrDefault(task => task.IsFaulted || task.IsCanceled);
            while (tasks.Any() && failedTask == null)
            {
                try
                {
                    await Task.WhenAny(tasks);
                }
                catch (Exception)
                {
                }
                failedTask = tasks.FirstOrDefault(task => task.IsFaulted || task.IsCanceled);
                if (quitAfterMap) return;
                if (failedTask != null) break;
                tasks = tasks.Where(task => !task.IsCompleted && !task.IsFaulted && !task.IsCanceled);
            }
            if (!token.IsCancellationRequested)
            {
                if (failedTask != null)
                {
                    ExceptionDispatchInfo.Capture(failedTask.Exception).Throw();
                }
                throw new Exception("Processes quit without failing");
            }
            throw new TaskCanceledException();
        }
        /// <summary>
        /// Restarts the extractor, to some extent, clears known asset ids,
        /// allows data to be pushed to CDF, and begins mapping the opcua
        /// directory again
        /// </summary>
        public void RestartExtractor(CancellationToken token)
        {
            UAClient.WaitForOperations().Wait();
            MapUAToCDF(token).Wait();
        }
        /// <summary>
        /// Closes the extractor, mainly just shutting down the opcua client.
        /// </summary>
        public void Close()
        {
            if (!UAClient.Started) return;
            try
            {
                UAClient.Close();
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed to cleanly shut down UAClient");
            }
            UAClient.WaitForOperations().Wait(10000);
            Log.Information("Extractor closed");
        }
        /// <summary>
        /// Starts the extractor, calling BrowseDirectory on the root node, then pushes all nodes to CDF once finished.
        /// </summary>
        private async Task MapUAToCDF(CancellationToken token)
        {
            foreach (var pusher in pushers)
            {
                pusher.Reset();
            }
            Log.Debug("Begin mapping directory");
            try
            {
                await UAClient.BrowseDirectoryAsync(RootNode, HandleNode, token);
            }
            catch (Exception)
            {
                throw;
            }
            Log.Debug("End mapping directory");
            var varList = new List<BufferedVariable>();
            var nodeList = new List<BufferedNode>();
            var nodeMap = new Dictionary<string, BufferedNode>();
            var tsList = new List<BufferedVariable>();
            var fullTsList = new List<BufferedVariable>();

            while (commonQueue.TryDequeue(out BufferedNode buffer))
            {
                if (buffer.IsVariable && buffer is BufferedVariable buffVar)
                {
                    if (buffVar.IsProperty)
                    {
                        nodeMap.TryGetValue(UAClient.GetUniqueId(buffVar.ParentId), out BufferedNode parent);
                        if (parent == null) continue;
                        if (parent.properties == null)
                        {
                            parent.properties = new List<BufferedVariable>();
                        }
                        parent.properties.Add(buffVar);
                    }
                    else
                    {
                        varList.Add(buffVar);
                    }
                }
                else
                {
                    nodeList.Add(buffer);
                }
                nodeMap.Add(UAClient.GetUniqueId(buffer.Id), buffer);
            }
            Log.Information("Getting data for {NumVariables} variables and {NumObjects} objects", varList.Count, nodeList.Count);
            try
            {
                UAClient.ReadNodeData(nodeList.Concat(varList), token);
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed to read node data");
            }
            foreach (var node in varList)
            {
                if (AllowTSMap(node))
                {
                    tsList.Add(node);
                    NodeStates[node.Id] = new NodeExtractionState(node);
                    if (node.ArrayDimensions != null && node.ArrayDimensions.Length > 0 && node.ArrayDimensions[0] > 0)
                    {
                        for (int i = 0; i < node.ArrayDimensions[0]; i++)
                        {
                            fullTsList.Add(new BufferedVariable(node, i));
                        }
                        nodeList.Add(node);
                    }
                    else
                    {
                        fullTsList.Add(node);
                    }
                }
            }

            pushData = fullTsList.Any();

            var pushes = pushers.Select(pusher => pusher.PushNodes(nodeList, fullTsList, token)).ToList();
            var pushResult = await Task.WhenAll(pushes);
            if (!pushResult.All(res => res))
            {
                throw new Exception("Pushing nodes failed");
            }


            var statesToSync = NodeStates.Values.Where(state => state.Historizing);
            var getLatestTasks = pushers.Select(pusher => pusher.InitLatestTimestamps(statesToSync, token));
            var getLatestResult = await Task.WhenAll(getLatestTasks);
            if (!getLatestResult.All(res => res))
            {
                throw new Exception("Getting latest timestamp failed");
            }

            foreach (var node in nodeList.Concat(varList))
            {
                Log.Debug(node.ToDebugDescription());
            }

            Log.Information("Synchronize {NumNodesToSynch} nodes", tsList.Count);
            try
            {
                UAClient.SubscribeToNodes(tsList, SubscriptionHandler, token);
                if (config.Events.EmitterIds != null && config.Events.EventIds != null && config.Events.EmitterIds.Any() && config.Events.EventIds.Any())
                {
                    pushEvents = true;
                    var emitters = config.Events.EmitterIds.Select(proto => proto.ToNodeId(UAClient, ObjectIds.Server)).ToList();
                    var latest = Utils.ReadDateFromFile();
                    foreach (var id in emitters)
                    {
                        EventEmitterStates[id] = new EventExtractionState(id);
                        EventEmitterStates[id].InitTimestamp(latest);
                    }

                    managedNodes = nodeList.Concat(varList).Select(node => node.Id).ToHashSet();
                    var eventFields = UAClient.SubscribeToEvents(
                        emitters,
                        config.Events.EventIds.Select(proto => proto.ToNodeId(UAClient, ObjectTypeIds.BaseEventType)).ToList(),
                        managedNodes,
                        EventSubscriptionHandler,
                        token);
                    foreach (var field in eventFields)
                    {
                        ActiveEvents[field.Key] = field.Value;
                    }
                    if (config.Events.HistorizingEmitterIds != null && config.Events.HistorizingEmitterIds.Any())
                    {
                        var histEmitters = config.Events.HistorizingEmitterIds.Select(proto => proto.ToNodeId(UAClient, ObjectIds.Server)).ToList();
                        foreach (var id in histEmitters)
                        {
                            if (!EventEmitterStates.ContainsKey(id)) throw new Exception("Historical emitter not in emitter list");
                            EventEmitterStates[id].Historizing = true;
                        }
                        UAClient.HistoryReadEvents(histEmitters,
                            config.Events.EventIds.Select(proto => proto.ToNodeId(UAClient, ObjectTypeIds.BaseEventType)).ToList(),
                            managedNodes,
                            HistoryEventHandler,
                            token);
                    }
                }

                await UAClient.HistoryReadData(NodeStates.Values.Where(state => state.Historizing), HistoryDataHandler, token);
            }
            catch (Exception e)
            {
                if (e is TaskCanceledException) throw;
                Log.Error("Failed to synchronize nodes");
                throw;
            }
        }
        public async Task ReadProperties(IEnumerable<BufferedNode> nodes, CancellationToken token)
        {
            Task newTask = null;
            lock (propertySetLock)
            {
                nodes = nodes.Where(node => !pendingProperties.Contains(node.Id) && !node.PropertiesRead);
                if (nodes.Any())
                {
                    newTask = Task.Run(() => UAClient.GetNodeProperties(nodes, token));
                    propertyReadTasks.Add(newTask);
                }
            }
            await Task.WhenAll(propertyReadTasks);
            lock (propertySetLock)
            {
                if (!pendingProperties.Any()) return;
                foreach (var node in nodes)
                {
                    node.PropertiesRead = true;
                    pendingProperties.Remove(node.Id);
                }
                if (newTask != null)
                {
                    propertyReadTasks.Remove(newTask);
                }
            }
        }
        /// <summary>
        /// Is the variable allowed to be mapped to a timeseries?
        /// </summary>
        /// <param name="node">Variable to be tested</param>
        /// <returns>True if variable may be mapped to a timeseries</returns>
        public bool AllowTSMap(BufferedVariable node)
        {
            return (!node.DataType.isString || config.Extraction.AllowStringVariables)
                && (node.ValueRank == ValueRanks.Scalar
                    || config.Extraction.MaxArraySize > 0 && node.ArrayDimensions != null && node.ArrayDimensions.Length == 1
                    && node.ArrayDimensions[0] > 0 && node.ArrayDimensions[0] <= config.Extraction.MaxArraySize);
        }
        #endregion

        #region Handlers
        /// <summary>
        /// Callback for the browse operation, creates <see cref="BufferedNode"/>s and enqueues them.
        /// </summary>
        /// <remarks>
        /// A FIFO queue ensures that parents will always be created before their children
        /// </remarks>
        /// <param name="node">Description of the node to be handled</param>
        /// <param name="parentId">Id of the parent node</param>
        private void HandleNode(ReferenceDescription node, NodeId parentId)
        {
            Log.Verbose("HandleNode {parent} {node}", parentId, node);

            if (node.NodeClass == NodeClass.Object)
            {
                var bufferedNode = new BufferedNode(UAClient.ToNodeId(node.NodeId),
                        node.DisplayName.Text, parentId);
                Log.Verbose("HandleNode Object {name}", bufferedNode.DisplayName);
                commonQueue.Enqueue(bufferedNode);
            }
            else if (node.NodeClass == NodeClass.Variable)
            {
                var bufferedNode = new BufferedVariable(UAClient.ToNodeId(node.NodeId),
                        node.DisplayName.Text, parentId);
                if (node.TypeDefinition == VariableTypeIds.PropertyType)
                {
                    bufferedNode.IsProperty = true;
                }
                Log.Verbose("HandleNode Variable {name}", bufferedNode.DisplayName);
                commonQueue.Enqueue(bufferedNode);
            }
        }
        private IEnumerable<BufferedDataPoint> ToDataPoint(DataValue value, NodeExtractionState variable, string uniqueId)
        {
            if (variable.ArrayDimensions != null && variable.ArrayDimensions.Length > 0 && variable.ArrayDimensions[0] > 0)
            {
                var ret = new List<BufferedDataPoint>();
                if (!(value.Value is Array))
                {
                    Log.Warning("Bad datapoint on variable {BadPointName}, {BadPointValue}", uniqueId, value.Value.ToString());
                    return new BufferedDataPoint[0];
                }
                var values = (Array)value.Value;
                for (int i = 0; i < Math.Min(variable.ArrayDimensions[0], values.Length); i++)
                {
                    var dp = variable.DataType.isString
                        ? new BufferedDataPoint(
                            value.SourceTimestamp,
                            $"{uniqueId}[{i}]",
                            UAClient.ConvertToString(values.GetValue(i)))
                        : new BufferedDataPoint(
                            value.SourceTimestamp,
                            $"{uniqueId}[{i}]",
                            UAClient.ConvertToDouble(values.GetValue(i)));
                    if (!dp.isString && !double.IsFinite(dp.doubleValue))
                    {
                        if (config.Extraction.NonFiniteReplacement != null)
                        {
                            dp.doubleValue = config.Extraction.NonFiniteReplacement.Value;
                        }
                        else
                        {
                            continue;
                        }
                    }
                    ret.Add(dp);
                }
                return ret;
            }
            var sdp = variable.DataType.isString
                ? new BufferedDataPoint(
                    value.SourceTimestamp,
                    uniqueId,
                    UAClient.ConvertToString(value.Value))
                : new BufferedDataPoint(
                    value.SourceTimestamp,
                    uniqueId,
                    UAClient.ConvertToDouble(value.Value));

            if (!sdp.isString && !double.IsFinite(sdp.doubleValue))
            {
                if (config.Extraction.NonFiniteReplacement != null)
                {
                    sdp.doubleValue = config.Extraction.NonFiniteReplacement.Value;
                }
                else
                {
                    return new BufferedDataPoint[0];
                }
            }
            return new BufferedDataPoint[1] { sdp };
        }
        /// <summary>
        /// Handles notifications on subscribed items, pushes all new datapoints to the queue.
        /// </summary>
        /// <param name="item">Modified item</param>
        private void SubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs)
        {
            string uniqueId = UAClient.GetUniqueId(item.ResolvedNodeId);
            var node = NodeStates[item.ResolvedNodeId];

            foreach (var datapoint in item.DequeueValues())
            {
                if (StatusCode.IsNotGood(datapoint.StatusCode))
                {
                    Log.Warning("Bad datapoint: {BadDatapointExternalId}", uniqueId);
                    continue;
                }
                var buffDps = ToDataPoint(datapoint, node, uniqueId);
                node.UpdateFromStream(buffDps);
                if (!node.IsStreaming) return;
                foreach (var buffDp in buffDps)
                {
                    Log.Verbose("Subscription DataPoint {dp}", buffDp.ToDebugDescription());
                }
                foreach (var pusher in pushers)
                {
                    foreach (var buffDp in buffDps)
                    {
                        pusher.BufferedDPQueue.Enqueue(buffDp);
                    }
                }
            }
        }
        private BufferedEvent ConstructEvent(EventFilter filter, VariantCollection eventFields)
        {
            var eventTypeIndex = filter.SelectClauses.FindIndex(atr => atr.TypeDefinitionId == ObjectTypeIds.BaseEventType && atr.BrowsePath[0] == BrowseNames.EventType);
            if (eventTypeIndex < 0)
            {
                Log.Warning("Triggered event has no type, ignoring.");
                return null;
            }
            var eventType = eventFields[eventTypeIndex].Value as NodeId;
            // Many servers don't handle filtering on history data.
            if (!ActiveEvents.ContainsKey(eventType)) return null;
            var targetEventFields = ActiveEvents[eventType];

            var extractedProperties = new Dictionary<string, object>();

            for (int i = 0; i < filter.SelectClauses.Count; i++)
            {
                var clause = filter.SelectClauses[i];
                if (targetEventFields.Any(field => field.Item1 == clause.TypeDefinitionId && field.Item2 == clause.BrowsePath[0] && clause.BrowsePath.Count == 1))
                {
                    var name = clause.BrowsePath[0].Name;
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
            }
            try
            {
                var sourceNode = extractedProperties["SourceNode"];
                if (!managedNodes.Contains(sourceNode)) return null;
                var buffEvent = new BufferedEvent
                {
                    Message = UAClient.ConvertToString(extractedProperties.GetValueOrDefault("Message")),
                    EventId = config.Extraction.IdPrefix + Convert.ToBase64String((byte[])extractedProperties["EventId"]),
                    SourceNode = (NodeId)extractedProperties["SourceNode"],
                    Time = (DateTime)extractedProperties.GetValueOrDefault("Time"),
                    EventType = (NodeId)extractedProperties["EventType"],
                    MetaData = extractedProperties
                        .Where(kvp => kvp.Key != "Message" && kvp.Key != "EventId" && kvp.Key != "SourceNode" && kvp.Key != "Time" && kvp.Key != "EventType")
                        .ToDictionary(kvp => kvp.Key, kvp => kvp.Value)
                };
                return buffEvent;
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed to construct bufferedEvent from raw fields");
                return null;
            }
        }
        private void EventSubscriptionHandler(MonitoredItem item, MonitoredItemNotificationEventArgs eventArgs)
        {
            if (!(eventArgs.NotificationValue is EventFieldList triggeredEvent))
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
            var buffEvent = ConstructEvent(filter, eventFields);
            if (buffEvent == null) return;
            var eventState = EventEmitterStates[item.ResolvedNodeId];
            eventState.UpdateFromStream(buffEvent);
            if (!eventState.IsStreaming) return;
            Log.Debug(buffEvent.ToDebugDescription());
            foreach (var pusher in pushers)
            {
                pusher.BufferedEventQueue.Enqueue(buffEvent);
            }
        }
        /// <summary>
        /// Callback for HistoryRead operations. Simply pushes all datapoints to the queue.
        /// </summary>
        /// <param name="data">Collection of data to be handled</param>
        /// <param name="final">True if this is the final call for this node, and the lock may be removed</param>
        /// <param name="nodeid">Id of the node in question</param>
        private int HistoryDataHandler(IEncodeable rawData, bool final, NodeId nodeid, HistoryReadDetails details)
        {
            if (rawData == null) return 0;
            if (!(rawData is HistoryData data))
            {
                Log.Warning("Incorrect result type of history read data");
                return 0;
            }
            if (data == null || data.DataValues == null) return 0;
            var nodeState = NodeStates[nodeid];

            string uniqueId = UAClient.GetUniqueId(nodeid);

            DateTime last = data.DataValues.Max(dp => dp.SourceTimestamp);
            nodeState.UpdateFromFrontfill(last, final);
            int cnt = 0;
            foreach (var datapoint in data.DataValues)
            {
                if (StatusCode.IsNotGood(datapoint.StatusCode))
                {
                    Log.Warning("Bad datapoint: {BadDatapointExternalId}", uniqueId);
                    continue;
                }
                var buffDps = ToDataPoint(datapoint, nodeState, uniqueId);
                foreach (var buffDp in buffDps)
                {
                    Log.Verbose("History DataPoint {dp}", buffDp.ToDebugDescription());
                    cnt++;
                }
                foreach (var pusher in pushers)
                {
                    foreach (var buffDp in buffDps)
                    {
                        pusher.BufferedDPQueue.Enqueue(buffDp);
                    }
                }
            }
            if (final)
            {
                var buffered = nodeState.FlushBuffer();
                foreach (var pusher in pushers)
                {
                    foreach (var dplist in buffered)
                    {
                        foreach (var dp in dplist)
                        {
                            pusher.BufferedDPQueue.Enqueue(dp);
                        }
                    }
                }
            }
            return cnt;
        }
        private int HistoryEventHandler(IEncodeable rawEvts, bool final, NodeId nodeid, HistoryReadDetails details)
        {
            if (rawEvts == null) return 0;
            if (!(rawEvts is HistoryEvent evts))
            {
                Log.Warning("Incorrect return type of history read events");
                return 0;
            }
            if (!(details is ReadEventDetails eventDetails))
            {
                Log.Warning("Incorrect details type of history read events");
                return 0;
            }
            var filter = eventDetails.Filter;
            if (filter == null)
            {
                Log.Warning("No event filter, ignoring");
                return 0;
            }
            if (evts == null || evts.Events == null) return 0;
            var emitterState = EventEmitterStates[nodeid];
            DateTime last = DateTime.MinValue;
            int cnt = 0;
            foreach (var evt in evts.Events)
            {
                var buffEvt = ConstructEvent(filter, evt.EventFields);
                if (buffEvt == null) continue;
                if (buffEvt.Time > last)
                {
                    last = buffEvt.Time;
                }
                foreach (var pusher in pushers)
                {
                    pusher.BufferedEventQueue.Enqueue(buffEvt);
                }
                cnt++;
            }
            emitterState.UpdateFromFrontfill(last, final);
            if (final)
            {
                var buffered = emitterState.FlushBuffer();
                foreach (var pusher in pushers)
                {
                    foreach (var evt in buffered)
                    {
                        pusher.BufferedEventQueue.Enqueue(evt);
                    }
                }
            }
            return cnt;
        }
        #endregion
    }
}
