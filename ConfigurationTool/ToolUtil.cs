using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Opc.Ua;
using Opc.Ua.Client;
using Serilog;

namespace Cognite.OpcUa.Config
{
    static class ToolUtil
    {
        public static async Task<T> RunWithTimeout<T>(Task<T> toRun, int timeoutSec)
        {
            await Task.WhenAny(Task.Delay(TimeSpan.FromSeconds(timeoutSec)), toRun);
            if (!toRun.IsCompleted) throw new TimeoutException();
            return toRun.Result;
        }

        public static async Task RunWithTimeout(Task toRun, int timeoutSec)
        {
            await Task.WhenAny(Task.Delay(TimeSpan.FromSeconds(timeoutSec)), toRun);
            if (!toRun.IsCompleted) throw new TimeoutException();
        }

        public static Task RunWithTimeout(Action toRun, int timeoutSec)
        {
            return RunWithTimeout(Task.Run(toRun), timeoutSec);
        }

        public static Task<T> RunWithTimeout<T>(Func<T> toRun, int timeoutSec)
        {
            return RunWithTimeout(Task.Run(toRun), timeoutSec);
        }
        public static bool IsChildOf(IEnumerable<BufferedNode> nodes, BufferedNode child, NodeId parent)
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
        public static Action<ReferenceDescription, NodeId> GetSimpleListWriterCallback(List<BufferedNode> target, UAClient client)
        {
            return (node, parentId) =>
            {
                if (node.NodeClass == NodeClass.Object || node.NodeClass == NodeClass.DataType || node.NodeClass == NodeClass.ObjectType)
                {
                    var bufferedNode = new BufferedNode(client.ToNodeId(node.NodeId),
                        node.DisplayName.Text, parentId);
                    Log.Verbose("HandleNode Object {name}", bufferedNode.DisplayName);
                    target.Add(bufferedNode);
                }
                else if (node.NodeClass == NodeClass.Variable)
                {
                    var bufferedNode = new BufferedVariable(client.ToNodeId(node.NodeId),
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
        public static IEnumerable<BufferedDataPoint> ToDataPoint(DataValue value, NodeExtractionState variable, string uniqueId, UAClient client)
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
                            client.ConvertToString(values.GetValue(i)))
                        : new BufferedDataPoint(
                            value.SourceTimestamp,
                            $"{uniqueId}[{i}]",
                            UAClient.ConvertToDouble(values.GetValue(i)));
                    ret.Add(dp);
                }
                return ret;
            }
            var sdp = variable.DataType.IsString
                ? new BufferedDataPoint(
                    value.SourceTimestamp,
                    uniqueId,
                    client.ConvertToString(value.Value))
                : new BufferedDataPoint(
                    value.SourceTimestamp,
                    uniqueId,
                    UAClient.ConvertToDouble(value.Value));
            return new[] { sdp };
        }

        public static MonitoredItemNotificationEventHandler GetSimpleListWriterHandler(
            List<BufferedDataPoint> points,
            IDictionary<NodeId, NodeExtractionState> states,
            UAClient client)
        {
            return (item, args) =>
            {
                string uniqueId = client.GetUniqueId(item.ResolvedNodeId);
                var state = states[item.ResolvedNodeId];
                foreach (var datapoint in item.DequeueValues())
                {
                    if (StatusCode.IsNotGood(datapoint.StatusCode))
                    {
                        Log.Debug("Bad streaming datapoint: {BadDatapointExternalId} {SourceTimestamp}", uniqueId, datapoint.SourceTimestamp);
                        continue;
                    }
                    var buffDps = ToDataPoint(datapoint, state, uniqueId, client);
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

        public static BufferedDataPoint[] ReadResultToDataPoints(IEncodeable rawData, NodeExtractionState state, UAClient client)
        {
            if (rawData == null) return Array.Empty<BufferedDataPoint>();
            if (!(rawData is HistoryData data))
            {
                Log.Warning("Incorrect result type of history read data");
                return Array.Empty<BufferedDataPoint>();
            }

            if (data.DataValues == null) return Array.Empty<BufferedDataPoint>();
            string uniqueId = client.GetUniqueId(state.Id);

            var result = new List<BufferedDataPoint>();

            foreach (var datapoint in data.DataValues)
            {
                if (StatusCode.IsNotGood(datapoint.StatusCode))
                {
                    Log.Debug("Bad history datapoint: {BadDatapointExternalId} {SourceTimestamp}", uniqueId,
                        datapoint.SourceTimestamp);
                    continue;
                }

                var buffDps = ToDataPoint(datapoint, state, uniqueId, client);
                foreach (var buffDp in buffDps)
                {
                    Log.Verbose("History DataPoint {dp}", buffDp.ToDebugDescription());
                    result.Add(buffDp);
                }
            }

            return result.ToArray();
        }
    }
}
