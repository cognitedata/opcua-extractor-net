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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Opc.Ua;
using Opc.Ua.Client;
using Serilog;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;
using YamlDotNet.Serialization.TypeInspectors;

namespace Cognite.OpcUa.Config
{
    public static class ToolUtil
    {
        private static readonly ILogger log = Log.Logger.ForContext(typeof(ToolUtil));

        /// <summary>
        /// Run with timeout, returning the result of the task or throwing a TimeoutException
        /// </summary>
        /// <typeparam name="T">Type of return value</typeparam>
        /// <param name="toRun">Task to run</param>
        /// <param name="timeoutSec">Seconds before timeout</param>
        /// <returns>The return value of toRun if it completed within timeout.</returns>
        public static async Task<T> RunWithTimeout<T>(Task<T> toRun, int timeoutSec)
        {
            if (toRun == null) throw new ArgumentNullException(nameof(toRun));
            await Task.WhenAny(Task.Delay(TimeSpan.FromSeconds(timeoutSec)), toRun);
            if (!toRun.IsCompleted) throw new TimeoutException();
            return toRun.Result;
        }
        /// <summary>
        /// Run with timeout, returning nothing or throwing a TimeoutException
        /// </summary>
        /// <param name="toRun">Task to run</param>
        /// <param name="timeoutSec">Seconds before timeout</param>
        public static async Task RunWithTimeout(Task toRun, int timeoutSec)
        {
            if (toRun == null) throw new ArgumentNullException(nameof(toRun));
            await Task.WhenAny(Task.Delay(TimeSpan.FromSeconds(timeoutSec)), toRun);
            if (!toRun.IsCompleted) throw new TimeoutException();
        }
        /// <summary>
        /// Run with timeout, returning nothing or throwing a TimeoutException
        /// </summary>
        /// <param name="toRun">Action to run</param>
        /// <param name="timeoutSec">Seconds before timeout</param>
        public static Task RunWithTimeout(Action toRun, int timeoutSec)
        {
            return RunWithTimeout(Task.Run(toRun), timeoutSec);
        }
        /// <summary>
        /// Run with timeout, returning the result of the task or throwing a TimeoutException
        /// </summary>
        /// <typeparam name="T">Type of return value</typeparam>
        /// <param name="toRun">Action to run</param>
        /// <param name="timeoutSec">Seconds before timeout</param>
        /// <returns>The return value of toRun if it completed within timeout.</returns>
        public static Task<T> RunWithTimeout<T>(Func<T> toRun, int timeoutSec)
        {
            return RunWithTimeout(Task.Run(toRun), timeoutSec);
        }
        /// <summary>
        /// Returns true if, given the list of nodes, the node child has a parent/grandparent with id parent
        /// </summary>
        /// <param name="nodes">Node hierarchy</param>
        /// <param name="child">Child to look for parents for</param>
        /// <param name="parent">Parent to look for</param>
        /// <returns>True if child is descendant of parent</returns>
        public static bool IsChildOf(IEnumerable<BufferedNode> nodes, BufferedNode child, NodeId parent)
        {
            var next = child ?? throw new ArgumentNullException(nameof(child));

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
        /// <summary>
        /// Callback for browse, writes the resulting nodes to a list
        /// </summary>
        /// <param name="target">List to write to</param>
        /// <param name="client">UAClient instance for namespaces</param>
        /// <returns>Callback for Browse in UAClient</returns>
        public static Action<ReferenceDescription, NodeId> GetSimpleListWriterCallback(List<BufferedNode> target, UAClient client)
        {
            return (node, parentId) =>
            {
                if (node.NodeClass == NodeClass.Object || node.NodeClass == NodeClass.DataType || node.NodeClass == NodeClass.ObjectType)
                {
                    var bufferedNode = new BufferedNode(client.ToNodeId(node.NodeId),
                        node.DisplayName.Text, parentId);
                    log.Verbose("HandleNode Object {name}", bufferedNode.DisplayName);
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

                    log.Verbose("HandleNode Variable {name}", bufferedNode.DisplayName);
                    target.Add(bufferedNode);
                }
            };
        }
        public static IEnumerable<BufferedDataPoint> ToDataPoint(DataValue value, NodeExtractionState variable, string uniqueId, UAClient client)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (variable == null || value == null) return Array.Empty<BufferedDataPoint>();
            if (variable.ArrayDimensions != null && variable.ArrayDimensions.Count > 0 && variable.ArrayDimensions[0] > 0)
            {
                var ret = new List<BufferedDataPoint>();
                if (!(value.Value is Array))
                {
                    log.Debug("Bad array datapoint: {BadPointName} {BadPointValue}", uniqueId, value.Value.ToString());
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
        /// <summary>
        /// Get a subscription handler that writes datapoints to a list
        /// </summary>
        /// <param name="points">Target list to write to</param>
        /// <param name="states">Overview of states to use</param>
        /// <param name="client">UAClient for namespaces</param>
        /// <returns>Subscription handler for datapoints</returns>
        public static MonitoredItemNotificationEventHandler GetSimpleListWriterHandler(
            List<BufferedDataPoint> points,
            IDictionary<NodeId, NodeExtractionState> states,
            UAClient client)
        {
            return (item, args) =>
            {
                try
                {
                    string uniqueId = client.GetUniqueId(item.ResolvedNodeId);
                    var state = states[item.ResolvedNodeId];
                    foreach (var datapoint in item.DequeueValues())
                    {
                        if (StatusCode.IsNotGood(datapoint.StatusCode))
                        {
                            log.Debug("Bad streaming datapoint: {BadDatapointExternalId} {SourceTimestamp}", uniqueId,
                                datapoint.SourceTimestamp);
                            continue;
                        }

                        var buffDps = ToDataPoint(datapoint, state, uniqueId, client);
                        state.UpdateFromStream(buffDps);
                        foreach (var buffDp in buffDps)
                        {
                            log.Verbose("Subscription DataPoint {dp}", buffDp.ToDebugDescription());
                        }

                        points.AddRange(buffDps);
                    }
                }
                catch (Exception ex)
                {
                    Log.Warning(ex, "Error in list writer callback");
                }

            };
        }

        public static BufferedDataPoint[] ReadResultToDataPoints(IEncodeable rawData, NodeExtractionState state, UAClient client)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (rawData == null || state == null) return Array.Empty<BufferedDataPoint>();
            if (!(rawData is HistoryData data))
            {
                log.Warning("Incorrect result type of history read data");
                return Array.Empty<BufferedDataPoint>();
            }

            if (data.DataValues == null) return Array.Empty<BufferedDataPoint>();
            string uniqueId = state.Id;

            var result = new List<BufferedDataPoint>();

            foreach (var datapoint in data.DataValues)
            {
                if (StatusCode.IsNotGood(datapoint.StatusCode))
                {
                    log.Debug("Bad history datapoint: {BadDatapointExternalId} {SourceTimestamp}", uniqueId,
                        datapoint.SourceTimestamp);
                    continue;
                }

                var buffDps = ToDataPoint(datapoint, state, uniqueId, client);
                foreach (var buffDp in buffDps)
                {
                    log.Verbose("History DataPoint {dp}", buffDp.ToDebugDescription());
                    result.Add(buffDp);
                }
            }

            return result.ToArray();
        }

        
        /// <summary>
        /// Intelligently converts an instance of FullConfig to a string config file. Only writing entries that differ from the default values.
        /// </summary>
        /// <param name="config">Config to convert</param>
        /// <returns>Final config string, can be written directly to file or parsed further</returns>
        public static string ConfigResultToString(FullConfig config)
        {
            var serializer = new SerializerBuilder()
                .WithTagMapping("!cdf", typeof(CognitePusherConfig))
                .WithTagMapping("!influx", typeof(InfluxPusherConfig))
                .WithTypeInspector(insp => new DefaultFilterTypeInspector(insp))
                .WithNamingConvention(HyphenatedNamingConvention.Instance)
                .Build();

            var clearEmptyRegex = new Regex("^\\s*[a-zA-Z-_\\d]*:\\s*({}|\\[\\])\\s*\n", RegexOptions.Multiline);
            var doubleIndentRegex = new Regex("(^ +)", RegexOptions.Multiline);
            var fixListIndentRegex = new Regex("(^ +-)", RegexOptions.Multiline);

            string raw = serializer.Serialize(config);
            raw = clearEmptyRegex.Replace(raw, "");
            raw = doubleIndentRegex.Replace(raw, "$1$1");
            raw = fixListIndentRegex.Replace(raw, "  $1");


            string configText = "# This suggested configuration was generated by the ConfigurationTool.\n\n"
                             + raw;

            return configText;
        }
        public static bool NodeNameContains(BufferedNode node, string str)
        {
            if (node == null) return false;
            string identifier = node.Id.IdType == IdType.String ? (string)node.Id.Identifier : null;
            return identifier != null && identifier.StartsWith(str, StringComparison.InvariantCultureIgnoreCase)
                || node.DisplayName != null && node.DisplayName.StartsWith(str, StringComparison.InvariantCultureIgnoreCase);
        }
        public static bool NodeNameStartsWith(BufferedNode node, string str)
        {
            if (node == null) return false;
            string identifier = node.Id.IdType == IdType.String ? (string)node.Id.Identifier : null;
            return identifier != null && identifier.Contains(str, StringComparison.InvariantCultureIgnoreCase)
                        || node.DisplayName != null && node.DisplayName.Contains(str, StringComparison.InvariantCultureIgnoreCase);
        }
    }
    public class DefaultFilterTypeInspector : TypeInspectorSkeleton
    {
        private readonly ITypeInspector innerTypeDescriptor;
        public DefaultFilterTypeInspector(ITypeInspector innerTypeDescriptor)
        {
            this.innerTypeDescriptor = innerTypeDescriptor;
        }

        public override IEnumerable<IPropertyDescriptor> GetProperties(Type type, object container)
        {
            var props = innerTypeDescriptor.GetProperties(type, container);

            var dfs = Activator.CreateInstance(type);

            props = props.Where(p =>
            {
                var name = PascalCaseNamingConvention.Instance.Apply(p.Name);
                var prop = type.GetProperty(name);
                var df = prop?.GetValue(dfs);
                var val = prop?.GetValue(container);

                // Some config objects have private properties, since this is a write-back of config we shouldn't save those
                if (!p.CanWrite) return false;
                // Some custom properties are kept on the config object for convenience
                if (name == "ConfigDir") return false;
                // A few properties are kept in order to encourage the user to define them
                if (name == "IdPrefix") return true;
                // Compare the value of each property with its default, and check for empty arrays, don't save those.
                // This creates minimal config files
                if (val != null && (val is IEnumerable list) && !list.GetEnumerator().MoveNext()) return false;

                return df != null && !df.Equals(val) || df == null && val != null;
            });

            return props;
        }
    }
}
