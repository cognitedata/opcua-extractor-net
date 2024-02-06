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

using Cognite.Extractor.Configuration;
using Cognite.OpcUa.History;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Config
{
    public static class ToolUtil
    {
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
            if (toRun.Exception != null) throw new FatalException("Task failed during RunWithTimeout", toRun.Exception);
            return await toRun;
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
            if (toRun.Exception != null) throw new FatalException("Task failed during RunWithTimeout", toRun.Exception);
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
        public static bool IsChildOf(IEnumerable<BaseUANode> nodes, BaseUANode child, NodeId parent)
        {
            var next = child ?? throw new ArgumentNullException(nameof(child));

            do
            {
                if (next.ParentId == parent)
                {
                    return true;
                }
                if (next.ParentId.IsNullNodeId)
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
        public static Action<ReferenceDescription, NodeId, bool> GetSimpleListWriterCallback(ICollection<BaseUANode> target, UAClient client, TypeManager typeManager, ILogger log)
        {
            return (node, parentId, visited) =>
            {
                if (visited) return;
                var res = BaseUANode.Create(node, parentId, null, client, typeManager);
                if (res == null) return;
                log.LogTrace("HandleNode {NodeClass} {Name}", res.NodeClass, res.Name);
                target.Add(res);
            };
        }
        /// <summary>
        /// Transforms a data value into a list of <see cref="UADataPoint"/>,
        /// given a <see cref="VariableExtractionState"/> representing the variable being extracted.
        /// The number of datapoints is given by the smallest of the dimension of <paramref name="value"/>, and
        /// the array dimensions of <paramref name="variable"/>.
        /// </summary>
        /// <param name="value">Value to convert</param>
        /// <param name="variable">Variable that generated the data value</param>
        /// <param name="client">IUAClientAccess with an active connection to the server, for conversions</param>
        /// <returns>A collection of data points</returns>
        public static IEnumerable<UADataPoint> ToDataPoint(
            DataValue value,
            VariableExtractionState variable,
            IUAClientAccess client,
            ILogger log)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (variable == null || value == null) return Array.Empty<UADataPoint>();
            if (variable.IsArray)
            {
                var ret = new List<UADataPoint>();
                if (!(value.Value is Array))
                {
                    log.LogDebug("Bad array datapoint: {BadPointName} {BadPointValue}", variable.Id, value.Value.ToString());
                    return Enumerable.Empty<UADataPoint>();
                }
                var values = (Array)value.Value;
                for (int i = 0; i < Math.Min(variable.ArrayDimensions[0], values.Length); i++)
                {
                    var dp = variable.DataType.IsString
                        ? new UADataPoint(
                            value.SourceTimestamp,
                            $"{variable.Id}[{i}]",
                            client.StringConverter.ConvertToString(values.GetValue(i)),
                            value.StatusCode)
                        : new UADataPoint(
                            value.SourceTimestamp,
                            $"{variable.Id}[{i}]",
                            UAClient.ConvertToDouble(values.GetValue(i)),
                            value.StatusCode);
                    ret.Add(dp);
                }
                return ret;
            }
            var sdp = variable.DataType.IsString
                ? new UADataPoint(
                    value.SourceTimestamp,
                    variable.Id,
                    client.StringConverter.ConvertToString(value.Value),
                    value.StatusCode)
                : new UADataPoint(
                    value.SourceTimestamp,
                    variable.Id,
                    UAClient.ConvertToDouble(value.Value),
                    value.StatusCode);
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
            List<UADataPoint> points,
            IDictionary<NodeId, VariableExtractionState> states,
            UAClient client,
            ILogger log,
            bool writeBadDatapoints)
        {
            return (item, args) =>
            {
                try
                {
                    var state = states[item.ResolvedNodeId];
                    foreach (var datapoint in item.DequeueValues())
                    {
                        if (StatusCode.IsNotGood(datapoint.StatusCode))
                        {
                            log.LogDebug("Bad streaming datapoint: {BadDatapointExternalId} {SourceTimestamp} {Code}", state.Id,
                                datapoint.SourceTimestamp, datapoint.StatusCode);

                            if (!writeBadDatapoints) continue;
                        }

                        var buffDps = ToDataPoint(datapoint, state, client, log);
                        state.UpdateFromStream(buffDps);
                        foreach (var buffDp in buffDps)
                        {
                            log.LogTrace("Subscription DataPoint {DataPoint}", buffDp.ToString());
                        }

                        points.AddRange(buffDps);
                    }
                }
                catch (Exception ex)
                {
                    log.LogWarning(ex, "Error in list writer callback");
                }

            };
        }
        /// <summary>
        /// Transform the result of a history read into an array of <see cref="UADataPoint"/>,
        /// given a <see cref="VariableExtractionState"/> representing the source variable.
        /// </summary>
        /// <param name="rawData">Raw data to be converted</param>
        /// <param name="state">State representing the source variable</param>
        /// <param name="client">Access to a connection to the OPC-UA server</param>
        /// <returns>Array of <see cref="UADataPoint"/> representing the DataValues in the IEncodable</returns>
        public static UADataPoint[] ReadResultToDataPoints(IEncodeable rawData, VariableExtractionState state, IUAClientAccess client, ILogger log)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (rawData == null || state == null) return Array.Empty<UADataPoint>();
#pragma warning disable CA1508 // Avoid dead conditional code - While it is always true here, this should be kept.
            if (!(rawData is HistoryData data))
#pragma warning restore CA1508 // Avoid dead conditional code
            {
                log.LogWarning("Incorrect result type of history read data");
                return Array.Empty<UADataPoint>();
            }

            if (data.DataValues == null) return Array.Empty<UADataPoint>();
            var result = new List<UADataPoint>();

            foreach (var datapoint in data.DataValues)
            {
                if (StatusCode.IsNotGood(datapoint.StatusCode))
                {
                    log.LogDebug("Bad history datapoint: {BadDatapointExternalId} {SourceTimestamp}", state.Id,
                        datapoint.SourceTimestamp);
                    continue;
                }

                var buffDps = ToDataPoint(datapoint, state, client, log);
                foreach (var buffDp in buffDps)
                {
                    log.LogTrace("History DataPoint {DataPoint}", buffDp.ToString());
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
            string raw = ConfigurationUtils.ConfigToString(config,
                new[] { "IdPrefix" },
                new[] { "ConfigDir", "BaseExcludeProperties", "IdpAuthentication", "ApiKey", "Password" },
                new[] { "Cognite" },
                false);

            string configText = $"# This suggested configuration was generated by the ConfigurationTool.{Environment.NewLine}{Environment.NewLine}{raw}";

            return configText;
        }
        /// <summary>
        /// Method to check if a given UANode starts with the given string,
        /// this checks both a potential string identifier on the Id, and the DisplayName.
        /// </summary>
        /// <param name="node">Node to test</param>
        /// <param name="str">String to check</param>
        /// <returns>True if the node name or identifier starts with the given string.</returns>
        public static bool NodeNameStartsWith(BaseUANode node, string str)
        {
            if (node == null) return false;
            string? identifier = node.Id.IdType == IdType.String ? (string)node.Id.Identifier : null;
            return identifier != null && identifier.StartsWith(str, StringComparison.InvariantCultureIgnoreCase)
                || node.Name != null && node.Name.StartsWith(str, StringComparison.InvariantCultureIgnoreCase);
        }
        /// <summary>
        /// Method to check if a given UANode contains the given string,
        /// this checks both a potential string identifier on the Id, and the DisplayName.
        /// </summary>
        /// <param name="node">Node to test</param>
        /// <param name="str">String to check</param>
        /// <returns>True if the node name or identifier contains the given string.</returns>
        public static bool NodeNameContains(BaseUANode node, string str)
        {
            if (node == null) return false;
            string? identifier = node.Id.IdType == IdType.String ? (string)node.Id.Identifier : null;
            return identifier != null && identifier.Contains(str, StringComparison.InvariantCultureIgnoreCase)
                        || node.Name != null && node.Name.Contains(str, StringComparison.InvariantCultureIgnoreCase);
        }
    }
}
