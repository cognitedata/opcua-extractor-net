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
using Cognite.OpcUa.Pushers;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.NodeSources
{
    public class CDFNodeSource : BaseNodeSource
    {
        private readonly CDFPusher pusher;
        private readonly CDFNodeSourceConfig sourceConfig;
        private readonly string database;

        public CDFNodeSource(ILogger<CDFNodeSource> log, FullConfig config, UAExtractor extractor, UAClient client, CDFPusher pusher)
            : base(log, config, extractor, client)
        {
            if (config.Cognite?.RawNodeBuffer == null) throw new InvalidOperationException("RawNodeBuffer config required");
            if (config.Cognite.RawNodeBuffer.Database == null) throw new ConfigurationException("Database must be set");
            database = config.Cognite.RawNodeBuffer.Database;
            this.pusher = pusher;
            sourceConfig = config.Cognite.RawNodeBuffer;
        }
        private readonly List<UAVariable> readVariables = new List<UAVariable>();
        private readonly List<UANode> readNodes = new List<UANode>();

        private static async Task<IEnumerable<SavedNode>?> DeserializeRawData(IEnumerable<RawRow<Dictionary<string, JsonElement>>> rows, JsonSerializerOptions options, CancellationToken token)
        {
            using var stream = new MemoryStream();
            await JsonSerializer.SerializeAsync(stream, rows.Select(row => row.Columns), options, token);
            stream.Seek(0, SeekOrigin.Begin);
            return JsonSerializer.Deserialize<IEnumerable<SavedNode>>(stream, options);
        }

        public async Task ReadRawNodes(CancellationToken token)
        {
            var options = new JsonSerializerOptions();
            Extractor.StringConverter.AddConverters(options, ConverterType.Node);

            var nodeSet = new HashSet<NodeId>();

            bool dataEnabled = Config.Subscriptions.DataPoints || Config.History.Enabled && Config.History.Data;
            bool eventsEnabled = Config.Subscriptions.Events || Config.History.Enabled && Config.Events.History;
            eventsEnabled = eventsEnabled && Config.Events.Enabled;

            if ((dataEnabled || eventsEnabled) && !string.IsNullOrEmpty(sourceConfig.TimeseriesTable))
            {
                IEnumerable<SavedNode> nodes;
                try
                {
                    var tsData = await pusher.GetRawRows(database, sourceConfig.TimeseriesTable, new[] {
                        "NodeId", "ParentNodeId", "name", "DataTypeId", "InternalInfo"
                    }, token);
                    nodes = await DeserializeRawData(tsData, options, token) ?? Enumerable.Empty<SavedNode>();
                }
                catch (Exception ex)
                {
                    Log.LogError("Failed to retrieve and deserialize raw timeseries from CDF: {Message}", ex.Message);
                    return;
                }

                foreach (var node in nodes)
                {
                    if (node.NodeId == null || node.NodeId.IsNullNodeId || !nodeSet.Add(node.NodeId)) continue;
                    string? name = node.Name;
                    if (name == null || node.InternalInfo == null) continue;
                    // If this is an array element, we need to strip the postfix from the name, since we are treating it
                    // as its parent.
                    if (node.InternalInfo.ArrayDimensions != null && node.InternalInfo.Index >= 0)
                    {
                        var postfix = $"[{node.InternalInfo.Index}]";
                        name = name.Substring(0, name.Length - postfix.Length);
                    }
                    var variable = new UAVariable(node.NodeId, name, node.ParentNodeId ?? NodeId.Null, node.InternalInfo.NodeClass)
                    {
                        VariableAttributes =
                        {
                            AccessLevel = node.InternalInfo.AccessLevel,
                            ArrayDimensions = node.InternalInfo.ArrayDimensions,
                            DataType = Extractor.DataTypeManager.GetDataType(node.DataTypeId),
                            EventNotifier = node.InternalInfo.EventNotifier,
                            ShouldSubscribeData = node.InternalInfo.ShouldSubscribeData,
                            ShouldSubscribeEvents = node.InternalInfo.ShouldSubscribeEvents,
                            ValueRank = node.InternalInfo.ValueRank,
                            Historizing = node.InternalInfo.Historizing
                        },
                        Source = NodeSource.CDF,
                        AsEvents = node.InternalInfo.AsEvents
                    };
                    variable.VariableAttributes.InitializeAfterRead(Config);
                    readVariables.Add(variable);
                }
            }

            if (eventsEnabled && !string.IsNullOrEmpty(sourceConfig.AssetsTable))
            {
                IEnumerable<SavedNode> nodes;
                try
                {
                    var assetData = await pusher.GetRawRows(database, sourceConfig.AssetsTable, new[]
                    {
                        "NodeId", "ParentNodeId", "name", "InternalInfo"
                    }, token);
                    nodes = await DeserializeRawData(assetData, options, token) ?? Enumerable.Empty<SavedNode>();
                }
                catch (Exception ex)
                {
                    Log.LogError("Failed to retrieve and deserialize raw assets from CDF: {Message}", ex.Message);
                    return;
                }

                foreach (var node in nodes)
                {
                    if (node.NodeId == null || node.NodeId.IsNullNodeId || !nodeSet.Add(node.NodeId)) continue;
                    if (node.Name == null || node.InternalInfo == null) continue;

                    var obj = new UANode(node.NodeId, node.Name, node.ParentNodeId ?? NodeId.Null, node.InternalInfo.NodeClass)
                    {
                        Attributes =
                        {
                            EventNotifier = node.InternalInfo.EventNotifier,
                            ShouldSubscribeEvents = node.InternalInfo.ShouldSubscribeEvents
                        },
                        Source = NodeSource.CDF
                    };
                    readNodes.Add(obj);
                }
            }
            Log.LogInformation("Retrieved {Obj} objects and {Var} variables from CDF Raw", readNodes.Count, readVariables.Count);
        }

        public override async Task<NodeSourceResult?> ParseResults(CancellationToken token)
        {
            if (!readVariables.Any() && !readNodes.Any()) return null;

            await GetExtraNodeData(token);

            FinalDestinationObjects.AddRange(readNodes);
            FinalSourceObjects.AddRange(readNodes);
            foreach (var variable in readVariables)
            {
                AddVariableToLists(variable);
            }

            readNodes.Clear();
            readVariables.Clear();

            if (!FinalDestinationObjects.Any() && !FinalDestinationVariables.Any() && !FinalSourceVariables.Any() && !FinalReferences.Any())
            {
                Log.LogInformation("Mapping resulted in no new nodes");
                return null;
            }

            foreach (var node in FinalSourceObjects.Concat(FinalSourceVariables))
            {
                InitNodeState(Config.Extraction.Update, node);
            }

            Log.LogInformation("Mapping resulted in {ObjCount} destination objects and {TsCount} destination timeseries," +
                " {SourceObj} objects and {SourceVar} variables.",
                FinalDestinationObjects.Count, FinalDestinationVariables.Count,
                FinalSourceObjects.Count, FinalSourceVariables.Count);

            return new NodeSourceResult(
                FinalSourceObjects,
                FinalSourceVariables,
                FinalDestinationObjects,
                FinalDestinationVariables,
                FinalReferences,
                false);
        }

        private async Task GetExtraNodeData(CancellationToken token)
        {
            // Datatype metadata might make sense if we have enum variables, either way this is cheap.
            var distinctDataTypes = readVariables.Select(variable => variable.DataType.Raw).ToHashSet();
            await Extractor.DataTypeManager.GetDataTypeMetadataAsync(distinctDataTypes, token);
        }
    }
}
