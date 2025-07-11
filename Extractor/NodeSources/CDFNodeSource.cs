/* Cognite Extractor for OPC-UA
Copyright (C) 2023 Cognite AS

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
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Pushers;
using Cognite.OpcUa.TypeCollectors;
using CogniteSdk;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using System.Threading;
using Opc.Ua;
using Cognite.OpcUa.Types;

namespace Cognite.OpcUa.NodeSources
{
    public class CDFNodeSource : INodeSource
    {
        private readonly CDFPusher pusher;
        private readonly CDFNodeSourceConfig sourceConfig;
        private readonly string database;

        private UANodeCollection nodeMap = new();

        private readonly TypeManager typeManager;
        private readonly ILogger logger;
        private readonly UAExtractor extractor;
        private readonly FullConfig config;

        public CDFNodeSource(ILogger logger, FullConfig config, UAExtractor extractor, CDFPusher pusher, TypeManager typeManager)
        {
            if (config.Cognite?.RawNodeBuffer == null) throw new InvalidOperationException("RawNodeBuffer config required");
            if (config.Cognite.RawNodeBuffer.Database == null) throw new ConfigurationException("Database must be set");
            database = config.Cognite.RawNodeBuffer.Database;
            this.pusher = pusher;
            sourceConfig = config.Cognite.RawNodeBuffer;
            this.typeManager = typeManager;
            this.logger = logger;
            this.extractor = extractor;
            this.config = config;
        }

        private static async Task<IEnumerable<SavedNode>?> DeserializeRawData(IEnumerable<RawRow<Dictionary<string, JsonElement>>> rows, JsonSerializerOptions options, CancellationToken token)
        {
            using var stream = new MemoryStream();
            await JsonSerializer.SerializeAsync(stream, rows.Select(row => row.Columns), options, token);
            stream.Seek(0, SeekOrigin.Begin);
            return JsonSerializer.Deserialize<IEnumerable<SavedNode>>(stream, options);
        }

        private NodeLoadResult TakeResults()
        {
            var res = new NodeLoadResult(
                nodeMap,
                Enumerable.Empty<UAReference>(),
                true,
                config.Source.AltSourceBackgroundBrowse);
            nodeMap = new();
            return res;
        }

        public Task Initialize(CancellationToken token)
        {
            return Task.CompletedTask;
        }

        public async Task<NodeLoadResult> LoadNodes(IEnumerable<NodeId> nodesToBrowse, uint nodeClassMask, HierarchicalReferenceMode hierarchicalReferences, string purpose, CancellationToken token)
        {
            // Ignores nodesToBrowse, nothing really to do with that here
            var options = new JsonSerializerOptions();
            extractor.TypeConverter.AddConverters(options, ConverterType.Node);

            var nodeSet = new HashSet<NodeId>();

            bool dataEnabled = config.Subscriptions.DataPoints || config.History.Enabled && config.History.Data;
            bool eventsEnabled = config.Subscriptions.Events || config.History.Enabled && config.Events.History;
            eventsEnabled = eventsEnabled && config.Events.Enabled;

            int objCount = 0, varCount = 0;
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
                    logger.LogError(ex, "Failed to retrieve and deserialize raw timeseries from CDF: {Message}", ex.Message);
                    TakeResults();
                    return new NodeLoadResult(new UANodeCollection(), Enumerable.Empty<UAReference>(), true, true);
                }

                foreach (var node in nodes)
                {
                    if (node.NodeId == null || node.NodeId.IsNullNodeId || !nodeSet.Add(node.NodeId)) continue;

                    var res = BaseUANode.FromSavedNode(node, typeManager);
                    if (res is not UAVariable variable)
                    {
                        logger.LogWarning("Node {Id} {Name} in variables table is not a variable, skipping", res?.Id, res?.Name);
                        continue;
                    }

                    if (nodeMap.TryAdd(variable)) varCount++;

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
                    logger.LogError(ex, "Failed to retrieve and deserialize raw assets from CDF: {Message}", ex.Message);
                    TakeResults();
                    return new NodeLoadResult(new UANodeCollection(), Enumerable.Empty<UAReference>(), true, true);
                }

                foreach (var node in nodes)
                {
                    if (node.NodeId == null || node.NodeId.IsNullNodeId || !nodeSet.Add(node.NodeId)) continue;

                    var res = BaseUANode.FromSavedNode(node, typeManager);
                    if (res is not UAObject obj) continue;

                    if (nodeMap.TryAdd(obj)) objCount++;
                }
            }
            logger.LogInformation("Retrieved {Obj} objects and {Var} variables from CDF Raw", objCount, varCount);

            return TakeResults();
        }

        public Task<NodeLoadResult> LoadNonHierarchicalReferences(IReadOnlyDictionary<NodeId, BaseUANode> parentNodes, bool getTypeReferences, bool initUnknownNodes, string purpose, CancellationToken token)
        {
            return Task.FromResult(
                new NodeLoadResult(new UANodeCollection(), Enumerable.Empty<UAReference>(), true, true)
            );
        }
    }
}
