using Cognite.Extractor.Common;
using Cognite.OpcUa.Pushers;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Newtonsoft.Json;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using YamlDotNet.Serialization;

namespace Cognite.OpcUa.NodeSources
{
    public class CDFNodeSource : INodeSource
    {
        private readonly FullConfig fullConfig;
        private readonly UAExtractor extractor;
        private readonly UAClient client;
        private readonly CDFPusher pusher;
        private readonly CDFNodeSourceConfig config;

        public CDFNodeSource(FullConfig config, UAExtractor extractor, UAClient client, CDFPusher pusher)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            fullConfig = config;
            this.extractor = extractor;
            this.client = client;
            this.pusher = pusher;
            this.config = config.Cognite.RawNodeBuffer;
        }
        private readonly List<UAVariable> readVariables = new List<UAVariable>();
        private readonly List<UANode> readNodes = new List<UANode>();

        private static async Task<IEnumerable<SavedNode>> DeserializeRawData(IEnumerable<RawRow> rows, JsonSerializer serializer, CancellationToken token)
        {
            using var stream = new MemoryStream();
            await System.Text.Json.JsonSerializer.SerializeAsync(stream, rows.Select(row => row.Columns), null, token);
            stream.Seek(0, SeekOrigin.Begin);
            using var sr = new StreamReader(stream);
            using var reader = new JsonTextReader(sr);
            return serializer.Deserialize<IEnumerable<SavedNode>>(reader);
        }

        public async Task ReadRawNodes(CancellationToken token)
        {
            var serializer = new JsonSerializer();
            extractor.StringConverter.AddConverters(serializer, ConverterType.Node);

            var nodeSet = new HashSet<NodeId>();

            bool dataEnabled = fullConfig.Subscriptions.DataPoints || fullConfig.History.Enabled && fullConfig.History.Data;
            bool eventsEnabled = fullConfig.Subscriptions.Events || fullConfig.History.Enabled && fullConfig.Events.History;
            eventsEnabled = eventsEnabled && fullConfig.Events.Enabled;

            if ((dataEnabled || eventsEnabled) && !string.IsNullOrEmpty(config.TimeseriesTable))
            {
                var tsData = await pusher.GetRawRows(config.Database, config.TimeseriesTable, new[] {
                    "NodeId", "ParentNodeId", "name", "DataTypeId", "InternalInfo"
                }, token);

                var nodes = await DeserializeRawData(tsData, serializer, token);
                foreach (var node in nodes)
                {
                    if (node.NodeId == null || node.NodeId.IsNullNodeId || !nodeSet.Add(node.NodeId)) continue;
                    var variable = new UAVariable(node.NodeId, node.Name, node.ParentNodeId, NodeClass.Variable);
                    variable.VariableAttributes.AccessLevel = node.InternalInfo.AccessLevel;
                    variable.VariableAttributes.ArrayDimensions = new Collection<int>(node.InternalInfo.ArrayDimensions);
                    variable.VariableAttributes.DataType = extractor.DataTypeManager.GetDataType(node.DataTypeId);
                    variable.VariableAttributes.EventNotifier = node.InternalInfo.EventNotifier;
                    variable.VariableAttributes.Historizing = node.InternalInfo.Historizing;
                    variable.VariableAttributes.ShouldSubscribe = node.InternalInfo.ShouldSubscribe;
                    variable.VariableAttributes.ValueRank = node.InternalInfo.ValueRank;
                    readVariables.Add(variable);
                }
            }

            if (eventsEnabled && !string.IsNullOrEmpty(config.AssetsTable))
            {
                var assetData = await pusher.GetRawRows(config.Database, config.TimeseriesTable, new[]
                {
                    "NodeId", "ParentNodeId", "name", "InternalInfo"
                }, token);
                var nodes = await DeserializeRawData(assetData, serializer, token);
                foreach (var node in nodes)
                {
                    if (node.NodeId == null || node.NodeId.IsNullNodeId || !nodeSet.Add(node.NodeId)) continue;
                    var obj = new UANode(node.NodeId, node.Name, node.ParentNodeId, NodeClass.Object);
                    obj.Attributes.EventNotifier = node.InternalInfo.EventNotifier;
                    obj.Attributes.ShouldSubscribe = node.InternalInfo.ShouldSubscribe;
                    readNodes.Add(obj);
                }
            }
            

        }


        public async Task<BrowseResult> ParseResults(CancellationToken token)
        {
            return null;
        }
    }
}
