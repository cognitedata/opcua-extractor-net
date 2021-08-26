using Cognite.Extractor.Common;
using Cognite.OpcUa.Pushers;
using Cognite.OpcUa.Types;
using CogniteSdk;
using Newtonsoft.Json;
using Opc.Ua;
using Serilog;
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
    public class CDFNodeSource : BaseNodeSource
    {
        private readonly CDFPusher pusher;
        private readonly CDFNodeSourceConfig sourceConfig;
        private readonly ILogger log = Log.Logger.ForContext(typeof(CDFNodeSource));
        private readonly string database;

        public CDFNodeSource(FullConfig config, UAExtractor extractor, UAClient client, CDFPusher pusher)
            : base(config, extractor, client)
        {
            if (config.Cognite?.RawNodeBuffer == null) throw new InvalidOperationException("RawNodeBuffer config required");
            if (config.Cognite.RawNodeBuffer.Database == null) throw new ConfigurationException("Database must be set");
            database = config.Cognite.RawNodeBuffer.Database;
            this.pusher = pusher;
            sourceConfig = config.Cognite.RawNodeBuffer;
        }
        private readonly List<UAVariable> readVariables = new List<UAVariable>();
        private readonly List<UANode> readNodes = new List<UANode>();

        private static async Task<IEnumerable<SavedNode>?> DeserializeRawData(IEnumerable<RawRow> rows, JsonSerializer serializer, CancellationToken token)
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
            Extractor.StringConverter.AddConverters(serializer, ConverterType.Node);

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
                    nodes = await DeserializeRawData(tsData, serializer, token) ?? Enumerable.Empty<SavedNode>();
                }
                catch (Exception ex)
                {
                    log.Error("Failed to retrieve and deserialize raw timeseries from CDF: {msg}", ex.Message);
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
                            ShouldSubscribe = node.InternalInfo.ShouldSubscribe,
                            ValueRank = node.InternalInfo.ValueRank,
                        },
                        Source = NodeSource.CDF
                    };
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
                    nodes = await DeserializeRawData(assetData, serializer, token) ?? Enumerable.Empty<SavedNode>();
                }
                catch (Exception ex)
                {
                    log.Error("Failed to retrieve and deserialize raw assets from CDF: {msg}", ex.Message);
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
                            ShouldSubscribe = node.InternalInfo.ShouldSubscribe
                        },
                        Source = NodeSource.CDF
                    };
                    readNodes.Add(obj);
                }
            }
            log.Information("Retrieved {as} objects and {ts} variables from CDF Raw", readNodes.Count, readVariables.Count);
        }


        public override async Task<BrowseResult?> ParseResults(CancellationToken token)
        {
            if (!readVariables.Any() && !readNodes.Any()) return null;

            await GetExtraNodeData(token);

            finalDestinationObjects.AddRange(readNodes);
            finalSourceObjects.AddRange(readNodes);
            foreach (var variable in readVariables)
            {
                if (!Extractor.DataTypeManager.AllowTSMap(variable)) continue;
                AddVariableToLists(variable);
            }

            readNodes.Clear();
            readVariables.Clear();

            if (!finalDestinationObjects.Any() && !finalDestinationVariables.Any() && !finalSourceVariables.Any() && !finalReferences.Any())
            {
                log.Information("Mapping resulted in no new nodes");
                return null;
            }

            foreach (var node in finalSourceObjects.Concat(finalSourceVariables))
            {
                InitNodeState(Config.Extraction.Update, node);
            }

            log.Information("Mapping resulted in {obj} destination objects and {ts} destination timeseries," +
                " {robj} objects and {var} variables.",
                finalDestinationObjects.Count, finalDestinationVariables.Count,
                finalSourceObjects.Count, finalSourceVariables.Count);

            return new BrowseResult(
                finalSourceObjects,
                finalSourceVariables,
                finalDestinationObjects,
                finalDestinationVariables,
                finalReferences);
        }

        private async Task GetExtraNodeData(CancellationToken token)
        {
            // Datatype metadata might make sense if we have enum variables, either way this is cheap.
            var distinctDataTypes = readVariables.Select(variable => variable.DataType.Raw).ToHashSet();
            await Extractor.DataTypeManager.GetDataTypeMetadataAsync(distinctDataTypes, token);
        }
    }
}
