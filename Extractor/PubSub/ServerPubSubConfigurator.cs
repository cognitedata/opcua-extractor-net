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

using Cognite.OpcUa.Config;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.PubSub
{
    internal class ReaderWrapper
    {
        public TargetVariablesDataType? Targets { get; set; }
        public DataSetReaderDataType Reader { get; }
        public InternalNode ReaderNode { get; }
        public ConnectionWrapper Connection { get; }
        public ReaderWrapper(DataSetReaderDataType reader, InternalNode node, ConnectionWrapper connection)
        {
            Reader = reader;
            ReaderNode = node;
            Connection = connection;
        }
    }

    internal class ConnectionWrapper
    {
        public PubSubConnectionDataType Connection { get; }
        public IList<ReaderWrapper> FinalReaders { get; } = new List<ReaderWrapper>();
        public ConnectionWrapper(PubSubConnectionDataType connection)
        {
            Connection = connection;
        }
    }


    /// <summary>
    /// Class responsible for loading configuration from an OPC-UA server.
    /// </summary>
    public class ServerPubSubConfigurator
    {
        private readonly UAClient client;
        private PubSubConfigurationDataType config;
        private readonly Dictionary<NodeId, InternalNode> nodeMap = new Dictionary<NodeId, InternalNode>();
        private readonly ILogger log;
        private readonly Dictionary<NodeId, ReaderWrapper> readers = new Dictionary<NodeId, ReaderWrapper>();
        private readonly Dictionary<NodeId, ConnectionWrapper> connections = new Dictionary<NodeId, ConnectionWrapper>();
        private readonly PubSubConfig pubSubConfig;

        public ServerPubSubConfigurator(ILogger log, UAClient client, PubSubConfig pubSubConfig)
        {
            this.client = client;
            config = new PubSubConfigurationDataType();
            this.pubSubConfig = pubSubConfig;
            this.log = log;
        }

        private async Task<bool> LoadNodeValues(CancellationToken token)
        {
            var toRead = nodeMap.Values.Where(node => node.NodeClass == NodeClass.Variable).ToList();
            // Some nodes can get _very_ large, we should read these separately.
            var bySize = toRead.GroupBy(node => node.BrowseName == "DataSetMetaData" || node.BrowseName == "PublishedData");
            if (bySize.Count() != 2)
            {
                log.LogWarning("Not enough information to configure pubsub based on server hierarchy");
                return false;
            }
            var values = (await client.ReadRawValues(bySize.First(group => !group.Key).Select(node => node.NodeId), token)).ToList();
            foreach (var node in bySize.First(group => group.Key))
            {
                values.AddRange(await client.ReadRawValues(new[] { node.NodeId }, token));
            }

            foreach (var val in values)
            {
                nodeMap[val.Key].Value = val.Value;
            }
            return true;
        }

        private async Task CorrectWriterParents(CancellationToken token)
        {
            // Due to the weird structure of the hierarchy, writers may end up with data sets as parents,
            // while we need to know their group
            var toCorrect = nodeMap.Values.Where(node =>
                node.TypeDefinition == ObjectTypeIds.DataSetWriterType
                && node?.Parent?.TypeDefinition != ObjectTypeIds.WriterGroupType)
                .ToList();

            foreach (var node in toCorrect) node.AltParent = node.Parent;

            log.LogInformation("Browse data set writers in reverse to obtain their parents");

            var result = await client.Browser.BrowseLevel(
                new BrowseParams
                {
                    BrowseDirection = BrowseDirection.Inverse,
                    IncludeSubTypes = true,
                    ReferenceTypeId = ReferenceTypeIds.HasDataSetWriter,
                    Nodes = toCorrect.ToDictionary(node => node.NodeId, node => new BrowseNode(node.NodeId)),
                },
                token, purpose: "data set writers");

            foreach (var kvp in result)
            {
                foreach (var node in kvp.Value)
                {
                    var writer = nodeMap[kvp.Key];
                    writer.AltParent = writer.Parent;
                    var id = client.ToNodeId(node.NodeId);
                    writer.Parent = nodeMap[id];
                    writer.Parent.AddChild(writer);
                    writer.ReferenceType = node.ReferenceTypeId;
                }
            }
        }

        private static long ToNumeric(InternalNode? value)
        {
            if (value?.Value?.Value == null) return 0;
            try
            {
                return Convert.ToInt64(value.Value.Value);
            }
            catch
            {
                return 0;
            }
        }


        private void BuildDataSetMetadata()
        {
            var root = nodeMap[ObjectIds.PublishSubscribe_PublishedDataSets];
            foreach (var dataSet in root.AllChildren.Where(child => child.TypeDefinition == ObjectTypeIds.PublishedDataItemsType
                || child.TypeDefinition == ObjectTypeIds.PublishedDataSetType))
            {
                var publishedItems = (dataSet.Children.GetValueOrDefault("PublishedData")?.Value?.Value as ExtensionObject[])
                    ?.SelectNonNull(ext => ext.Body as PublishedVariableDataType)?.ToArray();
                var id = dataSet.Children.GetValueOrDefault("DataSetClassId")?.Value?.Value as Uuid?;

                if (publishedItems == null
                    || (dataSet.Children.GetValueOrDefault("DataSetMetaData")?.Value?.Value as ExtensionObject)
                    ?.Body is not DataSetMetaDataType metaData
                    || (dataSet.Children.GetValueOrDefault("ConfigurationVersion")?.Value?.Value as ExtensionObject)
                    ?.Body is not ConfigurationVersionDataType version) continue;

                var subscribedDataSet = new TargetVariablesDataType
                {
                    TargetVariables = new FieldTargetDataTypeCollection()
                };

                if (metaData.Fields.Count != publishedItems.Length)
                {
                    log.LogError("Incorrect number of fields in data set {BrowseName}: {FieldCount} fields {ItemCount} items",
                        dataSet.BrowseName, metaData.Fields.Count, publishedItems.Length);
                    continue;
                }

                for (int i = 0; i < metaData.Fields.Count; i++)
                {
                    var field = metaData.Fields[i];
                    var item = publishedItems[i];
                    subscribedDataSet.TargetVariables.Add(new FieldTargetDataType
                    {
                        AttributeId = item.AttributeId,
                        DataSetFieldId = field.DataSetFieldId,
                        OverrideValue = new Variant(TypeInfo.GetDefaultValue(field.DataType, field.ValueRank)),
                        OverrideValueHandling = OverrideValueHandling.OverrideValue,
                        TargetNodeId = item.PublishedVariable
                    });
                }

                // Select optimal reader based on configuration
                ReaderWrapper? finalReader = null;
                foreach (var writer in dataSet.AllChildren.Where(child => child.TypeDefinition == ObjectTypeIds.DataSetWriterType))
                {
                    if (!readers.TryGetValue(writer.NodeId, out var reader)) continue;

                    // Either this is the first, it is using a UADP and the previous is using JSON,
                    // or they are using the same profile, but this reader belongs to a connection with more
                    // readers. (We try to keep as few connections as possible while covering all the data).
                    if (finalReader != null)
                    {
                        var oldProfile = finalReader.Connection.Connection.TransportProfileUri;
                        var profile = reader.Connection.Connection.TransportProfileUri;

                        if (pubSubConfig.PreferUadp)
                        {
                            if (profile == Profiles.PubSubMqttJsonTransport
                                && oldProfile == Profiles.PubSubMqttUadpTransport) continue;
                        }
                        else
                        {
                            if (profile == Profiles.PubSubMqttUadpTransport
                                && oldProfile == Profiles.PubSubMqttJsonTransport) continue;
                        }

                        if (profile == oldProfile
                            && reader.Connection.FinalReaders.Count <= finalReader.Connection.FinalReaders.Count) continue;
                    }

                    finalReader = reader;
                }

                if (finalReader != null)
                {
                    finalReader.Targets = subscribedDataSet;
                    finalReader.Reader.DataSetMetaData = metaData;
                    finalReader.Reader.SubscribedDataSet = new ExtensionObject(subscribedDataSet);
                    if (subscribedDataSet.TargetVariables?.Any(v => v.AttributeId == Attributes.Value) ?? false)
                    {
                        finalReader.Connection.FinalReaders.Add(finalReader);
                    }
                }
            }

        }

        private void BuildConnections()
        {
            var root = nodeMap[ObjectIds.PublishSubscribe];
            config.Connections = new PubSubConnectionDataTypeCollection();

            var connections = root.AllChildren.Where(child => child.TypeDefinition == ObjectTypeIds.PubSubConnectionType);
            foreach (var conn in connections)
            {
                var cConn = new PubSubConnectionDataType
                {
                    Name = conn.BrowseName,
                    Enabled = true,
                    TransportProfileUri = conn.Children.GetValueOrDefault("TransportProfileUri")?.Value?.Value as string
                };
                if (cConn.TransportProfileUri != Profiles.PubSubMqttJsonTransport
                    && cConn.TransportProfileUri != Profiles.PubSubMqttUadpTransport) continue;

                var wrapper = new ConnectionWrapper(cConn);
                this.connections[conn.NodeId] = wrapper;

                if (conn.Children.TryGetValue("Address", out var addr))
                {
                    var cAddr = new NetworkAddressUrlDataType
                    {
                        NetworkInterface = string.Empty,
                        Url = addr.Children.GetValueOrDefault("Url")?.Value?.Value as string
                    };
                    cConn.Address = new ExtensionObject(cAddr);
                }
                if (conn.Children.TryGetValue("ConnectionProperties", out var cProps))
                {
                    if (cProps.Value?.Value is Opc.Ua.KeyValuePair[] cPropsArr)
                    {
                        cConn.ConnectionProperties = new KeyValuePairCollection(cPropsArr);
                    }
                }
                cConn.PublisherId = conn.Children.GetValueOrDefault("PublisherId")?.Value?.WrappedValue ?? Variant.Null;
                cConn.ReaderGroups = new ReaderGroupDataTypeCollection();


                foreach (var group in conn.AllChildren.Where(child => child.ReferenceType == ReferenceTypeIds.HasWriterGroup))
                {
                    var cGroup = new ReaderGroupDataType
                    {
                        Name = group.BrowseName,
                        Enabled = true,
                        MaxNetworkMessageSize = group.Children.TryGetValue("MaxNetworkMessageSize", out var mSize)
                        && mSize.Value?.Value is uint mSizeVal ? mSizeVal : 0,
                        MessageSettings = new ExtensionObject(new ReaderGroupMessageDataType()),
                        TransportSettings = new ExtensionObject(new ReaderGroupTransportDataType())
                    };

                    var groupId = (ushort)ToNumeric(group.Children.GetValueOrDefault("WriterGroupId"));

                    uint networkContentMask = 0;
                    uint groupVersion = 0;
                    if (group.Children.TryGetValue("MessageSettings", out var gMessageSettings))
                    {
                        networkContentMask =
                            (uint)ToNumeric(gMessageSettings.Children.GetValueOrDefault("NetworkMessageContentMask"));
                        groupVersion =
                            (uint)ToNumeric(gMessageSettings.Children.GetValueOrDefault("GroupVersion"));
                    }

                    cGroup.DataSetReaders = new DataSetReaderDataTypeCollection();

                    foreach (var writer in group.AllChildren.Where(child =>
                        child.ReferenceType == ReferenceTypeIds.HasDataSetWriter))
                    {
                        var cReader = new DataSetReaderDataType
                        {
                            PublisherId = cConn.PublisherId,
                            WriterGroupId = 0,
                            DataSetWriterId = (ushort)ToNumeric(writer.Children.GetValueOrDefault("DataSetWriterId"))
                        };

                        if (writer.Children.TryGetValue("MessageSettings", out var wMessageSettings))
                        {
                            uint messageContentMask =
                                (uint)ToNumeric(wMessageSettings.Children.GetValueOrDefault("DataSetMessageContentMask"));
                            if (cConn.TransportProfileUri == Profiles.PubSubMqttJsonTransport)
                            {
                                cReader.MessageSettings = new ExtensionObject(new JsonDataSetReaderMessageDataType
                                {
                                    DataSetMessageContentMask = messageContentMask,
                                    NetworkMessageContentMask = networkContentMask
                                });
                            }
                            else
                            {
                                ushort networkMessageNumber =
                                    (ushort)ToNumeric(wMessageSettings.Children.GetValueOrDefault("NetworkMessageNumber"));
                                cReader.MessageSettings = new ExtensionObject(new UadpDataSetReaderMessageDataType
                                {
                                    DataSetMessageContentMask = messageContentMask,
                                    NetworkMessageContentMask = networkContentMask,
                                    NetworkMessageNumber = 0,
                                    GroupVersion = 0
                                });
                            }
                        }

                        var wTransportSettings = writer.Children["TransportSettings"];
                        var tSettings = new BrokerDataSetReaderTransportDataType
                        {
                            QueueName = wTransportSettings.Children.GetValueOrDefault("QueueName")?.Value?.Value as string,
                            MetaDataQueueName = wTransportSettings.Children.GetValueOrDefault("MetaDataQueueName")
                            ?.Value?.Value as string,
                            RequestedDeliveryGuarantee = BrokerTransportQualityOfService.AtLeastOnce
                        };

                        cReader.TransportSettings = new ExtensionObject(tSettings);
                        cReader.DataSetFieldContentMask =
                            (uint)ToNumeric(writer.Children.GetValueOrDefault("DataSetFieldContentMask"));
                        cReader.KeyFrameCount =
                            (uint)ToNumeric(writer.Children.GetValueOrDefault("KeyFrameCount"));

                        cReader.Enabled = true;

                        cGroup.DataSetReaders.Add(cReader);
                        readers[writer.NodeId] = new ReaderWrapper(cReader, writer, wrapper);
                    }

                    cConn.ReaderGroups.Add(cGroup);
                }
            }
        }

        public async Task<PubSubConfigurationDataType?> Build(CancellationToken token)
        {
            config = new PubSubConfigurationDataType();

            var root = await client.Browser.GetRootNodes(new[] { ObjectIds.PublishSubscribe }, token);
            HandleNode(root.First(), NodeId.Null, false);

            log.LogInformation("Browse server PubSub hierarchy to identify settings");
            await client.Browser.BrowseDirectory(
                new[] { ObjectIds.PublishSubscribe },
                HandleNode,
                token,
                doFilter: false,
                purpose: "identifying PubSub settings");

            // Read values
            if (!await LoadNodeValues(token)) return null;
            await CorrectWriterParents(token);
            BuildConnections();
            BuildDataSetMetadata();

            // Add useful connections to the config.
            foreach (var conn in connections.Values)
            {
                if (!conn.FinalReaders.Any()) continue;
                config.Connections.Add(conn.Connection);
            }

            return config;
        }

        private void HandleNode(ReferenceDescription node, NodeId parentId, bool visited)
        {
            if (visited) return;
            var parent = parentId == null || parentId.IsNullNodeId ? null : nodeMap.GetValueOrDefault(parentId);
            var id = client.ToNodeId(node.NodeId);
            var child = new InternalNode(
                node.NodeClass,
                node.BrowseName.Name,
                id,
                client.ToNodeId(node.TypeDefinition),
                node.ReferenceTypeId,
                parent);

            nodeMap[id] = child;
        }
    }

    internal class InternalNode
    {
        public NodeClass NodeClass { get; }
        public string BrowseName { get; }
        public InternalNode? Parent { get; set; }
        public InternalNode? AltParent { get; set; }
        public Dictionary<string, InternalNode> Children = new Dictionary<string, InternalNode>();
        private readonly Dictionary<NodeId, InternalNode> childrenById = new Dictionary<NodeId, InternalNode>();
        public IEnumerable<InternalNode> AllChildren => childrenById.Values;
        public DataValue? Value { get; set; }
        public NodeId NodeId { get; }
        public NodeId TypeDefinition { get; }
        public NodeId ReferenceType { get; set; }

        public InternalNode(
            NodeClass nc,
            string name,
            NodeId id,
            NodeId typeDefinitionId,
            NodeId referenceTypeId,
            InternalNode? parent)
        {
            NodeClass = nc;
            BrowseName = name;
            Parent = parent;
            NodeId = id;
            TypeDefinition = typeDefinitionId;
            ReferenceType = referenceTypeId;
            if (parent != null) parent.AddChild(this);
        }

        public void AddChild(InternalNode node)
        {
            Children[node.BrowseName] = node;
            childrenById[node.NodeId] = node;
        }
    }
}
