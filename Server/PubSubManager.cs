using Opc.Ua;
using Opc.Ua.PubSub;
using Opc.Ua.PubSub.Transport;
using Serilog;
using System;

namespace Server
{
    public sealed class PubSubManager : IDisposable
    {
        private bool started;
        private readonly PubSubConfigurationDataType config;
        private readonly DataSetWriterDataType[] writers = new DataSetWriterDataType[4];
        private readonly PublishedDataSetDataType[] dataSets = new PublishedDataSetDataType[2];
        private readonly PublishedDataItemsDataType[] items = new PublishedDataItemsDataType[2];

        private UaPubSubApplication app;

        public PubSubManager(string mqttUrl)
        {
            config = new PubSubConfigurationDataType
            {
                Connections = new PubSubConnectionDataTypeCollection()
            };
            CreateUADPMQTTConnection(mqttUrl);
            CreateJSONMQTTConnection(mqttUrl);
            CreateDataSets();
        }

        private void CreateDataSets()
        {
            dataSets[0] = new PublishedDataSetDataType
            {
                Name = "Basic",
                DataSetMetaData = new DataSetMetaDataType()
                {
                    DataSetClassId = Uuid.Empty,
                    Name = "Basic",
                    Fields = new FieldMetaDataCollection(),
                    ConfigurationVersion = new ConfigurationVersionDataType
                    {
                        MajorVersion = 1,
                        MinorVersion = 0
                    }
                }
            };
            items[0] = new PublishedDataItemsDataType()
            {
                PublishedData = new PublishedVariableDataTypeCollection()
            };

            dataSets[1] = new PublishedDataSetDataType
            {
                Name = "Custom",
                DataSetMetaData = new DataSetMetaDataType()
                {
                    DataSetClassId = Uuid.Empty,
                    Name = "Custom",
                    Fields = new FieldMetaDataCollection(),
                    ConfigurationVersion = new ConfigurationVersionDataType
                    {
                        MajorVersion = 1,
                        MinorVersion = 0
                    }
                }
            };
            items[1] = new PublishedDataItemsDataType()
            {
                PublishedData = new PublishedVariableDataTypeCollection()
            };
        }

        private void CreateUADPMQTTConnection(string mqttUrl)
        {
            var conn = new PubSubConnectionDataType
            {
                Name = "Connection UADP",
                Enabled = true,
                PublisherId = 1,
                TransportProfileUri = Profiles.PubSubMqttUadpTransport,
                Address = new ExtensionObject(new NetworkAddressUrlDataType
                {
                    NetworkInterface = string.Empty,
                    Url = mqttUrl
                })
            };

            var props = new MqttClientProtocolConfiguration(version: EnumMqttProtocolVersion.V500);
            conn.ConnectionProperties = props.ConnectionProperties;

            // Define a writer group containing two writers, one for "Basic", one for "Custom"
            var msgSettings = new UadpWriterGroupMessageDataType
            {
                DataSetOrdering = DataSetOrderingType.AscendingWriterId,
                GroupVersion = 0,
                NetworkMessageContentMask = (uint)(UadpNetworkMessageContentMask.PublisherId
                        | UadpNetworkMessageContentMask.GroupHeader
                        | UadpNetworkMessageContentMask.WriterGroupId
                        | UadpNetworkMessageContentMask.PayloadHeader
                        | UadpNetworkMessageContentMask.GroupVersion
                        | UadpNetworkMessageContentMask.NetworkMessageNumber
                        | UadpNetworkMessageContentMask.SequenceNumber)
            };
            string topQueueName = "ua-test-publish";

            var writerGroup = new WriterGroupDataType
            {
                Name = "Writer group 1",
                Enabled = true,
                PublishingInterval = 500,
                KeepAliveTime = 5000,
                MaxNetworkMessageSize = 1500,
                HeaderLayoutUri = "UADP-Cyclic-Fixed",
                MessageSettings = new ExtensionObject(msgSettings),
                TransportSettings = new ExtensionObject(new BrokerWriterGroupTransportDataType
                {
                    QueueName = topQueueName
                })
            };

            var writerTransport = new BrokerDataSetWriterTransportDataType
            {
                QueueName = topQueueName
            };
            var writerMessage = new UadpDataSetWriterMessageDataType
            {
                DataSetMessageContentMask = (uint)(UadpDataSetMessageContentMask.Status | UadpDataSetMessageContentMask.SequenceNumber)
            };

            writers[0] = new DataSetWriterDataType
            {
                Name = "Basic Writer",
                DataSetWriterId = 1,
                Enabled = true,
                DataSetFieldContentMask = (uint)(DataSetFieldContentMask.RawData | DataSetFieldContentMask.SourceTimestamp),
                DataSetName = "Basic",
                KeyFrameCount = 1,
                MessageSettings = new ExtensionObject(writerMessage),
                TransportSettings = new ExtensionObject(writerTransport)
            };
            writerGroup.DataSetWriters.Add(writers[0]);

            writers[1] = new DataSetWriterDataType
            {
                Name = "Custom Writer",
                DataSetWriterId = 2,
                Enabled = true,
                DataSetFieldContentMask = (uint)(DataSetFieldContentMask.RawData | DataSetFieldContentMask.SourceTimestamp),
                DataSetName = "Custom",
                KeyFrameCount = 1,
                MessageSettings = new ExtensionObject(writerMessage),
                TransportSettings = new ExtensionObject(writerTransport)
            };
            writerGroup.DataSetWriters.Add(writers[1]);
            conn.WriterGroups.Add(writerGroup);

            config.Connections.Add(conn);
        }

        private void CreateJSONMQTTConnection(string mqttUrl)
        {
            var conn = new PubSubConnectionDataType
            {
                Name = "Connection JSON",
                Enabled = true,
                PublisherId = 2,
                TransportProfileUri = Profiles.PubSubMqttJsonTransport,
                Address = new ExtensionObject(new NetworkAddressUrlDataType
                {
                    NetworkInterface = string.Empty,
                    Url = mqttUrl
                })
            };

            var props = new MqttClientProtocolConfiguration(version: EnumMqttProtocolVersion.V500);
            conn.ConnectionProperties = props.ConnectionProperties;

            // Define a writer group containing two writers, one for "Basic", one for "Custom"
            var msgSettings = new JsonWriterGroupMessageDataType
            {
                NetworkMessageContentMask = (uint)(JsonNetworkMessageContentMask.NetworkMessageHeader
                       | JsonNetworkMessageContentMask.DataSetMessageHeader
                       | JsonNetworkMessageContentMask.PublisherId
                       | JsonNetworkMessageContentMask.DataSetClassId
                       | JsonNetworkMessageContentMask.ReplyTo)
            };
            string topQueueName = "ua-test-publish-json";

            var writerGroup = new WriterGroupDataType
            {
                Name = "Writer group 1",
                Enabled = true,
                PublishingInterval = 500,
                KeepAliveTime = 5000,
                MaxNetworkMessageSize = 1500,
                MessageSettings = new ExtensionObject(msgSettings),
                TransportSettings = new ExtensionObject(new BrokerWriterGroupTransportDataType
                {
                    QueueName = topQueueName
                })
            };

            var writerTransport = new BrokerDataSetWriterTransportDataType
            {
                QueueName = topQueueName
            };
            var writerMessage = new JsonDataSetWriterMessageDataType
            {
                DataSetMessageContentMask = (uint)(JsonDataSetMessageContentMask.Status
                    | JsonDataSetMessageContentMask.SequenceNumber)
            };

            writers[2] = new DataSetWriterDataType
            {
                Name = "Basic Writer",
                DataSetWriterId = 1,
                Enabled = true,
                DataSetFieldContentMask = (uint)(DataSetFieldContentMask.RawData | DataSetFieldContentMask.SourceTimestamp),
                DataSetName = "Basic",
                KeyFrameCount = 1,
                MessageSettings = new ExtensionObject(writerMessage),
                TransportSettings = new ExtensionObject(writerTransport)
            };
            writerGroup.DataSetWriters.Add(writers[2]);

            writers[3] = new DataSetWriterDataType
            {
                Name = "Custom Writer",
                DataSetWriterId = 2,
                Enabled = true,
                DataSetFieldContentMask = (uint)(DataSetFieldContentMask.None | DataSetFieldContentMask.SourceTimestamp),
                DataSetName = "Custom",
                KeyFrameCount = 1,
                MessageSettings = new ExtensionObject(writerMessage),
                TransportSettings = new ExtensionObject(writerTransport)
            };
            writerGroup.DataSetWriters.Add(writers[3]);
            conn.WriterGroups.Add(writerGroup);

            config.Connections.Add(conn);
        }

        public void AddPubSubVariable(BaseDataVariableState state, BuiltInType type, int dataSetIndex)
        {
            var meta = new FieldMetaData
            {
                Name = state.DisplayName.Text,
                DataSetFieldId = new Uuid(Guid.NewGuid()),
                BuiltInType = (byte)type,
                DataType = state.DataType,
                ValueRank = state.ValueRank,
                ArrayDimensions = state.ArrayDimensions != null ? new UInt32Collection(state.ArrayDimensions) : null
            };
            var variable = new PublishedVariableDataType
            {
                PublishedVariable = state.NodeId,
                AttributeId = Attributes.Value
            };

            dataSets[dataSetIndex].DataSetMetaData.Fields.Add(meta);
            items[dataSetIndex].PublishedData.Add(variable);
        }

        public PubSubConfigurationDataType Build()
        {
            dataSets[0].DataSetSource = new ExtensionObject(items[0]);
            dataSets[1].DataSetSource = new ExtensionObject(items[1]);

            config.PublishedDataSets = new PublishedDataSetDataTypeCollection(dataSets);

            return config;
        }

        public void Dispose()
        {
            app?.Dispose();
            app = null;
        }

        public void Start()
        {
            if (app != null)
            {
                app.Stop();
                app.Dispose();
            }
            app = UaPubSubApplication.Create(config);
            app.Start();
            Log.Information("Start pubsub application");
            started = true;
        }

        public void ReportDataChange(DataValue value, NodeId id)
        {
            if (!started) return;
            app.DataStore.WritePublishedDataItem(id, Attributes.Value, value);
        }
    }
}
