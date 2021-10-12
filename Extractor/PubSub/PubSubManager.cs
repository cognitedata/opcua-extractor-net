﻿using Opc.Ua;
using Opc.Ua.PubSub;
using Opc.Ua.PubSub.Encoding;
using Serilog;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.PubSub
{
    public sealed class PubSubManager : IDisposable
    {
        private readonly ServerPubSubConfigurator configurator;
        private UaPubSubApplication? app;
        private readonly ILogger log = Log.Logger.ForContext(typeof(PubSubManager));
        private readonly PubSubConfig config;
        private readonly UAExtractor extractor;
        public PubSubManager(UAClient client, UAExtractor extractor, PubSubConfig config)
        {
            this.config = config;
            configurator = new ServerPubSubConfigurator(client, config);
            this.extractor = extractor;
        }

        public async Task Start(CancellationToken token)
        {
            if (app != null)
            {
                app.Dispose();
            }

            var config = await configurator.Build(token);
            if (config == null)
            {
                log.Error("Configuring subscriber failed");
                return;
            }

            app = UaPubSubApplication.Create(config);

            log.Information("Starting pubsub server with {cnt} connections", config.Connections.Count);

            foreach (var conn in config.Connections)
            {
                log.Debug("Connection: {name}, with {cnt} groups", conn.Name, conn.ReaderGroups.Count);
                log.Debug("Profile: {name}", conn.TransportProfileUri);
                log.Debug("Address: {addr}", (conn.Address.Body as NetworkAddressUrlDataType)?.Url);
                foreach (var group in conn.ReaderGroups)
                {
                    log.Debug("    Group: {name}, with {cnt} readers", group.Name, group.DataSetReaders.Count);
                    foreach (var reader in group.DataSetReaders)
                    {
                        log.Debug("        Reader: {name}, with {cnt} targets", reader.Name, (reader.SubscribedDataSet?.Body
                            as TargetVariablesDataType)?.TargetVariables.Count);
                        log.Debug("        Queue: {name}", (reader.TransportSettings.Body as BrokerDataSetReaderTransportDataType)
                            ?.QueueName);
                        log.Debug("        Writer: {group}:{name}", reader.WriterGroupId, reader.DataSetWriterId);
                    }
                }
            }

            app.DataReceived += DataReceived;
            app.Start();
        }

        private void DataReceived(object sender, SubscribedDataEventArgs e)
        {
            if (e.NetworkMessage is UadpNetworkMessage uadpMessage)
            {
                log.Verbose("UADP Network DataSetMessage ({0} DataSets): Source={1}, SequenceNumber={2}",
                        e.NetworkMessage.DataSetMessages.Count, e.Source, uadpMessage.SequenceNumber);
            }
            else if (e.NetworkMessage is JsonNetworkMessage jsonMessage)
            {
                log.Verbose("JSON Network DataSetMessage ({0} DataSets): Source={1}, MessageId={2}",
                        e.NetworkMessage.DataSetMessages.Count, e.Source, jsonMessage.MessageId);
            }

            foreach (var dataSetMessage in e.NetworkMessage.DataSetMessages)
            {
                var dataSet = dataSetMessage.DataSet;
                log.Verbose("\tDataSet.Name={0}, DataSetWriterId={1}, SequenceNumber={2}", dataSet.Name,
                    dataSet.DataSetWriterId, dataSetMessage.SequenceNumber);

                for (int i = 0; i < dataSet.Fields.Length; i++)
                {
                    var field = dataSet.Fields[i];
                    log.Verbose("\t\tTargetNodeId:{0}, Attribute:{1}, Value:{2}, TS:{3}",
                        field.TargetNodeId, field.TargetAttribute, field.Value, field.Value.SourceTimestamp);

                    if (field.TargetAttribute != Attributes.Value) continue;

                    var variable = extractor.State.GetNodeState(field.TargetNodeId);
                    if (variable == null)
                    {
                        log.Verbose("Missing state for pub-sub node: {id}", field.TargetNodeId);
                        continue;
                    }

                    extractor.Streamer.HandleStreamedDatapoint(field.Value, variable);
                }
            }
        }

        public void Dispose()
        {
            app?.Dispose();
            app = null;
        }
    }
}
