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

using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.PubSub;
using Opc.Ua.PubSub.Encoding;
using System;
using System.IO;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;

namespace Cognite.OpcUa.PubSub
{
    public sealed class PubSubManager : IDisposable
    {
        private readonly ServerPubSubConfigurator configurator;
        private UaPubSubApplication? app;
        private readonly ILogger<PubSubManager> log;
        private readonly PubSubConfig config;
        private readonly UAExtractor extractor;
        public PubSubManager(ILogger<PubSubManager> logger, UAClient client, UAExtractor extractor, PubSubConfig config)
        {
            this.config = config;
            configurator = new ServerPubSubConfigurator(logger, client, config);
            this.extractor = extractor;
            log = logger;
        }

        public async Task Start(CancellationToken token)
        {
            if (app != null)
            {
                app.Dispose();
                app = null;
            }

            var config = await GetConfig(token);
            if (config == null)
            {
                log.LogError("Configuring subscriber failed");
                return;
            }

            app = UaPubSubApplication.Create(config);

            log.LogInformation("Starting pubsub server with {Count} connections", config.Connections.Count);

            foreach (var conn in config.Connections)
            {
                log.LogDebug("Connection: {Name}, with {Count} groups", conn.Name, conn.ReaderGroups.Count);
                log.LogDebug("Profile: {Name}", conn.TransportProfileUri);
                log.LogDebug("Address: {Address}", (conn.Address.Body as NetworkAddressUrlDataType)?.Url);
                foreach (var group in conn.ReaderGroups)
                {
                    log.LogDebug("    Group: {Name}, with {Count} readers", group.Name, group.DataSetReaders.Count);
                    foreach (var reader in group.DataSetReaders)
                    {
                        log.LogDebug("        Reader: {Name}, with {Count} targets", reader.Name, (reader.SubscribedDataSet?.Body
                            as TargetVariablesDataType)?.TargetVariables.Count);
                        log.LogDebug("        Queue: {Name}", (reader.TransportSettings.Body as BrokerDataSetReaderTransportDataType)
                            ?.QueueName);
                        log.LogDebug("        Writer: {Group}:{Name}", reader.WriterGroupId, reader.DataSetWriterId);
                    }
                }
            }

            app.DataReceived += DataReceived;
            await Task.Run(() => app.Start(), token);
        }

        public void Stop()
        {
            if (app != null)
            {
                app.Stop();
            }
        }

        private async Task<PubSubConfigurationDataType?> GetConfig(CancellationToken token)
        {
            var config = LoadConfig();
            if (config == null)
            {
                config = await configurator.Build(token);
                if (config != null) SaveConfig(config);
            }
            return config;
        }

        private PubSubConfigurationDataType? LoadConfig()
        {
            if (string.IsNullOrWhiteSpace(config.FileName)) return null;
            if (!File.Exists(config.FileName)) return null;
            try
            {
                using (var stream = new FileStream(config.FileName, FileMode.Open, FileAccess.Read))
                {
                    var s = new DataContractSerializer(typeof(PubSubConfigurationDataType));
                    return (PubSubConfigurationDataType)s.ReadObject(stream);
                }
            }
            catch (Exception ex)
            {
                log.LogError("Failed to deserialize pubsub config file: {Message}", ex.Message);
                return null;
            }
        }

        private void SaveConfig(PubSubConfigurationDataType config)
        {
            if (string.IsNullOrWhiteSpace(this.config.FileName)) return;
            log.LogInformation("Saving PubSub configuration to {Name}", this.config.FileName);
            using (var stream = new FileStream(this.config.FileName, FileMode.Create, FileAccess.Write))
            {
                var s = new DataContractSerializer(typeof(PubSubConfigurationDataType));
                var settings = new XmlWriterSettings { Indent = true };

                using (var w = XmlWriter.Create(stream, settings)) s.WriteObject(w, config);
            }
        }

        private void DataReceived(object sender, SubscribedDataEventArgs e)
        {
            if (e.NetworkMessage is UadpNetworkMessage uadpMessage)
            {
                log.LogTrace("UADP Network DataSetMessage ({DataSets} DataSets): Source={Source}, SequenceNumber={SequenceNumber}",
                        e.NetworkMessage.DataSetMessages.Count, e.Source, uadpMessage.SequenceNumber);
            }
            else if (e.NetworkMessage is JsonNetworkMessage jsonMessage)
            {
                log.LogTrace("JSON Network DataSetMessage ({DataSets} DataSets): Source={Source}, MessageId={MessageId}",
                        e.NetworkMessage.DataSetMessages.Count, e.Source, jsonMessage.MessageId);
            }

            foreach (var dataSetMessage in e.NetworkMessage.DataSetMessages)
            {
                var dataSet = dataSetMessage.DataSet;
                log.LogTrace("\tDataSet.Name={DataSetName}, DataSetWriterId={DataSetWriterId}, SequenceNumber={SequenceNumber}", dataSet.Name,
                    dataSet.DataSetWriterId, dataSetMessage.SequenceNumber);

                for (int i = 0; i < dataSet.Fields.Length; i++)
                {
                    var field = dataSet.Fields[i];
                    log.LogTrace("\t\tTargetNodeId:{TargetNodeId}, Attribute:{Attribute}, Value:{Value}, TS:{TimeStamp}",
                        field.TargetNodeId, field.TargetAttribute, field.Value, field.Value.SourceTimestamp);

                    if (field.TargetAttribute != Attributes.Value) continue;

                    var variable = extractor.State.GetNodeState(field.TargetNodeId);
                    if (variable == null)
                    {
                        log.LogTrace("\t\tMissing state for pub-sub node: {Id}", field.TargetNodeId);
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
