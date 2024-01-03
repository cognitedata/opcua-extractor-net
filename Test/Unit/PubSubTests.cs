using Cognite.Extractor.Common;
using Cognite.Extractor.Testing;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.PubSub;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Server;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public sealed class PubSubTestFixture : BaseExtractorTestFixture
    {
        public PubSubTestFixture() : base(new[] { PredefinedSetup.Base, PredefinedSetup.Custom, PredefinedSetup.PubSub }) { }
    }
    public class PubSubTests : IClassFixture<PubSubTestFixture>
    {
        private readonly PubSubTestFixture tester;
        public PubSubTests(ITestOutputHelper output, PubSubTestFixture tester)
        {
            ArgumentNullException.ThrowIfNull(tester);
            tester.Init(output);
            this.tester = tester;
            tester.Server.UpdateNode(tester.Ids.Base.DoubleVar1, 17);
            tester.Server.UpdateNode(tester.Ids.Base.DoubleVar2, -15);
            tester.Server.UpdateNode(tester.Ids.Base.BoolVar, 14 % 2 == 0);
            tester.Server.UpdateNode(tester.Ids.Base.IntVar, 12);
            tester.Server.UpdateNode(tester.Ids.Base.StringVar, $"Idx: {10}");

            tester.Server.UpdateNode(tester.Ids.Custom.Array, new double[] { 1, 2, 3, 4 });
            tester.Server.UpdateNode(tester.Ids.Custom.StringArray, new string[] { $"str{7}", $"str{-8}" });
            tester.Server.UpdateNode(tester.Ids.Custom.StringyVar, $"Idx: {3}");
            tester.Server.UpdateNode(tester.Ids.Custom.MysteryVar, 6);
            tester.Server.UpdateNode(tester.Ids.Custom.IgnoreVar, $"Idx: {2}");
            tester.Server.UpdateNode(tester.Ids.Custom.NumberVar, 5);
            tester.Server.UpdateNode(tester.Ids.Custom.EnumVar1, 1);
            tester.Server.UpdateNode(tester.Ids.Custom.EnumVar2, 123);
            tester.Server.UpdateNode(tester.Ids.Custom.EnumVar3, new[] { 123, 123, 123, 321 });
            tester.ResetConfig();
            tester.Client.TypeManager.Reset();
        }

        [Theory(Timeout = 20000)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestPubSubConfiguration(bool uadp)
        {
            var config = new PubSubConfig
            {
                Enabled = true,
                PreferUadp = uadp
            };

            var logger = tester.Provider.GetRequiredService<ILogger<PubSubTests>>();

            var configurator = new ServerPubSubConfigurator(logger, tester.Client, config);
            var built = await configurator.Build(tester.Source.Token);
            Assert.NotNull(built);

            Assert.Single(built.Connections);
            var conn = built.Connections.Single();

            Assert.Equal(uadp ? Profiles.PubSubMqttUadpTransport : Profiles.PubSubMqttJsonTransport, conn.TransportProfileUri);
            var addr = conn.Address.Body as NetworkAddressUrlDataType;
            Assert.Equal("mqtt://localhost:4060", addr.Url);

            Assert.Single(conn.ReaderGroups);
            var group = conn.ReaderGroups.Single();

            string type = uadp ? "UADP" : "JSON";
            Assert.Equal($"Writer group 1 {type}", group.Name);
            Assert.Equal(2, group.DataSetReaders.Count);
            var reader = group.DataSetReaders.First();
            string queueName = uadp ? "ua-test-publish" : "ua-test-publish-json";
            var dataSet = (reader.SubscribedDataSet?.Body as TargetVariablesDataType)?.TargetVariables;
            Assert.Equal(5, dataSet.Count);
            Assert.Equal(queueName, (reader.TransportSettings.Body as BrokerDataSetReaderTransportDataType)?.QueueName);
            foreach (var vb in dataSet)
            {
                Assert.NotNull(vb.TargetNodeId);
                Assert.Equal(Attributes.Value, vb.AttributeId);
            }

            reader = group.DataSetReaders.Last();
            dataSet = (reader.SubscribedDataSet?.Body as TargetVariablesDataType)?.TargetVariables;
            Assert.Equal(9, dataSet.Count);
            Assert.Equal(queueName, (reader.TransportSettings.Body as BrokerDataSetReaderTransportDataType)?.QueueName);
            foreach (var vb in dataSet)
            {
                Assert.NotNull(vb.TargetNodeId);
                Assert.Equal(Attributes.Value, vb.AttributeId);
            }
        }
        [Theory(Timeout = 20000)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestPubSubData(bool uadp)
        {
            tester.Config.History.Enabled = false;
            tester.Config.Subscriptions.DataPoints = false;
            tester.Config.Extraction.DataTypes.AutoIdentifyTypes = true;
            tester.Config.Extraction.DataTypes.AllowStringVariables = true;
            tester.Config.Extraction.DataTypes.MaxArraySize = 4;
            tester.Config.PubSub.Enabled = true;
            tester.Config.PubSub.PreferUadp = uadp;
            using var extractor = tester.BuildExtractor();

            await extractor.RunExtractor(true);

            var queue = (Queue<UADataPoint>)extractor.Streamer.GetType()
                .GetField("dataPointQueue", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(extractor.Streamer);

            int dpsPerBatch = 21;

            // At least two batches
            await TestUtils.WaitForCondition(() => queue.Count > dpsPerBatch, 20);

            var dps = queue.ToArray();

            Assert.Equal(dpsPerBatch, dps.DistinctBy(dp => dp.Id).Count());
        }
    }
}
