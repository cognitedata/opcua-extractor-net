using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Testing;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Subscriptions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Integration
{
    [Collection("Shared server tests")]
    public class RebrowseTriggerManagerTests
    {
        private readonly StaticServerTestFixture tester;
        private readonly ITestOutputHelper _output;
        private IDictionary<string, NamespacePublicationDateState> _extractionStates =
            new Dictionary<string, NamespacePublicationDateState>();

        public RebrowseTriggerManagerTests(ITestOutputHelper output, StaticServerTestFixture tester)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
            tester.ResetConfig();
            tester.Init(output);
            tester.Client.TypeManager.Reset();
            _output = output;
        }

        [Theory]
        [MemberData(nameof(TriggeringConfigurationStates))]
        public async Task TestRebrowseIsSubscribed(RebrowseTriggersConfig config)
        {
            // Arrange
            var pusher = new DummyPusher(new DummyPusherConfig());
            tester.Config.Extraction.RebrowseTriggers = config;
            using var extractor = tester.BuildExtractor(pushers: pusher);

            // Act
            var runTask = extractor.RunExtractor();
            await extractor.WaitForSubscriptions();

            // Assert
            Assert.True(tester.TryGetSubscription(SubscriptionName.RebrowseTriggers, out var _));

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
        }

        [Theory]
        [MemberData(nameof(NonTriggeringConfigurationStates))]
        public async Task TestRebrowseIsNotSubscribed(RebrowseTriggersConfig config)
        {
            // Arrange
            var pusher = new DummyPusher(new DummyPusherConfig());
            tester.Config.Extraction.RebrowseTriggers = config;
            using var extractor = tester.BuildExtractor(true, pushers: pusher);

            // Act
            var runTask = extractor.RunExtractor();
            await extractor.WaitForSubscriptions();

            // Assert
            Assert.False(tester.TryGetSubscription(SubscriptionName.RebrowseTriggers, out var _));

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
        }

        [Fact]
        public async Task TestRebrowseIsTriggered()
        {
            // Arrange
            var cdfPusher = new DummyPusher(new DummyPusherConfig());
            tester.Config.Extraction.RebrowseTriggers = new RebrowseTriggersConfig
            {
                Targets = new RebrowseTriggerTargets { NamespacePublicationDate = true }
            };
            tester.Config.StateStorage = new StateStorageConfig
            {
                Database = StateStoreConfig.StorageType.LiteDb,
                Location = "lite-state-rebrowse.db",
                InfluxVariableStore = "namespace_publication_dates"
            };
            using var stateStore = new LiteDBStateStore(
                tester.Config.StateStorage,
                tester.Provider.GetRequiredService<ILogger<LiteDBStateStore>>()
            );
            using var extractor = tester.BuildExtractor(true, stateStore, cdfPusher);
            var npdId = tester.Server.Server.GetNamespacePublicationDateId();
            var npds = new NamespacePublicationDateState(npdId.ToString());
            var simulatedLastTimestamp = ((DateTimeOffset)DateTime.UtcNow).ToUnixTimeSeconds() - 10;
            npds.LastTimestamp = simulatedLastTimestamp;
            npds.LastTimeModified = DateTime.UtcNow;
            _extractionStates.TryAdd(npdId.ToString(), npds);
            var runTask = extractor.RunExtractor();
            await stateStore.StoreExtractionState<
                NamespacePublicationDateStorableState,
                NamespacePublicationDateState
            >(
                _extractionStates.Values.ToList(),
                tester.Config.StateStorage.NamespacePublicationDateStore,
                (state) =>
                    new NamespacePublicationDateStorableState
                    {
                        Id = state.Id,
                        CreatedAt = DateTime.UtcNow,
                        LastTimestamp = npds.LastTimestamp,
                    },
                tester.Source.Token
            );
            await extractor.WaitForSubscriptions();
            var initialCount = cdfPusher.PushedNodes.Count;
            var addedId = tester.Server.Server.AddObject(
                tester.Ids.Audit.Root,
                "NodeToAddForRebrowse"
            );

            // Act
            tester.Server.Server.SetNamespacePublicationDate(DateTime.UtcNow);

            // Assert
            await TestUtils.WaitForCondition(
                () => cdfPusher.PushedNodes.ContainsKey(addedId),
                10,
                "Expected node to be discovered"
            );
            Assert.True(cdfPusher.PushedNodes.ContainsKey(addedId));

            await stateStore.RestoreExtractionState<
                NamespacePublicationDateStorableState,
                NamespacePublicationDateState
            >(
                _extractionStates,
                tester.Config.StateStorage.NamespacePublicationDateStore,
                (value, item) =>
                {
                    value.LastTimestamp = item.LastTimestamp;
                },
                tester.Source.Token
            );
            Assert.True(_extractionStates.TryGetValue(npdId.ToString(), out var newNpds));
            Assert.True(simulatedLastTimestamp < newNpds.LastTimestamp);
            tester.Server.Server.RemoveNode(addedId);
            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
        }

        public static IEnumerable<object[]> TriggeringConfigurationStates =>
            new List<object[]>
            {
                new object[]
                {
                    new RebrowseTriggersConfig
                    {
                        Targets = new RebrowseTriggerTargets { NamespacePublicationDate = true }
                    }
                },
                // Should trigger if an existing namespace uri is specified
                new object[]
                {
                    new RebrowseTriggersConfig
                    {
                        Targets = new RebrowseTriggerTargets { NamespacePublicationDate = true },
                        Namespaces = new List<string> { "opc.tcp://test.localhost" },
                    }
                },
                // Should trigger if at least one existing namespace uri is specified
                new object[]
                {
                    new RebrowseTriggersConfig
                    {
                        Targets = new RebrowseTriggerTargets { NamespacePublicationDate = true },
                        Namespaces = new List<string>
                        {
                            "opc.tcp://test.localhost",
                            "unknown://uri"
                        },
                    }
                },
            };

        public static IEnumerable<object[]> NonTriggeringConfigurationStates =>
            new List<object[]>
            {
                new object[] { new RebrowseTriggersConfig() },
                new object[] { null },
                new object[]
                {
                    new RebrowseTriggersConfig
                    {
                        Targets = new RebrowseTriggerTargets { NamespacePublicationDate = false }
                    }
                },
                new object[]
                {
                    new RebrowseTriggersConfig
                    {
                        Targets = new RebrowseTriggerTargets { NamespacePublicationDate = false },
                        Namespaces = new List<string> { "unknown://uri" }
                    }
                },
                new object[]
                {
                    new RebrowseTriggersConfig
                    {
                        Targets = new RebrowseTriggerTargets { NamespacePublicationDate = true },
                        Namespaces = new List<string> { "unknown://uri" }
                    }
                },
            };
    }
}
