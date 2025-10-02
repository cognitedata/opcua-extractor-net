using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
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
        private readonly Dictionary<string, NamespacePublicationDateState> _extractionStates = new();

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
            using var pusher = new DummyPusher(new DummyPusherConfig());
            tester.Config.Extraction.RebrowseTriggers = config;
            await using var extractor = tester.BuildExtractor(pusher);

            // Act
            var runTask = tester.RunExtractor(extractor);
            await extractor.WaitForSubscription(SubscriptionName.RebrowseTriggers);

            // Assert
            Assert.True(tester.TryGetSubscription(SubscriptionName.RebrowseTriggers, out var _));

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
        }

        [Theory]
        [MemberData(nameof(NonTriggeringConfigurationStates))]
        public async Task TestRebrowseIsNotSubscribed(RebrowseTriggersConfig config)
        {
            // Arrange
            using var pusher = new DummyPusher(new DummyPusherConfig());
            tester.Config.Extraction.RebrowseTriggers = config;
            await using var extractor = tester.BuildExtractor(pusher, true);

            // Act
            var runTask = tester.RunExtractor(extractor);
            await extractor.WaitForSubscription(SubscriptionName.DataPoints);

            // Assert
            Assert.False(tester.TryGetSubscription(SubscriptionName.RebrowseTriggers, out var _));

            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
        }

        [Fact]
        public async Task TestRebrowseIsTriggered()
        {
            // Arrange
            using var cdfPusher = new DummyPusher(new DummyPusherConfig());
            tester.Config.Extraction.RebrowseTriggers = new RebrowseTriggersConfig
            {
                Targets = new RebrowseTriggerTargets { NamespacePublicationDate = true }
            };
            tester.Config.StateStorage = new StateStorageConfig
            {
                Database = StateStoreConfig.StorageType.LiteDb,
                Location = "lite-state-rebrowse.db",
            };
            using var stateStore = new LiteDBStateStore(
                tester.Config.StateStorage,
                tester.Provider.GetRequiredService<ILogger<LiteDBStateStore>>()
            );
            await using var extractor = tester.BuildExtractor(cdfPusher, true, stateStore);
            var npdId = tester.Client.GetUniqueId(tester.Server.Server.GetNamespacePublicationDateId());
            var npds = new NamespacePublicationDateState(npdId);
            var lts = DateTime.UtcNow.AddSeconds(-10);
            var simulatedLastTimestamp = lts.ToUnixTimeMilliseconds();
            npds.LastTimestamp = simulatedLastTimestamp;
            npds.LastTimeModified = DateTime.UtcNow;
            _extractionStates.TryAdd(npdId, npds);
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
            var runTask = tester.RunExtractor(extractor);
            await extractor.WaitForBrowseCompletion();
            await extractor.WaitForSubscription(SubscriptionName.RebrowseTriggers);
            var initialCount = cdfPusher.PushedNodes.Count;
            var addedId = tester.Server.Server.AddObject(
                tester.Ids.Audit.Root,
                "NodeToAddForRebrowse"
            );

            // Act
            var newTime = DateTime.UtcNow;
            _output.WriteLine($"New time set to {newTime.ToUnixTimeMilliseconds()}");
            tester.Server.Server.SetNamespacePublicationDate(newTime);

            // Assert
            await TestUtils.WaitForCondition(
                () => cdfPusher.PushedNodes.ContainsKey(addedId),
                10,
                "Expected node to be discovered"
            );

            await extractor.StoreState(tester.Source.Token);
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
            foreach (var id in _extractionStates)
            {
                _output.WriteLine($"Value of {id.Key} is {id.Value.LastTimestamp}");
            }
            Assert.True(_extractionStates.TryGetValue(npdId, out var newNpds));
            _output.WriteLine($"Test response {newTime.ToUnixTimeMilliseconds()}: {newNpds.LastTimestamp}");
            // Assert.True(false);
            Assert.Equal(newTime.ToUnixTimeMilliseconds(), newNpds.LastTimestamp);
            tester.Server.Server.RemoveNode(addedId);
            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
            try
            {
                File.Delete(tester.Config.StateStorage.Location);
            }
            catch { }
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
