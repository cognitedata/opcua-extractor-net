using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cognite.Extractor.Testing;
using Cognite.OpcUa;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Test.Integration
{
    [Collection("Shared server tests")]
    public class RebrowseTriggerManagerTests
    {
        private readonly StaticServerTestFixture tester;

        public RebrowseTriggerManagerTests(ITestOutputHelper output, StaticServerTestFixture tester)
        {
            this.tester = tester ?? throw new ArgumentNullException(nameof(tester));
            tester.ResetConfig();
            tester.Init(output);
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
            var initialCount = pusher.PushedNodes.Count;
            var addedId = tester.Server.Server.AddObject(tester.Ids.Audit.Root, "NodeToAddForRebrowse");

            // Assert
            await TestUtils.WaitForCondition(
                () => tester.Client.Started,
                10
            );
            Assert.True(tester.Client.TryGetSubscription(RebrowseTriggerManager.SubscriptionName, out var _));

            tester.Server.Server.RemoveNode(addedId);
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
            var initialCount = pusher.PushedNodes.Count;
            var addedId = tester.Server.Server.AddObject(tester.Ids.Audit.Root, "NodeToAddForRebrowse");
            // tester.Server.Server.SetNamespacePublicationDate(DateTime.UtcNow);

            // Assert
            await TestUtils.WaitForCondition(
               () => tester.Client.Started,
               10
            );
            // Assert.False(pusher.PushedNodes.ContainsKey(addedId));
            Assert.False(tester.Client.TryGetSubscription(RebrowseTriggerManager.SubscriptionName, out var _));

            tester.Server.Server.RemoveNode(addedId);
            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
        }

        [Fact]
        public async Task TestRebrowseIsNotTriggered()
        {
            // Arrange
            var pusher = new DummyPusher(new DummyPusherConfig());
            tester.Config.Extraction.RebrowseTriggers = new RebrowseTriggersConfig
            {
                Targets = new RebrowseTriggerTargets { NamespacePublicationDate = true }
            };
            using var extractor = tester.BuildExtractor(true, pushers: pusher);
            
            
            // Act
            var runTask = extractor.RunExtractor();
            await extractor.WaitForSubscriptions();
            var initialCount = pusher.PushedNodes.Count;
            var addedId = tester.Server.Server.AddObject(tester.Ids.Audit.Root, "NodeToAddForRebrowse");
            tester.Server.Server.SetNamespacePublicationDate(DateTime.UtcNow.AddDays(-1));

            // Assert
            await TestUtils.WaitForCondition(
               () => tester.Client.Started,
               10
            );
             Assert.False(pusher.PushedNodes.ContainsKey(addedId));

            tester.Server.Server.RemoveNode(addedId);
            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
        }

        [Fact]
        public async Task TestRebrowseIsTriggered()
        {
            // Arrange
            var pusher = new DummyPusher(new DummyPusherConfig());
            tester.Config.Extraction.RebrowseTriggers = new RebrowseTriggersConfig
            {
                Targets = new RebrowseTriggerTargets { NamespacePublicationDate = true }
            };
            using var extractor = tester.BuildExtractor(true, pushers: pusher);
            
            
            // Act
            var runTask = extractor.RunExtractor();
            await extractor.WaitForSubscriptions();
            var initialCount = pusher.PushedNodes.Count;
            var addedId = tester.Server.Server.AddObject(tester.Ids.Audit.Root, "NodeToAddForRebrowse");
            tester.Server.Server.SetNamespacePublicationDate(DateTime.UtcNow);

            // Assert
            await TestUtils.WaitForCondition(
               () => tester.Client.Started,
               10
            );
             Assert.False(pusher.PushedNodes.ContainsKey(addedId));

            tester.Server.Server.RemoveNode(addedId);
            await BaseExtractorTestFixture.TerminateRunTask(runTask, extractor);
        }

        public static IEnumerable<object[]> TriggeringConfigurationStates => new List<object[]>
        {
            new object[] {
                new RebrowseTriggersConfig
                {
                    Targets = new RebrowseTriggerTargets { NamespacePublicationDate = true }
                }
            },
            // Should trigger if an existing namespace uri is specified
            new object[] {
                new RebrowseTriggersConfig
                {
                    Targets = new RebrowseTriggerTargets { NamespacePublicationDate = true },
                    Namespaces = new List<string> { "opc.tcp://test.localhost" },
                }
            },
            // Should trigger if at least one existing namespace uri is specified
            new object[] {
                new RebrowseTriggersConfig
                {
                    Targets = new RebrowseTriggerTargets { NamespacePublicationDate = true },
                    Namespaces = new List<string> { "opc.tcp://test.localhost", "unknown://uri" },
                }
            },
        };

        public static IEnumerable<object[]> NonTriggeringConfigurationStates => new List<object[]>
        {
            new object[] { new RebrowseTriggersConfig () },
            new object[] { null },
            new object[] { 
                new RebrowseTriggersConfig {
                    Targets = new RebrowseTriggerTargets { NamespacePublicationDate = false }
                }
            },
            new object[] { 
                new RebrowseTriggersConfig {
                    Targets = new RebrowseTriggerTargets { NamespacePublicationDate = false },
                    Namespaces = new List<string> { "unknown://uri" }
                }
            },
            new object[] { 
                new RebrowseTriggersConfig {
                    Targets = new RebrowseTriggerTargets { NamespacePublicationDate = true },
                    Namespaces = new List<string> { "unknown://uri" }
                }
            },
        };
    }
}