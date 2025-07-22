using System.Collections.Generic;
using Cognite.OpcUa.Config;
using Xunit;

namespace Test.Config
{
    public class MqttConfigTest
    {
        [Fact]
        public void TestNestedTransmissionStrategyConfig()
        {
            // Arrange
            var config = new MqttPusherConfig();
            
            // Act - Set using new nested format
            config.SetTransmissionStrategy(
                MqttTransmissionStrategy.ROOT_NODE_BASED, 
                new List<List<string>>
                {
                    new() { "tag1", "tag2", "tag3" },
                    new() { "tag4", "tag5" }
                }
            );
            
            // Assert
            Assert.Equal(MqttTransmissionStrategy.ROOT_NODE_BASED, config.GetEffectiveTransmissionStrategy());
            Assert.NotNull(config.GetEffectiveTagLists());
            Assert.Equal(2, config.GetEffectiveTagLists().Count);
            Assert.Equal(3, config.GetEffectiveTagLists()[0].Count);
            Assert.Equal(2, config.GetEffectiveTagLists()[1].Count);
        }

        [Fact]
        public void TestLegacyTransmissionStrategyCompatibility()
        {
            // Arrange
            var config = new MqttPusherConfig
            {
                TransmissionStrategy = MqttTransmissionStrategy.TAG_LIST_BASED,
                TagLists = new List<List<string>>
                {
                    new() { "legacy1", "legacy2" }
                }
            };
            
            // Act & Assert - Should fallback to legacy properties
            Assert.Equal(MqttTransmissionStrategy.TAG_LIST_BASED, config.GetEffectiveTransmissionStrategy());
            Assert.NotNull(config.GetEffectiveTagLists());
            Assert.Single(config.GetEffectiveTagLists());
            Assert.Equal(2, config.GetEffectiveTagLists()[0].Count);
        }

        [Fact]
        public void TestNestedConfigOverridesLegacy()
        {
            // Arrange
            var config = new MqttPusherConfig
            {
                // Legacy settings
                TransmissionStrategy = MqttTransmissionStrategy.CHUNK_BASED,
                TagLists = new List<List<string>>
                {
                    new() { "legacy1", "legacy2" }
                }
            };
            
            // Act - Set nested config (should override legacy)
            config.SetTransmissionStrategy(
                MqttTransmissionStrategy.ROOT_NODE_BASED,
                new List<List<string>>
                {
                    new() { "nested1", "nested2", "nested3" }
                }
            );
            
            // Assert - Nested config should take precedence
            Assert.Equal(MqttTransmissionStrategy.ROOT_NODE_BASED, config.GetEffectiveTransmissionStrategy());
            Assert.Single(config.GetEffectiveTagLists());
            Assert.Equal(3, config.GetEffectiveTagLists()[0].Count);
            Assert.Contains("nested1", config.GetEffectiveTagLists()[0]);
        }

        [Fact]
        public void TestMqttTransmissionStrategyConfig()
        {
            // Arrange
            var strategyConfig = new MqttTransmissionStrategyConfig
            {
                DataGroupBy = MqttTransmissionStrategy.TAG_CHANGE_BASED,
                TagLists = new List<List<string>>
                {
                    new() { "s=S.A.Tag1", "s=S.A.Tag2" },
                    new() { "s=S.B.Tag1", "s=S.B.Tag2" }
                }
            };
            
            // Assert
            Assert.Equal(MqttTransmissionStrategy.TAG_CHANGE_BASED, strategyConfig.DataGroupBy);
            Assert.NotNull(strategyConfig.TagLists);
            Assert.Equal(2, strategyConfig.TagLists.Count);
        }
    }
} 