using System.Collections.Generic;
using System.IO;
using Cognite.OpcUa.Config;
using Xunit;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace Test.Config
{
    public class ConfigLoadingTest
    {
        [Fact]
        public void TestNestedMqttTransmissionStrategyYamlLoading()
        {
            // Arrange - Create a test YAML with nested mqtt-transmission-strategy
            var yamlContent = @"
enabled: true
mqtt-transmission-strategy:
  data-group-by: ROOT_NODE_BASED
  tag-lists:
    - [""tag1"", ""tag2"", ""tag3""]
    - [""tag4"", ""tag5""]
max-concurrency: 4
";

            var deserializer = new DeserializerBuilder()
                .WithNamingConvention(HyphenatedNamingConvention.Instance)
                .IgnoreUnmatchedProperties()
                .Build();

            // Act
            var config = deserializer.Deserialize<MqttPusherConfig>(yamlContent);

            // Assert
            Assert.True(config.Enabled);
            Assert.NotNull(config.TransmissionStrategyConfig);
            Assert.Equal(MqttTransmissionStrategy.ROOT_NODE_BASED, config.TransmissionStrategyConfig.DataGroupBy);
            Assert.NotNull(config.TransmissionStrategyConfig.TagLists);
            Assert.Equal(2, config.TransmissionStrategyConfig.TagLists.Count);
            Assert.Equal(3, config.TransmissionStrategyConfig.TagLists[0].Count);
            Assert.Equal(2, config.TransmissionStrategyConfig.TagLists[1].Count);
            
            // Test compatibility methods
            Assert.Equal(MqttTransmissionStrategy.ROOT_NODE_BASED, config.GetEffectiveTransmissionStrategy());
            Assert.Equal(2, config.GetEffectiveTagLists()?.Count);
        }

        [Fact]
        public void TestLegacyMqttTransmissionStrategyYamlLoading()
        {
            // Arrange - Create a test YAML with legacy flat structure
            var yamlContent = @"
enabled: true
transmission-strategy: TAG_LIST_BASED
tag-lists:
  - [""legacy1"", ""legacy2""]
max-concurrency: 2
";

            var deserializer = new DeserializerBuilder()
                .WithNamingConvention(HyphenatedNamingConvention.Instance)
                .IgnoreUnmatchedProperties()
                .Build();

            // Act
            var config = deserializer.Deserialize<MqttPusherConfig>(yamlContent);

            // Assert
            Assert.True(config.Enabled);
            Assert.Equal(MqttTransmissionStrategy.TAG_LIST_BASED, config.TransmissionStrategy);
            Assert.NotNull(config.TagLists);
            Assert.Single(config.TagLists);
            Assert.Equal(2, config.TagLists[0].Count);
            
            // Test compatibility methods (should fall back to legacy)
            Assert.Equal(MqttTransmissionStrategy.TAG_LIST_BASED, config.GetEffectiveTransmissionStrategy());
            Assert.Single(config.GetEffectiveTagLists());
        }
    }
} 