using Cognite.Extractor.Configuration;
using Cognite.Extractor.Testing;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;
using Xunit.Abstractions;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;
using YamlDotNet.Serialization.TypeInspectors;

namespace Test.Unit
{
    public class TransformationTest
    {
        private readonly NamespaceTable nss;
        private readonly ILogger log;

        public TransformationTest(ITestOutputHelper output)
        {
            nss = new NamespaceTable();
            nss.Append("opc.tcp://test-namespace.one");
            nss.Append("https://some-namespace.org");
            nss.Append("my:namespace:uri");
            var services = new ServiceCollection();
            services.AddTestLogging(output);
            log = services.BuildServiceProvider().GetRequiredService<ILogger<NodeTransformation>>();
            try
            {
                ConfigurationUtils.AddTypeConverter(new FieldFilterConverter());
            }
            catch { }
        }
        [Fact]
        public void TestNameFilter()
        {
            var filter = new NodeFilter
            {
                Name = new RegexFieldFilter("Test")
            };
            var nodes = new[]
            {
                new UAObject(new NodeId(1), null, null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(2), "OtherTest", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(3), "Test", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(4), "Other", null, null, new NodeId("parent", 0), null),
            };
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.Name, node.Id, node.FullAttributes.TypeDefinition?.Id, nss, node.NodeClass)).ToList();

            Assert.Equal(2, matched.Count);
            Assert.Equal(2, matchedBasic.Count);

            Assert.DoesNotContain(matched, node => (uint)node.Id.Identifier == 4u);
            Assert.DoesNotContain(matched, node => (uint)node.Id.Identifier == 1u);
        }
        [Fact]
        public void TestDescriptionFilter()
        {
            var filter = new NodeFilter
            {
                Description = new RegexFieldFilter("Test")
            };
            var nodes = new[]
            {
                new UAObject(new NodeId(1), "TestTest", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(2), "OtherTest", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(3), "Test", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(4), "Other", null, null, new NodeId("parent", 0), null)
            };
            nodes[0].Attributes.Description = "Some Test";
            nodes[1].Attributes.Description = "Some Other test";
            nodes[2].Attributes.Description = null;
            nodes[3].Attributes.Description = "";
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.Name, node.Id, node.FullAttributes.TypeDefinition?.Id, nss, node.NodeClass)).ToList();

            Assert.Single(matched);
            Assert.Empty(matchedBasic);

            var node = matched.First();
            Assert.Equal(1u, node.Id.Identifier);
        }
        [Fact]
        public void TestIdFilter()
        {
            var filter = new NodeFilter
            {
                Id = new RegexFieldFilter("id|1|i=3|s=4")
            };
            var nodes = new[]
            {
                new UAObject(new NodeId(1), "TestTest", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId("id", 0), "OtherTest", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(3), "Test", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(4), "Other", null, null, new NodeId("parent", 0), null),
            };
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                            filter.IsBasicMatch(node.Name, node.Id, node.FullAttributes.TypeDefinition?.Id, nss, node.NodeClass)).ToList();

            Assert.Equal(3, matched.Count);
            Assert.Equal(3, matchedBasic.Count);

            Assert.DoesNotContain(matched, node => node.Id.Identifier is uint unodeId && unodeId == 4u);
        }
        [Fact]
        public void TestNamespaceFilter()
        {
            var filter = new NodeFilter
            {
                Namespace = new RegexFieldFilter("test-|uri")
            };
            var nodes = new[]
            {
                new UAObject(new NodeId(1, 1), "TestTest", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(2, 2), "OtherTest", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(3, 2), "Test", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(4, 3), "Other", null, null, new NodeId("parent", 0), null),
            };
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.Name, node.Id, node.FullAttributes.TypeDefinition?.Id, nss, node.NodeClass)).ToList();

            Assert.Equal(2, matched.Count);
            Assert.Equal(2, matchedBasic.Count);

            Assert.Contains(matched, node => (uint)node.Id.Identifier == 1u);
            Assert.Contains(matched, node => (uint)node.Id.Identifier == 4u);
        }
        [Fact]
        public void TestTypeDefinitionFilter()
        {
            var filter = new NodeFilter
            {
                TypeDefinition = new RegexFieldFilter("i=1|test")
            };

            var nodes = new[]
            {
                new UAObject(new NodeId(1), "TestTest", null, null, new NodeId("parent", 0), new UAObjectType(new NodeId(1))),
                new UAObject(new NodeId(2), "OtherTest", null, null, new NodeId("parent", 0), new UAObjectType(new NodeId(2))),
                new UAObject(new NodeId(3), "Test", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(4), "Other", null, null, new NodeId("parent", 0), new UAObjectType(new NodeId("test", 0))),
            };
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.Name, node.Id, node.FullAttributes.TypeDefinition?.Id, nss, node.NodeClass)).ToList();

            Assert.Equal(2, matched.Count);
            Assert.Equal(2, matchedBasic.Count);

            Assert.Contains(matched, node => (uint)node.Id.Identifier == 1u);
            Assert.Contains(matched, node => (uint)node.Id.Identifier == 4u);
        }

        private static NodeId GetTypeDefinition(BaseUANode node)
        {
            if (node is UAObject obj) return obj.FullAttributes.TypeDefinition?.Id;
            else if (node is UAVariable vr) return vr.FullAttributes.TypeDefinition?.Id;
            return null;
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void TestIsArrayFilter(bool isArray)
        {
            var filter = new NodeFilter
            {
                IsArray = isArray
            };

            var nodes = new BaseUANode[]
            {
                new UAVariable(new NodeId(1), "TestTest", null, null, new NodeId("parent", 0), null),
                new UAVariable(new NodeId(2), "OtherTest", null, null, new NodeId("parent", 0), null),
                new UAVariable(new NodeId(3), "Test", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(4), "Other", null, null, new NodeId("parent", 0), null),
                new UAVariable(new NodeId(5), "Test", null, null, new NodeId("parent", 0), null)
            };
            (nodes[2].Attributes as Cognite.OpcUa.Nodes.VariableAttributes).ArrayDimensions = new[] { 4 };
            (nodes[4].Attributes as Cognite.OpcUa.Nodes.VariableAttributes).ArrayDimensions = new[] { 4 };

            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.Name, node.Id, GetTypeDefinition(node), nss, node.NodeClass)).ToList();

            Assert.Equal(2, matched.Count);
            Assert.Empty(matchedBasic);

            if (isArray)
            {
                Assert.Contains(matched, node => (uint)node.Id.Identifier == 3u);
                Assert.Contains(matched, node => (uint)node.Id.Identifier == 5u);
            }
            else
            {
                Assert.Contains(matched, node => (uint)node.Id.Identifier == 1u);
                Assert.Contains(matched, node => (uint)node.Id.Identifier == 2u);
            }
        }
        [Fact]
        public void TestParentFilter()
        {
            var filter = new NodeFilter
            {
                Parent = new NodeFilter
                {
                    Name = new RegexFieldFilter("parent1")
                }
            };

            var parent1 = new UAObject(new NodeId("parent1", 0), "parent1", null, null, NodeId.Null, null);
            var parent2 = new UAObject(new NodeId("parent2", 0), "parent2", null, null, NodeId.Null, null);

            var nodes = new[]
            {
                new UAObject(new NodeId(1, 1), "TestTest", null, parent1, new NodeId("parent1", 0), null) { Parent = parent1 },
                new UAObject(new NodeId(2, 2), "OtherTest", null, parent1, new NodeId("parent1", 0), null) { Parent = parent1 },
                new UAObject(new NodeId(3, 2), "Test", null, parent2, new NodeId("parent2", 0), null) { Parent = parent2 },
                new UAObject(new NodeId(4, 3), "Other", null, parent2, new NodeId("parent2", 0), null) { Parent = parent2 },
            };
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.Name, node.Id, node.FullAttributes.TypeDefinition?.Id, nss, node.NodeClass)).ToList();

            Assert.Equal(2, matched.Count);
            Assert.Empty(matchedBasic);

            Assert.Contains(matched, node => (uint)node.Id.Identifier == 1u);
            Assert.Contains(matched, node => (uint)node.Id.Identifier == 2u);
        }
        [Fact]
        public void TestNodeClassFilter()
        {
            var filter = new NodeFilter
            {
                NodeClass = NodeClass.Object
            };

            var nodes = new BaseUANode[]
            {
                new UAVariableType(new NodeId(1), "TestTest", null, null, new NodeId("parent", 0)),
                new UAObject(new NodeId(2), "OtherTest", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(3), "Test", null, null, new NodeId("parent", 0), null),
                new UAObjectType(new NodeId(4), "Other", null, null, new NodeId("parent", 0)),
            };
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.Name, node.Id, GetTypeDefinition(node), nss, node.NodeClass)).ToList();

            Assert.Equal(2, matched.Count);
            Assert.Equal(2, matchedBasic.Count);

            Assert.Contains(matched, node => (uint)node.Id.Identifier == 2u);
            Assert.Contains(matched, node => (uint)node.Id.Identifier == 3u);
        }
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void TestHistorizingFilter(bool historizing)
        {
            var filter = new NodeFilter
            {
                Historizing = historizing
            };

            var nodes = new BaseUANode[]
            {
                new UAVariable(new NodeId(1), "TestTest", null, null, new NodeId("parent", 0), null),
                new UAVariable(new NodeId(2), "OtherTest", null, null, new NodeId("parent", 0), null),
                new UAVariable(new NodeId(3), "Test", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(4), "Other", null, null, new NodeId("parent", 0), null),
                new UAVariable(new NodeId(5), "Test", null, null, new NodeId("parent", 0), null)
            };
            (nodes[2].Attributes as Cognite.OpcUa.Nodes.VariableAttributes).Historizing = true;
            (nodes[4].Attributes as Cognite.OpcUa.Nodes.VariableAttributes).Historizing = true;

            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.Name, node.Id, GetTypeDefinition(node), nss, node.NodeClass)).ToList();

            Assert.Equal(2, matched.Count);
            Assert.Empty(matchedBasic);

            if (historizing)
            {
                Assert.Contains(matched, node => (uint)node.Id.Identifier == 3u);
                Assert.Contains(matched, node => (uint)node.Id.Identifier == 5u);
            }
            else
            {
                Assert.Contains(matched, node => (uint)node.Id.Identifier == 1u);
                Assert.Contains(matched, node => (uint)node.Id.Identifier == 2u);
            }
        }
        [Fact]
        public void TestMultipleFilter()
        {
            var filter = new NodeFilter
            {
                Id = new RegexFieldFilter("i=1"),
                Description = new RegexFieldFilter("target"),
                TypeDefinition = new RegexFieldFilter("i=1"),
                IsArray = true,
                Historizing = true,
                Name = new RegexFieldFilter("target"),
                Parent = new NodeFilter
                {
                    Name = new RegexFieldFilter("parent1")
                },
                Namespace = new RegexFieldFilter("test-"),
                NodeClass = NodeClass.Variable
            };
            var parent1 = new UAObject(new NodeId("parent1", 0), "parent1", null, null, NodeId.Null, null);
            var parent2 = new UAObject(new NodeId("parent2", 0), "parent2", null, null, NodeId.Null, null);
            // Each node deviates on only one point.
            var nodes = new List<BaseUANode>();
            for (int i = 0; i < 10; i++)
            {
                NodeClass nodeClass = i == 7 ? NodeClass.VariableType : NodeClass.Variable;

                NodeId id;
                if (i == 0)
                {
                    id = new NodeId(2, 1);
                }
                else if (i == 6)
                {
                    id = new NodeId(1, 2);
                }
                else
                {
                    id = new NodeId(1, 1);
                }

                BaseUANode node;
                if (i != 7)
                {
                    node = new UAVariable(id, i == 4 ? "not" : "target", null, i == 5 ? parent2 : parent1, NodeId.Null, new UAVariableType(i == 2 ? new NodeId(2) : new NodeId(1)));
                    (node as UAVariable).FullAttributes.ArrayDimensions = i == 3 ? null : new[] { 4 };
                    (node as UAVariable).FullAttributes.Historizing = i == 8 ? true : false;
                }
                else
                {
                    node = new UAVariableType(id, "not", null, parent1, NodeId.Null);
                }

                node.Attributes.Description = i == 1 ? "not" : "target";
                nodes.Add(node);
            }

            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.Name, node.Id, GetTypeDefinition(node), nss, node.NodeClass)).ToList();

            Assert.Single(matched);
            Assert.Empty(matchedBasic);
        }
        [Fact]
        public void TestIgnoreTransformation()
        {
            var raw = new RawNodeTransformation
            {
                Filter = new NodeFilter
                {
                    Name = new RegexFieldFilter("Test")
                },
                Type = TransformationType.Ignore
            };
            var nodes = new[]
            {
                new UAObject(new NodeId(1), null, null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(2), "OtherTest", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(3), "Test", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(4), "Other", null, null, new NodeId("parent", 0), null),
            };
            var cfg = new FullConfig();
            cfg.GenerateDefaults();
            var trans = new NodeTransformation(raw, 0);
            foreach (var node in nodes)
            {
                trans.ApplyTransformation(log, node, nss, cfg);
            }
            Assert.False(nodes[0].Ignore);
            Assert.True(nodes[1].Ignore);
            Assert.True(nodes[2].Ignore);
            Assert.False(nodes[3].Ignore);
        }

        [Fact]
        public void TestPropertyTransformation()
        {
            var raw = new RawNodeTransformation
            {
                Filter = new NodeFilter
                {
                    Name = new RegexFieldFilter("Test")
                },
                Type = TransformationType.Property
            };
            var nodes = new[]
            {
                new UAObject(new NodeId(1), null, null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(2), "OtherTest", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(3), "Test", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(4), "Other", null, null, new NodeId("parent", 0), null),
            };
            var cfg = new FullConfig();
            cfg.GenerateDefaults();
            var trans = new NodeTransformation(raw, 0);
            foreach (var node in nodes)
            {
                trans.ApplyTransformation(log, node, nss, cfg);
            }
            Assert.False(nodes[0].IsProperty);
            Assert.True(nodes[1].IsProperty);
            Assert.True(nodes[2].IsProperty);
            Assert.False(nodes[3].IsProperty);
        }
        [Fact]
        public void TestTimeSeriesTransformation()
        {
            var raw = new RawNodeTransformation
            {
                Filter = new NodeFilter
                {
                    Name = new RegexFieldFilter("Test")
                },
                Type = TransformationType.Property
            };
            var raw2 = new RawNodeTransformation
            {
                Filter = new NodeFilter
                {
                    Name = new RegexFieldFilter("Other")
                },
                Type = TransformationType.TimeSeries
            };
            var nodes = new BaseUANode[]
            {
                new UAVariable(new NodeId(1), null, null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(2), "OtherTest", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(3), "Test", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(4), "Other", null, null, new NodeId("parent", 0), null),
            };
            var cfg = new FullConfig();
            cfg.GenerateDefaults();
            var trans = new NodeTransformation(raw, 0);
            var trans2 = new NodeTransformation(raw2, 0);
            foreach (var node in nodes)
            {
                trans.ApplyTransformation(log, node, nss, cfg);
                trans2.ApplyTransformation(log, node, nss, cfg);
            }
            Assert.False(nodes[0].IsProperty);
            Assert.True(nodes[1].IsProperty);
            Assert.True(nodes[2].IsProperty);
            Assert.False(nodes[3].IsProperty);
        }

        [Fact]
        public void TestAsEventTransformation()
        {
            var raw = new RawNodeTransformation
            {
                Filter = new NodeFilter
                {
                    Name = new RegexFieldFilter("Test")
                },
                Type = TransformationType.AsEvents
            };
            var nodes = new BaseUANode[]
            {
                new UAVariable(new NodeId(1), null, null, null, new NodeId("parent", 0), null),
                new UAVariable(new NodeId(2), "OtherTest", null, null, new NodeId("parent", 0), null),
                new UAVariable(new NodeId(3), "Test", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(4), "TestTest", null, null, new NodeId("parent", 0), null),
            };
            var cfg = new FullConfig();
            cfg.GenerateDefaults();
            var trans = new NodeTransformation(raw, 0);
            foreach (var node in nodes)
            {
                trans.ApplyTransformation(log, node, nss, cfg);
            }
            Assert.False((nodes[0] as UAVariable)?.AsEvents ?? false);
            Assert.True((nodes[1] as UAVariable)?.AsEvents ?? false);
            Assert.True((nodes[2] as UAVariable)?.AsEvents ?? false);
            Assert.False((nodes[3] as UAVariable)?.AsEvents ?? false);
        }

        [Fact]
        public void TestLogTransformation()
        {
            var raw = new RawNodeTransformation
            {
                Filter = new NodeFilter
                {
                    Name = new RegexFieldFilter("name"),
                    Namespace = new RegexFieldFilter("namespace"),
                    NodeClass = NodeClass.Variable,
                    Description = new RegexFieldFilter("description"),
                    TypeDefinition = new RegexFieldFilter("typeDefinition"),
                    Id = new RegexFieldFilter("id"),
                    IsArray = true,
                    Historizing = true,
                    Parent = new NodeFilter
                    {
                        Name = new RegexFieldFilter("name2"),
                        Namespace = new RegexFieldFilter("namespace2"),
                        NodeClass = NodeClass.Object,
                        Description = new RegexFieldFilter("description2"),
                        TypeDefinition = new RegexFieldFilter("typeDefinition2"),
                        Id = new RegexFieldFilter("id2"),
                        IsArray = false,
                        Historizing = false
                    }
                },
                Type = TransformationType.Property
            };
            var trans = new NodeTransformation(raw, 0);
            var result = trans.ToString();
            Assert.Equal(@"Transformation 0:
Type: Property
Filter:
    Name: name
    Description: description
    Id: id
    IsArray: True
    Historizing: True
    Namespace: namespace
    TypeDefinition: typeDefinition
    NodeClass: Variable
    Parent:
        Name: name2
        Description: description2
        Id: id2
        IsArray: False
        Historizing: False
        Namespace: namespace2
        TypeDefinition: typeDefinition2
        NodeClass: Object
".ReplaceLineEndings(), result.ReplaceLineEndings());
        }

        [Fact]
        public void TestIncludeTransformation()
        {
            var raw = new RawNodeTransformation
            {
                Filter = new NodeFilter
                {
                    Name = new RegexFieldFilter("Test")
                },
                Type = TransformationType.Include
            };
            var tf = new NodeTransformation(raw, 0);

            var tfs = new TransformationCollection(new List<NodeTransformation> { tf });

            var nodes = new BaseUANode[]
            {
                new UAVariable(new NodeId(1), null, null, null, new NodeId("parent", 0), null),
                new UAVariable(new NodeId(2), "OtherTest", null, null, new NodeId("parent", 0), null),
                new UAVariable(new NodeId(3), "Test", null, null, new NodeId("parent", 0), null),
                new UAObject(new NodeId(4), "TestTest", null, null, new NodeId("parent", 0), null),
            };

            var cfg = new FullConfig();
            cfg.GenerateDefaults();
            foreach (var node in nodes)
            {
                tfs.ApplyTransformations(log, node, nss, cfg);
            }
            Assert.True(nodes[0].Ignore);
            Assert.False(nodes[1].Ignore);
            Assert.False(nodes[2].Ignore);
            Assert.False(nodes[3].Ignore);
        }

        [Fact]
        public void TestFromYaml()
        {
            var extFile = "transform_entries.txt";
            File.WriteAllLines(extFile, new[] {
                "foo",
                "bar",
                "",
                "   ",
                "baz"
            });
            var data = @"
                transformations:
                    - type: Property
                      filter:
                        name: ""regex""
                        description:
                          - desc1
                          - desc2
                        id:
                          file: transform_entries.txt
            ";
            var conf = ConfigurationUtils.ReadString<ExtractionConfig>(data, false);

            Assert.Single(conf.Transformations);
            var tf = conf.Transformations.First();
            var desc = Assert.IsType<ListFieldFilter>(tf.Filter.Description);
            Assert.Equal(2, desc.Raw.Count());
            Assert.Null(desc.OriginalFile);
            Assert.Contains("desc1", desc.Raw);
            Assert.Contains("desc2", desc.Raw);
            var name = Assert.IsType<RegexFieldFilter>(tf.Filter.Name);
            Assert.Equal("regex", name.Raw);
            var nodeId = Assert.IsType<ListFieldFilter>(tf.Filter.Id);
            Assert.Equal(3, nodeId.Raw.Count());
            Assert.Contains("foo", nodeId.Raw);
            Assert.Contains("bar", nodeId.Raw);
            Assert.Contains("baz", nodeId.Raw);

            //var str = serializer.Serialize(conf.Transformations);
            var str = ConfigurationUtils.ConfigToString(
                conf.Transformations,
                Enumerable.Empty<string>(),
                Enumerable.Empty<string>(),
                Enumerable.Empty<string>(),
                false);
            Assert.Equal(
@"- type: ""Property""
    filter:
        name: ""regex""
        description:
          - ""desc1""
          - ""desc2""
        id:
            file: ""transform_entries.txt""
", str);
        }
    }
}
