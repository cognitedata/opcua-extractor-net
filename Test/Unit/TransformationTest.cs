using Cognite.Extractor.Testing;
using Cognite.OpcUa;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Opc.Ua;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

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
        }
        [Fact]
        public void TestNameFilter()
        {
            var raw = new RawNodeFilter
            {
                Name = "Test"
            };
            var nodes = new[]
            {
                new UAObject(new NodeId(1), null, null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(2), "OtherTest", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(3), "Test", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(4), "Other", null, null, new NodeId("parent"), null),
            };
            var filter = new NodeFilter(raw);
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.Attributes.DisplayName, node.Id, node.FullAttributes.TypeDefinition?.Id, nss, node.NodeClass)).ToList();

            Assert.Equal(2, matched.Count);
            Assert.Equal(2, matchedBasic.Count);

            Assert.DoesNotContain(matched, node => (uint)node.Id.Identifier == 4u);
            Assert.DoesNotContain(matched, node => (uint)node.Id.Identifier == 1u);
        }
        [Fact]
        public void TestDescriptionFilter()
        {
            var raw = new RawNodeFilter
            {
                Description = "Test"
            };
            var nodes = new[]
            {
                new UAObject(new NodeId(1), "TestTest", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(2), "OtherTest", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(3), "Test", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(4), "Other", null, null, new NodeId("parent"), null)
            };
            nodes[0].Attributes.Description = "Some Test";
            nodes[1].Attributes.Description = "Some Other test";
            nodes[2].Attributes.Description = null;
            nodes[3].Attributes.Description = "";
            var filter = new NodeFilter(raw);
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.Attributes.DisplayName, node.Id, node.FullAttributes.TypeDefinition?.Id, nss, node.NodeClass)).ToList();

            Assert.Single(matched);
            Assert.Empty(matchedBasic);

            var node = matched.First();
            Assert.Equal(1u, node.Id.Identifier);
        }
        [Fact]
        public void TestIdFilter()
        {
            var raw = new RawNodeFilter
            {
                Id = "id|1|i=3|s=4"
            };
            var nodes = new[]
            {
                new UAObject(new NodeId(1), "TestTest", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId("id"), "OtherTest", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(3), "Test", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(4), "Other", null, null, new NodeId("parent"), null),
            };
            var filter = new NodeFilter(raw);
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                            filter.IsBasicMatch(node.Attributes.DisplayName, node.Id, node.FullAttributes.TypeDefinition?.Id, nss, node.NodeClass)).ToList();

            Assert.Equal(3, matched.Count);
            Assert.Equal(3, matchedBasic.Count);

            Assert.DoesNotContain(matched, node => node.Id.Identifier is uint unodeId && unodeId == 4u);
        }
        [Fact]
        public void TestNamespaceFilter()
        {
            var raw = new RawNodeFilter
            {
                Namespace = "test-|uri"
            };
            var nodes = new[]
            {
                new UAObject(new NodeId(1, 1), "TestTest", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(2, 2), "OtherTest", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(3, 2), "Test", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(4, 3), "Other", null, null, new NodeId("parent"), null),
            };
            var filter = new NodeFilter(raw);
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.Attributes.DisplayName, node.Id, node.FullAttributes.TypeDefinition?.Id, nss, node.NodeClass)).ToList();

            Assert.Equal(2, matched.Count);
            Assert.Equal(2, matchedBasic.Count);

            Assert.Contains(matched, node => (uint)node.Id.Identifier == 1u);
            Assert.Contains(matched, node => (uint)node.Id.Identifier == 4u);
        }
        [Fact]
        public void TestTypeDefinitionFilter()
        {
            var raw = new RawNodeFilter
            {
                TypeDefinition = "i=1|test"
            };

            var nodes = new[]
            {
                new UAObject(new NodeId(1), "TestTest", null, null, new NodeId("parent"), new UAObjectType(new NodeId(1))),
                new UAObject(new NodeId(2), "OtherTest", null, null, new NodeId("parent"), new UAObjectType(new NodeId(1))),
                new UAObject(new NodeId(3), "Test", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(4), "Other", null, null, new NodeId("parent"), new UAObjectType(new NodeId("test"))),
            };
            var filter = new NodeFilter(raw);
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.Attributes.DisplayName, node.Id, node.FullAttributes.TypeDefinition?.Id, nss, node.NodeClass)).ToList();

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
            var raw = new RawNodeFilter
            {
                IsArray = isArray
            };

            var nodes = new BaseUANode[]
            {
                new UAVariable(new NodeId(1), "TestTest", null, null, new NodeId("parent"), null),
                new UAVariable(new NodeId(2), "OtherTest", null, null, new NodeId("parent"), null),
                new UAVariable(new NodeId(3), "Test", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(4), "Other", null, null, new NodeId("parent"), null),
                new UAVariable(new NodeId(5), "Test", null, null, new NodeId("parent"), null)
            };
            (nodes[2].Attributes as Cognite.OpcUa.Nodes.VariableAttributes).ArrayDimensions = new[] { 4 };
            (nodes[4].Attributes as Cognite.OpcUa.Nodes.VariableAttributes).ArrayDimensions = new[] { 4 };

            var filter = new NodeFilter(raw);
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.Attributes.DisplayName, node.Id, GetTypeDefinition(node), nss, node.NodeClass)).ToList();

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
            var raw = new RawNodeFilter
            {
                Parent = new RawNodeFilter
                {
                    Name = "parent1"
                }
            };

            var parent1 = new UAObject(new NodeId("parent1"), "parent1", null, null, NodeId.Null, null);
            var parent2 = new UAObject(new NodeId("parent2"), "parent2", null, null, NodeId.Null, null);

            var nodes = new[]
            {
                new UAObject(new NodeId(1, 1), "TestTest", null, parent1, new NodeId("parent1"), null) { Parent = parent1 },
                new UAObject(new NodeId(2, 2), "OtherTest", null, parent1, new NodeId("parent1"), null) { Parent = parent1 },
                new UAObject(new NodeId(3, 2), "Test", null, parent2, new NodeId("parent2"), null) { Parent = parent2 },
                new UAObject(new NodeId(4, 3), "Other", null, parent2, new NodeId("parent2"), null) { Parent = parent2 },
            };
            var filter = new NodeFilter(raw);
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.Attributes.DisplayName, node.Id, node.FullAttributes.TypeDefinition?.Id, nss, node.NodeClass)).ToList();

            Assert.Equal(2, matched.Count);
            Assert.Empty(matchedBasic);

            Assert.Contains(matched, node => (uint)node.Id.Identifier == 1u);
            Assert.Contains(matched, node => (uint)node.Id.Identifier == 2u);
        }
        [Fact]
        public void TestNodeClassFilter()
        {
            var raw = new RawNodeFilter
            {
                NodeClass = NodeClass.Object
            };

            var nodes = new BaseUANode[]
            {
                new UAVariableType(new NodeId(1), "TestTest", null, null, new NodeId("parent")),
                new UAObject(new NodeId(2), "OtherTest", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(3), "Test", null, null, new NodeId("parent"), null),
                new UAObjectType(new NodeId(4), "Other", null, null, new NodeId("parent")),
            };
            var filter = new NodeFilter(raw);
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.Attributes.DisplayName, node.Id, GetTypeDefinition(node), nss, node.NodeClass)).ToList();

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
            var raw = new RawNodeFilter
            {
                Historizing = historizing
            };

            var nodes = new BaseUANode[]
            {
                new UAVariable(new NodeId(1), "TestTest", null, null, new NodeId("parent"), null),
                new UAVariable(new NodeId(2), "OtherTest", null, null, new NodeId("parent"), null),
                new UAVariable(new NodeId(3), "Test", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(4), "Other", null, null, new NodeId("parent"), null),
                new UAVariable(new NodeId(5), "Test", null, null, new NodeId("parent"), null)
            };
            (nodes[2].Attributes as Cognite.OpcUa.Nodes.VariableAttributes).Historizing = true;
            (nodes[4].Attributes as Cognite.OpcUa.Nodes.VariableAttributes).Historizing = true;

            var filter = new NodeFilter(raw);
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.Attributes.DisplayName, node.Id, GetTypeDefinition(node), nss, node.NodeClass)).ToList();

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
            var raw = new RawNodeFilter
            {
                Id = "i=1",
                Description = "target",
                TypeDefinition = "i=1",
                IsArray = true,
                Historizing = true,
                Name = "target",
                Parent = new RawNodeFilter
                {
                    Name = "parent1"
                },
                Namespace = "test-",
                NodeClass = NodeClass.Variable
            };
            var parent1 = new UAObject(new NodeId("parent1"), "parent1", null, null, NodeId.Null, null);
            var parent2 = new UAObject(new NodeId("parent2"), "parent2", null, null, NodeId.Null, null);
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

            var filter = new NodeFilter(raw);
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.Attributes.DisplayName, node.Id, GetTypeDefinition(node), nss, node.NodeClass)).ToList();

            Assert.Single(matched);
            Assert.Empty(matchedBasic);
        }
        [Fact]
        public void TestIgnoreTransformation()
        {
            var raw = new RawNodeTransformation
            {
                Filter = new RawNodeFilter
                {
                    Name = "Test"
                },
                Type = TransformationType.Ignore
            };
            var nodes = new[]
            {
                new UAObject(new NodeId(1), null, null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(2), "OtherTest", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(3), "Test", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(4), "Other", null, null, new NodeId("parent"), null),
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
                Filter = new RawNodeFilter
                {
                    Name = "Test"
                },
                Type = TransformationType.Property
            };
            var nodes = new[]
            {
                new UAObject(new NodeId(1), null, null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(2), "OtherTest", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(3), "Test", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(4), "Other", null, null, new NodeId("parent"), null),
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
                Filter = new RawNodeFilter
                {
                    Name = "Test"
                },
                Type = TransformationType.Property
            };
            var raw2 = new RawNodeTransformation
            {
                Filter = new RawNodeFilter
                {
                    Name = "Other"
                },
                Type = TransformationType.TimeSeries
            };
            var nodes = new BaseUANode[]
            {
                new UAVariable(new NodeId(1), null, null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(2), "OtherTest", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(3), "Test", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(4), "Other", null, null, new NodeId("parent"), null),
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
            Assert.False(nodes[1].IsProperty);
            Assert.True(nodes[2].IsProperty);
            Assert.False(nodes[3].IsProperty);
        }

        [Fact]
        public void TestAsEventTransformation()
        {
            var raw = new RawNodeTransformation
            {
                Filter = new RawNodeFilter
                {
                    Name = "Test"
                },
                Type = TransformationType.AsEvents
            };
            var nodes = new BaseUANode[]
            {
                new UAVariable(new NodeId(1), null, null, null, new NodeId("parent"), null),
                new UAVariable(new NodeId(2), "OtherTest", null, null, new NodeId("parent"), null),
                new UAVariable(new NodeId(3), "Test", null, null, new NodeId("parent"), null),
                new UAObject(new NodeId(4), "TestTest", null, null, new NodeId("parent"), null),
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
                Filter = new RawNodeFilter
                {
                    Name = "name",
                    Namespace = "namespace",
                    NodeClass = NodeClass.Variable,
                    Description = "description",
                    TypeDefinition = "typeDefinition",
                    Id = "id",
                    IsArray = true,
                    Historizing = true,
                    Parent = new RawNodeFilter
                    {
                        Name = "name2",
                        Namespace = "namespace2",
                        NodeClass = NodeClass.Object,
                        Description = "description2",
                        TypeDefinition = "typeDefinition2",
                        Id = "id2",
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
    }
}
