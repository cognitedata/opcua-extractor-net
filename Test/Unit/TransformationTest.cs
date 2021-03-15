using Cognite.OpcUa;
using Cognite.OpcUa.Types;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Net.Http;
using Xunit;

namespace Test.Unit
{
    public class TransformationTest
    {
        private NamespaceTable nss;
        public TransformationTest()
        {
            nss = new NamespaceTable();
            nss.Append("opc.tcp://test-namespace.one");
            nss.Append("https://some-namespace.org");
            nss.Append("my:namespace:uri");
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
                new UANode(new NodeId(1), null, new NodeId("parent")),
                new UANode(new NodeId(2), "OtherTest", new NodeId("parent")),
                new UANode(new NodeId(3), "Test", new NodeId("parent")),
                new UANode(new NodeId(4), "Other", new NodeId("parent")),
            };
            var filter = new NodeFilter(raw);
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.DisplayName, node.Id, node.NodeType?.Id, nss, node.NodeClass)).ToList();

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
                new UANode(new NodeId(1), "TestTest", new NodeId("parent")) { Description = "Some Test" },
                new UANode(new NodeId(2), "OtherTest", new NodeId("parent")) { Description = "Some Other test" },
                new UANode(new NodeId(3), "Test", new NodeId("parent")) { Description = null },
                new UANode(new NodeId(4), "Other", new NodeId("parent")) { Description = "" },
            };
            var filter = new NodeFilter(raw);
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.DisplayName, node.Id, node.NodeType?.Id, nss, node.NodeClass)).ToList();

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
                new UANode(new NodeId(1), "TestTest", new NodeId("parent")),
                new UANode(new NodeId("id"), "OtherTest", new NodeId("parent")),
                new UANode(new NodeId(3), "Test", new NodeId("parent")),
                new UANode(new NodeId(4), "Other", new NodeId("parent")),
            };
            var filter = new NodeFilter(raw);
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                            filter.IsBasicMatch(node.DisplayName, node.Id, node.NodeType?.Id, nss, node.NodeClass)).ToList();

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
                new UANode(new NodeId(1, 1), "TestTest", new NodeId("parent")),
                new UANode(new NodeId(2, 2), "OtherTest", new NodeId("parent")),
                new UANode(new NodeId(3, 2), "Test", new NodeId("parent")),
                new UANode(new NodeId(4, 3), "Other", new NodeId("parent")),
            };
            var filter = new NodeFilter(raw);
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.DisplayName, node.Id, node.NodeType?.Id, nss, node.NodeClass)).ToList();

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
                new UANode(new NodeId(1), "TestTest", new NodeId("parent")) { NodeType = new UANodeType(new NodeId(1), false) },
                new UANode(new NodeId(2), "OtherTest", new NodeId("parent")) { NodeType = new UANodeType(new NodeId(2), false) },
                new UANode(new NodeId(3), "Test", new NodeId("parent")) { NodeType = null },
                new UANode(new NodeId(4), "Other", new NodeId("parent")) { NodeType = new UANodeType(new NodeId("test"), false) },
            };
            var filter = new NodeFilter(raw);
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.DisplayName, node.Id, node.NodeType?.Id, nss, node.NodeClass)).ToList();

            Assert.Equal(2, matched.Count);
            Assert.Equal(2, matchedBasic.Count);

            Assert.Contains(matched, node => (uint)node.Id.Identifier == 1u);
            Assert.Contains(matched, node => (uint)node.Id.Identifier == 4u);
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

            var nodes = new[]
            {
                new UAVariable(new NodeId(1), "TestTest", new NodeId("parent")),
                new UAVariable(new NodeId(2), "OtherTest", new NodeId("parent")),
                new UAVariable(new NodeId(3), "Test", new NodeId("parent")) { ArrayDimensions = new Collection<int>(new [] { 4 }) },
                new UANode(new NodeId(4), "Other", new NodeId("parent")),
                new UAVariable(new NodeId(5), "Test", new NodeId("parent")) { ArrayDimensions = new Collection<int>(new [] { 4 }) }
            };
            var filter = new NodeFilter(raw);
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.DisplayName, node.Id, node.NodeType?.Id, nss, node.NodeClass)).ToList();

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

            var parent1 = new UANode(new NodeId("parent1"), "parent1", NodeId.Null);
            var parent2 = new UANode(new NodeId("parent2"), "parent2", NodeId.Null);

            var nodes = new[]
            {
                new UANode(new NodeId(1, 1), "TestTest", new NodeId("parent1")) { Parent = parent1 },
                new UANode(new NodeId(2, 2), "OtherTest", new NodeId("parent1")) { Parent = parent1 },
                new UANode(new NodeId(3, 2), "Test", new NodeId("parent2")) { Parent = parent2 },
                new UANode(new NodeId(4, 3), "Other", new NodeId("parent2")) { Parent = parent2 },
            };
            var filter = new NodeFilter(raw);
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.DisplayName, node.Id, node.NodeType?.Id, nss, node.NodeClass)).ToList();

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

            var nodes = new[]
            {
                new UANode(new NodeId(1), "TestTest", new NodeId("parent"), NodeClass.VariableType),
                new UANode(new NodeId(2), "OtherTest", new NodeId("parent"), NodeClass.Object),
                new UANode(new NodeId(3), "Test", new NodeId("parent"), NodeClass.Object),
                new UANode(new NodeId(4), "Other", new NodeId("parent"), NodeClass.Unspecified),
            };
            var filter = new NodeFilter(raw);
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.DisplayName, node.Id, node.NodeType?.Id, nss, node.NodeClass)).ToList();

            Assert.Equal(2, matched.Count);
            Assert.Equal(2, matchedBasic.Count);

            Assert.Contains(matched, node => (uint)node.Id.Identifier == 2u);
            Assert.Contains(matched, node => (uint)node.Id.Identifier == 3u);
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
                Name = "target",
                Parent = new RawNodeFilter
                {
                    Name = "parent1"
                },
                Namespace = "test-",
                NodeClass = NodeClass.Variable
            };
            var parent1 = new UANode(new NodeId("parent1"), "parent1", NodeId.Null);
            var parent2 = new UANode(new NodeId("parent2"), "parent2", NodeId.Null);
            // Each node deviates on only one point.
            var nodes = new List<UANode>();
            for (int i = 0; i < 9; i++)
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
                var node = new UAVariable(id, i == 4 ? "not" : "target", NodeId.Null, nodeClass);
                node.Description = i == 1 ? "not" : "target";
                node.NodeType = new UANodeType(i == 2 ? new NodeId(2) : new NodeId(1), true);
                node.ArrayDimensions = i == 3 ? null : new Collection<int>(new[] { 4 });
                node.Parent = i == 5 ? parent2 : parent1;

                nodes.Add(node);
            }

            var filter = new NodeFilter(raw);
            var matched = nodes.Where(node => filter.IsMatch(node, nss)).ToList();
            var matchedBasic = nodes.Where(node =>
                filter.IsBasicMatch(node.DisplayName, node.Id, node.NodeType?.Id, nss, node.NodeClass)).ToList();

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
                new UANode(new NodeId(1), null, new NodeId("parent")),
                new UANode(new NodeId(2), "OtherTest", new NodeId("parent")),
                new UANode(new NodeId(3), "Test", new NodeId("parent")),
                new UANode(new NodeId(4), "Other", new NodeId("parent")),
            };
            var trans = new NodeTransformation(raw, 0);
            foreach (var node in nodes)
            {
                trans.ApplyTransformation(node, nss);
            }
            Assert.False(nodes[0].Ignore);
            Assert.True(nodes[1].Ignore);
            Assert.True(nodes[2].Ignore);
            Assert.False(nodes[3].Ignore);
        }
        [Fact]
        public void TestIgnoreInherit()
        {
            var raw = new RawNodeTransformation
            {
                Filter = new RawNodeFilter
                {
                    Id = "parent"
                },
                Type = TransformationType.Ignore
            };
            
            var node1 = new UANode(new NodeId("parent"), null, NodeId.Null);
            var node2 = new UANode(new NodeId(1), null, new NodeId("parent"));
            node2.Parent = node1;
            var nodes = new[]
            {
                node1, node2
            };
            var trans = new NodeTransformation(raw, 0);
            foreach (var node in nodes)
            {
                trans.ApplyTransformation(node, nss);
            }
            Assert.True(nodes[0].Ignore);
            Assert.True(nodes[1].Ignore);
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
                new UANode(new NodeId(1), null, new NodeId("parent")),
                new UANode(new NodeId(2), "OtherTest", new NodeId("parent")),
                new UANode(new NodeId(3), "Test", new NodeId("parent")),
                new UANode(new NodeId(4), "Other", new NodeId("parent")),
            };
            var trans = new NodeTransformation(raw, 0);
            foreach (var node in nodes)
            {
                trans.ApplyTransformation(node, nss);
            }
            Assert.False(nodes[0].IsProperty);
            Assert.True(nodes[1].IsProperty);
            Assert.True(nodes[2].IsProperty);
            Assert.False(nodes[3].IsProperty);
        }
        [Fact]
        public void TestPropertyInherit()
        {
            var raw = new RawNodeTransformation
            {
                Filter = new RawNodeFilter
                {
                    Id = "parent"
                },
                Type = TransformationType.Property
            };


            var node1 = new UANode(new NodeId("parent"), null, NodeId.Null);
            var node2 = new UANode(new NodeId(1), null, new NodeId("parent"));
            node2.Parent = node1;
            var nodes = new[]
            {
                node1, node2
            };
            var trans = new NodeTransformation(raw, 0);
            foreach (var node in nodes)
            {
                trans.ApplyTransformation(node, nss);
            }
            Assert.True(nodes[0].IsProperty);
            Assert.True(nodes[1].IsProperty);
        }
    }
}
