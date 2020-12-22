using Cognite.OpcUa;
using Cognite.OpcUa.Types;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public sealed class TypesTestFixture : BaseExtractorTestFixture
    {
        public TypesTestFixture() : base(62600) { }
    }
    public class TypesTest : MakeConsoleWork, IClassFixture<TypesTestFixture>
    {
        private readonly TypesTestFixture tester;
        public TypesTest(ITestOutputHelper output, TypesTestFixture tester) : base(output)
        {
            this.tester = tester;
        }
        #region uanode
        [Theory]
        [InlineData(true, true, true, true, true)]
        [InlineData(true, true, true, true, false)]
        [InlineData(true, true, true, false, false)]
        [InlineData(true, true, false, false, false)]
        [InlineData(true, false, false, false, false)]
        [InlineData(false, true, false, false, false)]
        [InlineData(false, false, true, false, false)]
        [InlineData(false, false, false, true, true)]
        [InlineData(false, false, false, false, false)]
        [InlineData(true, false, true, false, true)]
        public void TestChecksum(bool context, bool description, bool name, bool metadata, bool ntMeta)
        {
            var update = new TypeUpdateConfig
            {
                Context = context,
                Description = description,
                Name = name,
                Metadata = metadata
            };
            int csA, csB;
            void AssertNotEqualIf(bool cond)
            {
                if (cond)
                {
                    Assert.NotEqual(csA, csB);
                }
                else
                {
                    Assert.Equal(csA, csB);
                }
            }

            var nodeA = new UANode(new NodeId("node"), null, NodeId.Null);
            var nodeB = new UANode(new NodeId("node"), null, NodeId.Null);

            (int, int) Update(UANode nodeA, UANode nodeB)
            {
                int csA = nodeA.GetUpdateChecksum(update, false, ntMeta);
                int csB = nodeB.GetUpdateChecksum(update, false, ntMeta);
                return (csA, csB);
            }

            (csA, csB) = Update(nodeA, nodeB);

            Assert.Equal(csA, csB);

            // Test name
            nodeA = new UANode(new NodeId("node"), "name", NodeId.Null);
            (csA, csB) = Update(nodeA, nodeB);
            AssertNotEqualIf(update.Name);
            nodeB = new UANode(new NodeId("node"), "name", NodeId.Null);
            (csA, csB) = Update(nodeA, nodeB);
            Assert.Equal(csA, csB);

            // Test context
            nodeA = new UANode(new NodeId("node"), "name", new NodeId("parent"));
            (csA, csB) = Update(nodeA, nodeB);
            AssertNotEqualIf(update.Context);
            nodeB = new UANode(new NodeId("node"), "name", new NodeId("parent"));
            (csA, csB) = Update(nodeA, nodeB);
            Assert.Equal(csA, csB);

            // Test description
            nodeA.Description = "description";
            nodeB.Description = "otherDesc";
            (csA, csB) = Update(nodeA, nodeB);
            AssertNotEqualIf(update.Description);
            nodeB.Description = "description";
            (csA, csB) = Update(nodeA, nodeB);
            Assert.Equal(csA, csB);


            var propA = new UAVariable(new NodeId("propA"), "propA", NodeId.Null);
            propA.SetDataPoint("valueA", DateTime.UtcNow, tester.Client);
            var propB = new UAVariable(new NodeId("propB"), "propB", NodeId.Null);
            propB.SetDataPoint("valueB", DateTime.UtcNow, tester.Client);

            var propC = new UAVariable(new NodeId("propA"), "propA", NodeId.Null);
            propC.SetDataPoint("valueA", DateTime.UtcNow, tester.Client);
            var propD = new UAVariable(new NodeId("propB"), "propB", NodeId.Null);
            propD.SetDataPoint("valueC", DateTime.UtcNow, tester.Client);

            // Test metadata
            nodeA.Properties = new List<UAVariable>
            {
                propA, propB
            };
            nodeB.Properties = new List<UAVariable>
            {
                propC, propD
            };
            (csA, csB) = Update(nodeA, nodeB);
            AssertNotEqualIf(update.Metadata);
            nodeB.Properties[1].SetDataPoint("valueB", DateTime.UtcNow, tester.Client);
            (csA, csB) = Update(nodeA, nodeB);
            Assert.Equal(csA, csB);

            // Test NodeType metadata
            nodeA.NodeType = new UANodeType(new NodeId("type"), false);
            nodeB.NodeType = new UANodeType(new NodeId("type2"), false);
            (csA, csB) = Update(nodeA, nodeB);
            AssertNotEqualIf(ntMeta && update.Metadata);
            nodeB.NodeType = new UANodeType(new NodeId("type"), false);
            (csA, csB) = Update(nodeA, nodeB);
            Assert.Equal(csA, csB);

            // Test nested metadata
            var nestProp = new UAVariable(new NodeId("nestProp"), "nestProp", NodeId.Null);
            var nestProp2 = new UAVariable(new NodeId("nestProp"), "nestProp", NodeId.Null);

            nestProp.Properties = new List<UAVariable> { propA };
            nestProp2.Properties = new List<UAVariable> { propB };
            nodeA.Properties.Add(nestProp);
            nodeB.Properties.Add(nestProp2);

            (csA, csB) = Update(nodeA, nodeB);
            AssertNotEqualIf(update.Metadata);
            nestProp2.Properties = nestProp.Properties;
            (csA, csB) = Update(nodeA, nodeB);
            Assert.Equal(csA, csB);
        }

        #endregion
    }
}
