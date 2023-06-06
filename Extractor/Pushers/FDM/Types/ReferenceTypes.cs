using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using CogniteSdk.Beta.DataModels;
using Opc.Ua;
using System.Collections.Generic;

namespace Cognite.OpcUa.Pushers.FDM.Types
{
    public class NodeTypeReference : ReferenceNode
    {
        public NodeTypeReference(NodeClass nodeClass, string browseName, string externalId, UAReference uaReference)
            : base(nodeClass, browseName, externalId, uaReference)
        {
        }

        public FullUANodeType? Type { get; set; }
    }

    public class DMSReferenceNode : ReferenceNode
    {
        public UAVariable Node { get; set; }
        public BasePropertyType? DMSType { get; set; }
        public IEnumerable<string> BrowsePath { get; set; }
        public DMSReferenceNode(UAVariable node, UAReference reference, string externalId, IEnumerable<string> browsePath)
            : base(node.NodeClass, node.Attributes.BrowseName?.Name ?? node.Name ?? "", externalId, reference)
        {
            Node = node;
            BrowsePath = browsePath;
        }
    }
    public class ReferenceNode
    {
        public NodeClass NodeClass { get; }
        public string BrowseName { get; }
        public string ExternalId { get; }
        public UAReference Reference { get; }
        public ModellingRule ModellingRule { get; set; } = ModellingRule.Optional;

        public ReferenceNode(NodeClass nodeClass, string browseName, string externalId, UAReference uaReference)
        {
            Reference = uaReference;
            BrowseName = browseName;
            NodeClass = nodeClass;
            ExternalId = FDMUtils.SanitizeExternalId(externalId);
        }
    }
}