using Cognite.OpcUa.NodeSources;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cognite.OpcUa.Nodes
{
    public abstract class BaseUAType : BaseUANode
    {
        public BaseUAType(NodeId id, BaseUANode? parent, NodeId? parentId) : base(id, parent, parentId)
        {
        }

        public void Initialize(string? name, string? browseName, BaseUANode? parent, NodeId? parentId)
        {
            Parent = parent;
            FallbackParentId = parentId;
            Attributes.DisplayName = name;
            Attributes.BrowseName = browseName;
        }
    }
}
