using Cognite.OpcUa.NodeSources;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cognite.OpcUa.Nodes
{
    public abstract class BaseUAType : BaseUANode
    {
        public BaseUAType(NodeId id, string? displayName, BaseUANode? parent, NodeId? parentId) : base(id, displayName, parent, parentId)
        {
        }

        public void Initialize(string? name, BaseUANode? parent, NodeId? parentId)
        {
            DisplayName = name;
            Parent = parent;
            FallbackParentId = parentId;
        }
    }
}
