using Cognite.OpcUa.Config;
using Cognite.OpcUa.TypeCollectors;
using CogniteSdk;
using Opc.Ua;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.OpcUa.Nodes
{
    public abstract class BaseNodeAttributes
    {
        /// <summary>
        /// OPC-UA Description attribute
        /// </summary>
        public string? Description { get; private set; }
        /// <summary>
        /// True if this attribute collection has had its data populated at some point.
        /// </summary>
        public bool IsDataRead { get; private set; }
        /// <summary>
        /// List of properties belonging to this node.
        /// </summary>
        public IList<BaseUANode>? Properties { get; private set; }
        /// <summary>
        /// NodeClass of this node
        /// </summary>
        public NodeClass NodeClass { get; }

        public BaseNodeAttributes(NodeClass nodeClass)
        {
            NodeClass = nodeClass;
        }

        public virtual void LoadAttribute(DataValue value, uint attributeId, TypeManager typeManager)
        {
            switch (attributeId)
            {
                case Attributes.Description:
                    Description = value.GetValue<LocalizedText?>(null)?.Text;
                    break;
                default:
                    throw new InvalidOperationException($"Got unexpected unmatched attributeId, this is a bug: {attributeId}");
            }
        }

        public abstract IEnumerable<uint> GetAttributeSet(FullConfig config);

        /// <summary>
        /// Add property to list, creating the list if it does not exist.
        /// </summary>
        /// <param name="prop">Property to add</param>
        public void AddProperty(BaseUANode prop)
        {
            if (Properties == null)
            {
                Properties = new List<BaseUANode> { prop };
                return;
            }
            if (Properties.Any(oldProp => oldProp.Id == prop.Id)) return;
            Properties.Add(prop);
        }
    }

    public abstract class BaseUANode
    {
        public abstract BaseNodeAttributes Attributes { get; }

        public NodeClass NodeClass => Attributes.NodeClass;
        public IEnumerable<BaseUANode>? Properties => Attributes.Properties;

        public string? DisplayName { get; protected set; }
        public NodeId Id { get; }
        protected NodeId? FallbackParentId { get; set; }
        public NodeId ParentId => Parent?.Id ?? FallbackParentId ?? NodeId.Null;
        public BaseUANode? Parent { get; protected set; }

        public bool Ignore { get; set; }
        public bool IsRawProperty { get; set; }
        public bool IsChildOfType { get; set; }
        public bool IsProperty => IsRawProperty || IsChildOfType && NodeClass == NodeClass.Variable;
        public bool IsType =>
            NodeClass == NodeClass.ObjectType
            || NodeClass == NodeClass.VariableType
            || NodeClass == NodeClass.ReferenceType
            || NodeClass == NodeClass.DataType;
        public bool Changed { get; set; }

        public IEnumerable<BaseUANode> GetAllProperties()
        {
            if (Properties == null) return Enumerable.Empty<BaseUANode>();
            var result = new List<BaseUANode>();
            result.AddRange(Properties);
            foreach (var prop in Properties)
            {
                result.AddRange(prop.GetAllProperties());
            }
            return result;
        }

        public BaseUANode(NodeId id, string? displayName, BaseUANode? parent, NodeId? parentId)
        {
            Id = id;
            DisplayName = displayName;
            Parent = parent;
            FallbackParentId = parentId;
        }

        public virtual string? GetUniqueId(IUAClientAccess client)
        {
            return client.GetUniqueId(Id);
        }

        public virtual NodeId? TypeDefinition => null;

        public static BaseUANode? Create(ReferenceDescription node, NodeId? parentId, BaseUANode? parent, UAClient client, TypeManager typeManager)
        {
            var id = client.ToNodeId(node.NodeId);

            switch (node.NodeClass)
            {
                case NodeClass.Object:
                    return new UAObject(id, node.DisplayName?.Text, parent, parentId, typeManager.GetObjectType(client.ToNodeId(node.TypeDefinition)));
                case NodeClass.Variable:
                    return new UAVariable(id, node.DisplayName?.Text, parent, parentId, typeManager.GetVariableType(client.ToNodeId(node.TypeDefinition)));
                case NodeClass.ObjectType:
                    var objType = typeManager.GetObjectType(id);
                    objType.Initialize(node, parent, parentId);
                    return objType;
                case NodeClass.VariableType:
                    var varType = typeManager.GetVariableType(id);
                    varType.Initialize(node, parent, parentId);
                    return varType;
                case NodeClass.ReferenceType:
                    var refType = typeManager.GetReferenceType(id);
                    refType.Initialize(node, parent, parentId);
                    return refType;
                case NodeClass.DataType:
                    var dtType = typeManager.GetDataType(id);
                    dtType.Initialize(node, parent, parentId);
                    return dtType;
                default:
                    return null;
            }
        }
    }
}
