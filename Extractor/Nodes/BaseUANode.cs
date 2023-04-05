using Cognite.OpcUa.Config;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.TypeCollectors;
using CogniteSdk;
using Opc.Ua;
using Pipelines.Sockets.Unofficial.Arenas;
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
        public bool IsDataRead { get; set; }
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

        public virtual void LoadFromSavedNode(SavedNode node, TypeManager typeManager)
        {
        }

        protected void LoadFromBaseNodeState(NodeState state)
        {
            Description = state.Description?.Text;
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
        public BaseUANode? Parent { get; set; }

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
                    objType.Initialize(node.DisplayName?.Text, parent, parentId);
                    return objType;
                case NodeClass.VariableType:
                    var varType = typeManager.GetVariableType(id);
                    varType.Initialize(node.DisplayName?.Text, parent, parentId);
                    return varType;
                case NodeClass.ReferenceType:
                    var refType = typeManager.GetReferenceType(id);
                    refType.Initialize(node.DisplayName?.Text, parent, parentId);
                    return refType;
                case NodeClass.DataType:
                    var dtType = typeManager.GetDataType(id);
                    dtType.Initialize(node.DisplayName?.Text, parent, parentId);
                    return dtType;
                default:
                    return null;
            }
        }

        public static BaseUANode? FromSavedNode(SavedNode node, TypeManager typeManager)
        {
            if (node.NodeId == null || node.NodeId.IsNullNodeId) return null;
            string? name = node.Name;
            if (name == null || node.InternalInfo == null) return null;
            // If this is an array element, we need to strip the postfix from the name, since we are treating it
            // as its parent.
            if (node.InternalInfo.ArrayDimensions != null && node.InternalInfo.Index >= 0)
            {
                var postfix = $"[{node.InternalInfo.Index}]";
                name = name.Substring(0, name.Length - postfix.Length);
            }
            var id = node.NodeId;

            BaseUANode res;
            switch (node.InternalInfo.NodeClass)
            {
                case NodeClass.Object:
                    res = new UAObject(id, name, null, node.ParentNodeId, node.InternalInfo.TypeDefinition == null
                        ? null : typeManager.GetObjectType(node.InternalInfo.TypeDefinition));
                    break;
                case NodeClass.Variable:
                    res = new UAVariable(id, name, null, node.ParentNodeId, node.InternalInfo.TypeDefinition == null
                        ? null : typeManager.GetVariableType(node.InternalInfo.TypeDefinition));
                    break;
                case NodeClass.ObjectType:
                    var objType = typeManager.GetObjectType(id);
                    objType.Initialize(name, null, node.ParentNodeId);
                    res = objType;
                    break;
                case NodeClass.VariableType:
                    var varType = typeManager.GetVariableType(id);
                    varType.Initialize(name, null, node.ParentNodeId);
                    res = varType;
                    break;
                case NodeClass.ReferenceType:
                    var refType = typeManager.GetReferenceType(id);
                    refType.Initialize(name, null, node.ParentNodeId);
                    res = refType;
                    break;
                case NodeClass.DataType:
                    var dtType = typeManager.GetDataType(id);
                    dtType.Initialize(name, null, node.ParentNodeId);
                    res = dtType;
                    break;
                default:
                    return null;
            }

            res.Attributes.LoadFromSavedNode(node, typeManager);
            return res;
        }

        public static BaseUANode? FromNodeState(NodeState node, NodeId? parentId, TypeManager typeManager)
        {
            var id = node.NodeId;
            if (node is BaseObjectState objState)
            {
                var obj = new UAObject(id, node.DisplayName?.Text, null, parentId, typeManager.GetObjectType(objState.TypeDefinitionId));
                obj.FullAttributes.LoadFromNodeState(objState);
                return obj;
            }
            if (node is BaseVariableState varState)
            {
                var vr = new UAVariable(id, node.DisplayName?.Text, null, parentId, typeManager.GetVariableType(varState.TypeDefinitionId));
                vr.FullAttributes.LoadFromNodeState(varState, typeManager);
                return vr;
            }
            if (node is BaseObjectTypeState objTState)
            {
                var objType = typeManager.GetObjectType(id);
                objType.Initialize(node.DisplayName?.Text, null, parentId);
                objType.FullAttributes.LoadFromNodeState(objTState);
                return objType;
            }
            if (node is BaseVariableTypeState varTState)
            {
                var varType = typeManager.GetVariableType(id);
                varType.Initialize(node.DisplayName?.Text, null, parentId);
                varType.FullAttributes.LoadFromNodeState(varTState, typeManager);
                return varType;
            }
            if (node is DataTypeState dataTState)
            {
                var dataType = typeManager.GetDataType(id);
                dataType.Initialize(node.DisplayName?.Text, null, parentId);
                dataType.FullAttributes.LoadFromNodeState(dataTState);
                return dataType;
            }
            if (node is ReferenceTypeState refTState)
            {
                var refType = typeManager.GetReferenceType(id);
                refType.Initialize(node.DisplayName?.Text, null, parentId);
                refType.FullAttributes.LoadFromNodeState(refTState);
                return refType;
            }

            return null;
        }

    }
}
