using Cognite.OpcUa.Types;
using Opc.Ua;
using Serilog;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace Cognite.OpcUa
{
    public class NodeFilter
    {
        private Regex Name { get; }
        private Regex Description { get; }
        private Regex Id { get; }
        private bool? IsArray { get; }
        private NodeClass? NodeClass { get; }
        private Regex Namespace { get; }
        private Regex TypeDefinition { get; }
        private NodeFilter Parent { get; }
        public NodeFilter(RawNodeFilter filter)
        {
            // Filter with no elements applies to everything, which may be bizarre, but that's on the user.
            if (filter == null) return;
            Name = CreateRegex(filter.Name);
            Description = CreateRegex(filter.Description);
            Id = CreateRegex(filter.Id);
            Namespace = CreateRegex(filter.Namespace);
            TypeDefinition = CreateRegex(filter.TypeDefinition);
            IsArray = filter.IsArray;
            NodeClass = filter.NodeClass;
            if (filter.Parent != null)
            {
                Parent = new NodeFilter(filter.Parent);
            }
        }
        private string GetIdString(NodeId id)
        {
            var builder = new StringBuilder();
            NodeId.Format(builder, id.Identifier, id.IdType, 0);
            return builder.ToString();
        }
        private Regex CreateRegex(string raw)
        {
            if (string.IsNullOrEmpty(raw)) return null;

            return new Regex(raw, RegexOptions.Compiled | RegexOptions.Singleline | RegexOptions.CultureInvariant);
        }

        public bool IsBasicMatch(string name, NodeId id, NodeId typeDefinition, NamespaceTable namespaces, NodeClass nc)
        {
            if (Description != null || IsArray != null || Parent != null) return false;
            return MatchBasic(name, id, typeDefinition, namespaces, nc);
        }

        private bool MatchBasic(string name, NodeId id, NodeId typeDefinition, NamespaceTable namespaces, NodeClass nc)
        {
            if (Name != null && (string.IsNullOrEmpty(name) || !Name.IsMatch(name))) return false;
            if (Id != null)
            {
                if (id == null || id.IsNullNodeId) return false;
                var idstr = GetIdString(id);
                if (!Id.IsMatch(idstr)) return false;
            }
            if (Namespace != null && namespaces != null)
            {
                var ns = namespaces.GetString(id.NamespaceIndex);
                if (string.IsNullOrEmpty(ns)) return false;
                if (!Namespace.IsMatch(ns)) return false;
            }
            if (TypeDefinition != null)
            {
                if (typeDefinition == null || typeDefinition.IsNullNodeId) return false;
                var tdStr = GetIdString(typeDefinition);
                if (!TypeDefinition.IsMatch(tdStr)) return false;
            }
            if (NodeClass != null)
            {
                if (nc != NodeClass.Value) return false;
            }
            return true;
        }

        public bool IsMatch(UANode node, NamespaceTable ns)
        {
            if (!MatchBasic(node.DisplayName, node.Id, node.NodeType?.Id, ns, node.NodeClass)) return false;
            if (Description != null && (string.IsNullOrEmpty(node.Description) || !Description.IsMatch(node.Description))) return false;
            if (IsArray != null && (!(node is UAVariable variable) || variable.IsArray != IsArray)) return false;
            if (Parent != null && (node.Parent == null || !Parent.IsMatch(node.Parent, ns))) return false;
            return true;
        }
        public void Format(StringBuilder builder, int idx)
        {
            if (Name != null)
            {
                builder.Append(' ', (idx + 1) * 4);
                builder.AppendFormat("Name: {0}\n", Name);
            }
            if (Description != null)
            {
                builder.Append(' ', (idx + 1) * 4);
                builder.AppendFormat("Description: {0}\n", Description);
            }
            if (Id != null)
            {
                builder.Append(' ', (idx + 1) * 4);
                builder.AppendFormat("Id: {0}\n", Id);
            }
            if (IsArray != null)
            {
                builder.Append(' ', (idx + 1) * 4);
                builder.AppendFormat("IsArray: {0}\n", IsArray);
            }
            if (Namespace != null)
            {
                builder.Append(' ', (idx + 1) * 4);
                builder.AppendFormat("Namespace: {0}\n", Namespace);
            }
            if (Namespace != null)
            {
                builder.Append(' ', (idx + 1) * 4);
                builder.AppendFormat("TypeDefinition: {0}\n", TypeDefinition);
            }
            if (Parent != null)
            {
                builder.Append(' ', (idx + 1) * 4);
                builder.Append("Parent:\n");
                Parent.Format(builder, idx + 1);
            }
        }
        public override string ToString()
        {
            var builder = new StringBuilder();
            Format(builder, 0);
            return builder.ToString();
        }
    }

    public class NodeTransformation
    {
        public NodeFilter Filter { get; }
        public TransformationType Type { get; }
        private readonly ILogger log = Log.Logger.ForContext(typeof(UAExtractor));
        private readonly int index;
        public NodeTransformation(RawNodeTransformation raw, int index)
        {
            Filter = new NodeFilter(raw.Filter);
            Type = raw.Type;
            this.index = index;
        }
        public void ApplyTransformation(UANode node, NamespaceTable ns)
        {
            if (node.Parent != null)
            {
                node.Attributes.Ignore |= node.Parent.Ignore;
                node.Attributes.IsProperty |= node.Parent.IsProperty;
            }
            if (node.Ignore || node.IsProperty && Type == TransformationType.Property) return;
            if (Filter.IsMatch(node, ns))
            {
                switch (Type)
                {
                    case TransformationType.Ignore:
                        node.Attributes.Ignore = true;
                        log.Debug("Ignoring node {name} {id} due to matching ignore filter {idx}", node.DisplayName, node.Id, index);
                        break;
                    case TransformationType.Property:
                        node.Attributes.IsProperty = true;
                        log.Debug("Treating node {name} {id} as property due to matching filter {idx}", node.DisplayName, node.Id, index);
                        break;
                }
            }
        }
        public override string ToString()
        {
            var builder = new StringBuilder();
            builder.AppendFormat("Transformation {0}:\n", index);
            builder.AppendFormat("Type: {0}\n", Type);
            builder.Append("Filter:\n");
            Filter.Format(builder, 0);
            return builder.ToString();
        }
    }

    public enum TransformationType
    {
        Ignore,
        Property
    }    
}
