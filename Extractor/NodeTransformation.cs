using Cognite.OpcUa.Types;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace Cognite.OpcUa
{
    public class NodeFilter
    {
        public Regex Name { get; }
        public Regex Description { get; }
        public Regex Id { get; }
        public bool? IsArray { get; }
        public Regex Namespace { get; }
        public NodeFilter Parent { get; }
        public NodeFilter(RawNodeFilter filter)
        {
            // Filter with no elements applies to everything, which may be bizarre, but that's on the user.
            if (filter == null) return;
            Name = CreateRegex(filter.Name);
            Description = CreateRegex(filter.Description);
            Id = CreateRegex(filter.Id);
            Namespace = CreateRegex(filter.Namespace);
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

        public bool IsBasicMatch(string name, NodeId id, NamespaceTable namespaces)
        {
            if (Description != null || IsArray != null || Parent != null) return false;
            return MatchBasic(name, id, namespaces);
        }

        private bool MatchBasic(string name, NodeId id, NamespaceTable namespaces)
        {
            if (Name != null && (string.IsNullOrEmpty(name) || !Name.IsMatch(name))) return false;
            if (Id != null)
            {
                var idstr = GetIdString(id);
                if (!Id.IsMatch(idstr)) return false;
            }
            if (Namespace != null && namespaces != null)
            {
                var ns = namespaces.GetString(id.NamespaceIndex);
                if (!Namespace.IsMatch(ns)) return false;
            }
            return true;
        }

        public bool IsMatch(UANode node, NamespaceTable ns)
        {
            if (!MatchBasic(node.DisplayName, node.Id, ns)) return false;
            if (Description != null && (string.IsNullOrEmpty(node.Description) || !Description.IsMatch(node.Description))) return false;
            if (IsArray != null && (!(node is UAVariable variable) || variable.IsArray != IsArray)) return false;
            if (Parent != null && (node.Parent == null || !Parent.IsMatch(node.Parent, ns))) return false;
            return true;
        }
    }

    public class NodeTransformation
    {
        public NodeFilter Filter { get; }
        public TransformationType Type { get; }
        public NodeTransformation(RawNodeTransformation raw)
        {
            Filter = new NodeFilter(raw.Filter);
            switch (raw.Type)
            {
                case "ignore":
                    Type = TransformationType.Ignore;
                    break;
                case "property":
                    Type = TransformationType.Property;
                    break;
                default:
                    throw new ConfigurationException("Unknown transformation type: " + raw.Type);
            }
        }
        public void ApplyTransformation(UANode node, NamespaceTable ns)
        {
            if (node.Parent != null)
            {
                node.Ignore |= node.Parent.Ignore;
                node.IsProperty |= node.Parent.IsProperty;
            }
            if (Filter.IsMatch(node, ns))
            {
                switch (Type)
                {
                    case TransformationType.Ignore:
                        node.Ignore = true;
                        break;
                    case TransformationType.Property:
                        node.IsProperty = true;
                        break;
                }
            }
        }
    }

    public enum TransformationType
    {
        Ignore,
        Property
    }    
}
