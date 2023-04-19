/* Cognite Extractor for OPC-UA
Copyright (C) 2021 Cognite AS

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. */

using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System.Text;
using System.Text.RegularExpressions;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Class used to apply a complex filter to nodes.
    /// </summary>
    public class NodeFilter
    {
        private Regex? Name { get; }
        private Regex? Description { get; }
        private Regex? Id { get; }
        private bool? IsArray { get; }
        private NodeClass? NodeClass { get; }
        private Regex? Namespace { get; }
        private Regex? TypeDefinition { get; }
        private NodeFilter? Parent { get; }
        private bool? Historizing { get; }
        public NodeFilter(RawNodeFilter? filter)
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
            Historizing = filter.Historizing;
            if (filter.Parent != null)
            {
                Parent = new NodeFilter(filter.Parent);
            }
        }

        /// <summary>
        /// Return a representation if the identifier of <paramref name="id"/>, 
        /// on the form i=123, or s=string, etc.
        /// </summary>
        /// <param name="id">Identifier to get representation of</param>
        /// <returns>String representation of identifier of <paramref name="id"/></returns>
        private static string GetIdString(NodeId id)
        {
            var builder = new StringBuilder();
            NodeId.Format(builder, id.Identifier, id.IdType, 0);
            return builder.ToString();
        }

        /// <summary>
        /// Create regex from configured string.
        /// </summary>
        /// <param name="raw">Raw string to create regex for.</param>
        /// <returns>Created regex.</returns>
        private static Regex? CreateRegex(string? raw)
        {
            if (string.IsNullOrEmpty(raw)) return null;

            return new Regex(raw, RegexOptions.Compiled | RegexOptions.Singleline | RegexOptions.CultureInvariant);
        }

        /// <summary>
        /// Test for match using only basic properties available in when reading from the server.
        /// Will always return false if there are filters on not yet available fields.
        /// </summary>
        /// <param name="name">DisplayName</param>
        /// <param name="id">Raw NodeId</param>
        /// <param name="typeDefinition">TypeDefinition Id</param>
        /// <param name="namespaces">Source namespacetable</param>
        /// <param name="nc">NodeClass</param>
        /// <returns>True if match</returns>
        public bool IsBasicMatch(string name, NodeId id, NodeId typeDefinition, NamespaceTable namespaces, NodeClass nc)
        {
            if (Description != null || IsArray != null || Parent != null || Historizing != null) return false;
            return MatchBasic(name, id ?? NodeId.Null, typeDefinition, namespaces, nc);
        }
        /// <summary>
        /// Test for match using only basic properties available in when reading from the server.
        /// </summary>
        /// <param name="name">DisplayName</param>
        /// <param name="id">Raw NodeId</param>
        /// <param name="typeDefinition">TypeDefinition Id</param>
        /// <param name="namespaces">Source namespacetable</param>
        /// <param name="nc">NodeClass</param>
        /// <returns>True if match</returns>
        private bool MatchBasic(string? name, NodeId id, NodeId? typeDefinition, NamespaceTable namespaces, NodeClass nc)
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
        /// <summary>
        /// Return true if the given node matches the filter.
        /// </summary>
        /// <param name="node">Node to test</param>
        /// <param name="ns">Currently active namespace table</param>
        /// <returns>True if match</returns>
        public bool IsMatch(BaseUANode node, NamespaceTable ns)
        {
            if (node == null || !MatchBasic(node.Name, node.Id, node.TypeDefinition, ns, node.NodeClass)) return false;
            if (Description != null && (string.IsNullOrEmpty(node.Attributes.Description) || !Description.IsMatch(node.Attributes.Description))) return false;
            if (node is UAVariable variable)
            {
                if (IsArray != null && variable.IsArray != IsArray) return false;
                if (Historizing != null && variable.FullAttributes.Historizing != Historizing) return false;
            }
            else if (IsArray != null || Historizing != null) return false;
            if (Parent != null && (node.Parent == null || !Parent.IsMatch(node.Parent, ns))) return false;
            return true;
        }
        /// <summary>
        /// Create string representation, for logging.
        /// </summary>
        /// <param name="builder">StringBuilder to write to</param>
        /// <param name="idx">Level of nesting, for clean indentation.</param>
        public void Format(StringBuilder builder, int idx)
        {
            if (Name != null)
            {
                builder.Append(' ', (idx + 1) * 4);
                builder.AppendFormat("Name: {0}", Name);
                builder.AppendLine();
            }
            if (Description != null)
            {
                builder.Append(' ', (idx + 1) * 4);
                builder.AppendFormat("Description: {0}", Description);
                builder.AppendLine();
            }
            if (Id != null)
            {
                builder.Append(' ', (idx + 1) * 4);
                builder.AppendFormat("Id: {0}", Id);
                builder.AppendLine();
            }
            if (IsArray != null)
            {
                builder.Append(' ', (idx + 1) * 4);
                builder.AppendFormat("IsArray: {0}", IsArray);
                builder.AppendLine();
            }
            if (Historizing != null)
            {
                builder.Append(' ', (idx + 1) * 4);
                builder.AppendFormat("Historizing: {0}", Historizing);
                builder.AppendLine();
            }
            if (Namespace != null)
            {
                builder.Append(' ', (idx + 1) * 4);
                builder.AppendFormat("Namespace: {0}", Namespace);
                builder.AppendLine();
            }
            if (TypeDefinition != null)
            {
                builder.Append(' ', (idx + 1) * 4);
                builder.AppendFormat("TypeDefinition: {0}", TypeDefinition);
                builder.AppendLine();
            }
            if (NodeClass != null)
            {
                builder.Append(' ', (idx + 1) * 4);
                builder.AppendFormat("NodeClass: {0}", NodeClass);
                builder.AppendLine();
            }
            if (Parent != null)
            {
                builder.Append(' ', (idx + 1) * 4);
                builder.Append("Parent:");
                builder.AppendLine();
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

    /// <summary>
    /// Describes a transformation to the source hierarchy. Consists of a filter and a transformation type.
    /// </summary>
    public class NodeTransformation
    {
        public NodeFilter Filter { get; }
        public TransformationType Type { get; }
        private readonly int index;
        public NodeTransformation(RawNodeTransformation raw, int index)
        {
            Filter = new NodeFilter(raw.Filter);
            Type = raw.Type;
            this.index = index;
        }

        private bool ShouldSkip(BaseUANode node, FullConfig config)
        {
            // No reason to transform ignored nodes.
            if (node == null || node.Ignore) return true;

            switch (Type)
            {
                case TransformationType.Property:
                    // Already a property
                    return node.IsRawProperty;
                case TransformationType.TimeSeries:
                    // No need to transform to timeseries if node is not property, or node cannot be a timeseries.
                    return !node.IsRawProperty || node.NodeClass != NodeClass.Variable;
                case TransformationType.DropSubscriptions:
                    // No need to drop subscriptions if we are not subscribing to this node.
                    return !(node is UAVariable variable && variable.FullAttributes.ShouldSubscribe(config))
                        && !(node is UAObject obj && obj.FullAttributes.ShouldSubscribeToEvents(config));
                case TransformationType.AsEvents:
                    // If not a variable or already publishing values as events we skip here.
                    return node is not UAVariable evtvariable || evtvariable.AsEvents;
            }

            return false;
        }

        /// <summary>
        /// Modify the given node if it passes the filter.
        /// </summary>
        /// <param name="node">Node to test</param>
        /// <param name="ns">Active NamespaceTable</param>
        public void ApplyTransformation(ILogger log, BaseUANode node, NamespaceTable ns, FullConfig config)
        {
            if (ShouldSkip(node, config)) return;

            if (Filter.IsMatch(node, ns))
            {
                switch (Type)
                {
                    case TransformationType.Ignore:
                        node.Ignore = true;
                        log.LogTrace("Ignoring node {Name} {Id} due to matching ignore filter {Idx}", node.Name, node.Id, index);
                        break;
                    case TransformationType.Property:
                        node.IsRawProperty = true;
                        log.LogTrace("Treating node {Name} {Id} as property due to matching filter {Idx}", node.Name, node.Id, index);
                        break;
                    case TransformationType.DropSubscriptions:
                        if (node is UAVariable variable)
                        {
                            variable.FullAttributes.ShouldSubscribeOverride = false;
                        }
                        else if (node is UAObject obj)
                        {
                            obj.FullAttributes.SubscribeToEventsOverride = false;
                        }
                        log.LogDebug("Dropping subscriptions on node {Name} {Id} due to matching filter {Idx}", node.Name, node.Id, index);
                        break;
                    case TransformationType.TimeSeries:
                        node.IsRawProperty = false;
                        log.LogTrace("Treating node {Name} {Id} as timeseries due to matching filter {Idx}", node.Name, node.Id, index);
                        break;
                    case TransformationType.AsEvents:
                        if (node is UAVariable evtvariable)
                        {
                            evtvariable.AsEvents = true;
                        }
                        break;
                }
            }
        }
        public override string ToString()
        {
            var builder = new StringBuilder();
            builder.AppendFormat("Transformation {0}:", index);
            builder.AppendLine();
            builder.AppendFormat("Type: {0}", Type);
            builder.AppendLine();
            builder.Append("Filter:");
            builder.AppendLine();
            Filter.Format(builder, 0);
            return builder.ToString();
        }
    }

    public enum TransformationType
    {
        Ignore,
        Property,
        DropSubscriptions,
        TimeSeries,
        AsEvents
    }
}
