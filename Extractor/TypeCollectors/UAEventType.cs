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

using Opc.Ua;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.OpcUa.TypeCollectors
{
    /// <summary>
    /// Internal representation of node in EventType hierarchy
    /// </summary>
    internal class ChildNode
    {
        private readonly NodeClass nodeClass;
        private readonly QualifiedName browseName;
        private IList<ChildNode>? children;

        public ChildNode(QualifiedName browseName, NodeClass nc)
        {
            this.browseName = browseName;
            nodeClass = nc;
        }
        /// <summary>
        /// Add a child node to the internal collection of children
        /// </summary>
        /// <param name="desc">ReferenceDescription of child</param>
        /// <returns>Created child node</returns>
        public ChildNode AddChild(ReferenceDescription desc)
        {
            var node = new ChildNode(desc.BrowseName, desc.NodeClass);
            if (children == null)
            {
                children = new List<ChildNode> { node };
            }
            children.Add(node);
            return node;
        }
        /// <summary>
        /// Convert this node to a list of EventFields, also collects
        /// fields for all its children, and appends its own browseName to their path.
        /// </summary>
        /// <returns>Full list of fields for this node and its children</returns>
        public IEnumerable<EventField> ToFields()
        {
            if (nodeClass == NodeClass.Object && children == null) yield break;
            if (nodeClass == NodeClass.Variable)
            {
                yield return new EventField(browseName);
            }
            if (children != null)
            {
                foreach (var child in children)
                {
                    var childFields = child.ToFields();
                    foreach (var childField in childFields)
                    {
                        childField.BrowsePath.Insert(0, browseName);
                        yield return childField;
                    }
                }
            }
        }
    }
    /// <summary>
    /// Internal representation of an event type.
    /// </summary>
    public class UAEventType
    {
        public NodeId Id { get; }
        public LocalizedText DisplayName { get; }
        public UAEventType? Parent { get; set; }
        private readonly IList<ChildNode> children = new List<ChildNode>();
        public UAEventType(NodeId id, LocalizedText displayName)
        {
            Id = id;
            DisplayName = displayName;
        }

        public UAEventType(NodeId id, LocalizedText displayName, IEnumerable<EventField> fields)
        {
            Id = id;
            DisplayName = displayName;
            this.fields = new HashSet<EventField>(fields);
        }
        /// <summary>
        /// Add a child node to the internal collection of children
        /// </summary>
        /// <param name="desc">ReferenceDescription of child</param>
        /// <returns>Created child node</returns>
        internal ChildNode AddChild(ReferenceDescription desc)
        {
            var node = new ChildNode(desc.BrowseName, desc.NodeClass);
            children.Add(node);
            return node;
        }

        private HashSet<EventField>? fields;

        /// <summary>
        /// Retrieve all fields for this type, combining own fields with parent node fields.
        /// </summary>
        public HashSet<EventField> CollectedFields
        {
            get
            {
                if (fields == null)
                {
                    var lFields = children.SelectMany(child => child.ToFields());
                    if (Parent?.CollectedFields != null) lFields = Parent.CollectedFields.Concat(lFields);
                    fields = new HashSet<EventField>(lFields);
                }
                return fields;
            }
        }
    }
}
