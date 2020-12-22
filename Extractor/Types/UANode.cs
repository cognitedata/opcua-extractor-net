/* Cognite Extractor for OPC-UA
Copyright (C) 2020 Cognite AS

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

using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Opc.Ua;

namespace Cognite.OpcUa.Types
{
    /// <summary>
    /// Represents an opcua node.
    /// </summary>
    public class UANode
    {
        /// <summary>
        /// Raw NodeId in OPC-UA.
        /// </summary>
        public NodeId Id { get; }
        /// <summary>
        /// DisplayName in OPC-UA
        /// </summary>
        public string DisplayName { get; }
        /// <summary>
        /// True if this is also a BufferedVariable.
        /// </summary>
        public bool IsVariable { get; }
        /// <summary>
        /// NodeId of the parent. May be NodeId.Null for rootNodes
        /// </summary>
        public NodeId ParentId { get; }
        /// <summary>
        /// True if the properties of this object has been read.
        /// </summary>
        public bool PropertiesRead { get; set; }
        /// <summary>
        /// Description in OPC-UA
        /// </summary>
        public string Description { get; set; }
        /// <summary>
        /// True if the node has been modified after pushing.
        /// </summary>
        public bool Changed { get; set; }
        /// <summary>
        /// Raw OPC-UA EventNotifier attribute.
        /// </summary>
        public byte EventNotifier { get; set; }
        /// <summary>
        /// OPC-UA node type
        /// </summary>
        public UANodeType NodeType { get; set; }

        /// <summary>
        /// Return a string description, for logging
        /// </summary>
        /// <returns>Descriptive string</returns>
        public virtual string ToDebugDescription()
        {
            var builder = new StringBuilder();
            builder.AppendFormat(CultureInfo.InvariantCulture, "DisplayName: {0}\n", DisplayName);
            builder.AppendFormat(CultureInfo.InvariantCulture, "Id: {0}\n", Id);
            if (ParentId != null && !ParentId.IsNullNodeId)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "ParentId: {0}\n", ParentId);
            }
            if (Description != null)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "Description: {0}\n", Description);
            }
            if (EventNotifier != 0)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "EventNotifier: {0}\n", EventNotifier);
            }
            if (NodeType != null)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "NodeType: {0}\n", NodeType.Name ?? NodeType.Id);
            }

            if (Properties != null && Properties.Any())
            {
                builder.Append("Properties: {\n");
                foreach (var prop in Properties)
                {
                    builder.AppendFormat(CultureInfo.InvariantCulture, "    {0}: {1}\n", prop.DisplayName, prop.Value?.StringValue ?? "??");
                    if (prop.Properties != null && prop.Properties.Any())
                    {
                        foreach (var prop2 in prop.Properties)
                        {
                            builder.AppendFormat(CultureInfo.InvariantCulture, "        {0}: {1}\n",
                                prop2.DisplayName, prop2.Value?.StringValue ?? "??");
                        }
                    }
                }
                builder.Append('}');
            }
            return builder.ToString();
        }
        /// <summary>
        /// Properties in OPC-UA
        /// </summary>
        public IList<UAVariable> Properties { get; set; }
        /// <param name="Id">NodeId of buffered node</param>
        /// <param name="DisplayName">DisplayName of buffered node</param>
        /// <param name="ParentId">Id of parent of buffered node</param>
        public UANode(NodeId id, string displayName, NodeId parentId) : this(id, displayName, false, parentId) { }
        /// <param name="id">NodeId of buffered node</param>
        /// <param name="displayName">DisplayName of buffered node</param>
        /// <param name="isVariable">True if this is a variable</param>
        /// <param name="parentId">Id of parent of buffered node</param>
        protected UANode(NodeId id, string displayName, bool isVariable, NodeId parentId)
        {
            Id = id;
            DisplayName = displayName;
            IsVariable = isVariable;
            ParentId = parentId;
        }
        public int GetUpdateChecksum(TypeUpdateConfig update, bool dataTypeMetadata, bool nodeTypeMetadata)
        {
            if (update == null || !update.AnyUpdate) return 0;
            int checksum = 0;
            unchecked
            {
                if (update.Context)
                {
                    checksum += (ParentId?.GetHashCode() ?? 0);
                }
                if (update.Description)
                {
                    checksum = checksum * 31 + (Description?.GetHashCode() ?? 0);
                }
                if (update.Name)
                {
                    checksum = checksum * 31 + (DisplayName?.GetHashCode() ?? 0);
                }
                if (update.Metadata)
                {
                    int metaHash = 0;
                    if (Properties != null)
                    {
                        foreach (var prop in Properties.OrderBy(prop => prop.DisplayName))
                        {
                            metaHash *= 31;
                            if (prop.DisplayName == null) continue;
                            metaHash += (prop.DisplayName, prop.Value?.StringValue).GetHashCode();
                            if (prop.Properties?.Any() ?? false)
                            {
                                metaHash += prop.GetUpdateChecksum(new TypeUpdateConfig { Metadata = true }, false, false);
                            }
                        }
                        if (dataTypeMetadata && this is UAVariable variable)
                        {
                            metaHash = metaHash * 31 + variable.DataType.Raw.GetHashCode();
                        }
                        if (nodeTypeMetadata)
                        {
                            metaHash = metaHash * 31 + (NodeType?.Id?.GetHashCode() ?? 0);
                        }
                    }
                    checksum = checksum * 31 + metaHash;
                }
            }
            return checksum;
        }
    }
}
