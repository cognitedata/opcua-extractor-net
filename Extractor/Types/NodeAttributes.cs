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

using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Cognite.OpcUa.Types
{
    /// <summary>
    /// Class containing the base attributes of an OPC-UA node,
    /// managing its fields and properties externally, so that multiple instances
    /// of <see cref="UANode"/> can share the same actual OPC-UA node.
    /// </summary>
    public class NodeAttributes
    {
        public string Description { get; set; }
        public byte EventNotifier { get; set; }
        public UANodeType NodeType { get; set; }
        public bool IsProperty { get; set; }
        public bool Ignore { get; set; }
        public IList<UANode> Properties { get; set; }
        public NodeClass NodeClass { get; }
        public bool PropertiesRead { get; set; }
        public bool DataRead { get; set; }
        public NodeAttributes(NodeClass nc)
        {
            NodeClass = nc;
        }
        /// <summary>
        /// Retrieve the list of attribute ids to fetch.
        /// </summary>
        /// <param name="config">Active configuration</param>
        /// <returns>List of attributes</returns>
        public IEnumerable<uint> GetAttributeIds(FullConfig config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            var result = new List<uint> { Attributes.Description };
            switch (NodeClass)
            {
                case NodeClass.Object:
                    if (config.Events.Enabled)
                    {
                        result.Add(Attributes.EventNotifier);
                    }
                    break;
                case NodeClass.Variable:
                    if (config.History.Enabled)
                    {
                        result.Add(Attributes.Historizing);
                    }
                    if (config.Events.Enabled)
                    {
                        result.Add(Attributes.EventNotifier);
                    }
                    goto case NodeClass.VariableType;
                case NodeClass.VariableType:
                    result.Add(Attributes.DataType);
                    result.Add(Attributes.ValueRank);
                    if (IsProperty || config.Extraction.DataTypes.MaxArraySize != 0)
                    {
                        result.Add(Attributes.ArrayDimensions);
                    }
                    break;
            }
            return result;
        }
        /// <summary>
        /// Handle attribute read result. Should be overriden by subclasses to handle fields not found on the Object NodeClass.
        /// </summary>
        /// <param name="config">Active configuration</param>
        /// <param name="values">Full list of datavalues retrieved</param>
        /// <param name="idx">Current index in the list</param>
        /// <param name="client">UAClient this is read from</param>
        /// <returns>New index in list</returns>
        public virtual int HandleAttributeRead(FullConfig config, IList<DataValue> values, int idx, UAClient client)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));
            if (config == null) throw new ArgumentNullException(nameof(config));
            Description = values[idx++].GetValue<LocalizedText>(null)?.Text;
            if (NodeClass == NodeClass.Object && config.Events.Enabled)
            {
                EventNotifier = values[idx++].GetValue(EventNotifiers.None);
            }
            DataRead = true;
            return idx;
        }
    }
    /// <summary>
    /// Class containing the base attributes of an OPC-UA variable,
    /// managing its fields and properties externally, so that multiple instances
    /// of <see cref="UAVariable"/> can share the same actual OPC-UA node.
    /// </summary>
    public class VariableAttributes : NodeAttributes
    {
        public bool Historizing { get; set; }
        public UADataType DataType { get; set; }
        public int ValueRank { get; set; }
        public Collection<int> ArrayDimensions { get; set; }

        public VariableAttributes(NodeClass nc) : base(nc) { }
        /// <summary>
        /// Handle attribute read result for a variable.
        /// </summary>
        /// <param name="config">Active configuration</param>
        /// <param name="values">Full list of datavalues retrieved</param>
        /// <param name="idx">Current index in the list</param>
        /// <param name="client">UAClient this is read from</param>
        /// <returns>New index in list</returns>
        public override int HandleAttributeRead(FullConfig config, IList<DataValue> values, int idx, UAClient client)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (config == null) throw new ArgumentNullException(nameof(config));
            Description = values[idx++].GetValue<LocalizedText>(null)?.Text;
            if (NodeClass == NodeClass.Variable)
            {
                if (config.History.Enabled)
                {
                    Historizing = values[idx++].GetValue(false);
                }
                if (config.Events.Enabled)
                {
                    EventNotifier = values[idx++].GetValue(EventNotifiers.None);
                }
            }
            var dt = values[idx++].GetValue(NodeId.Null);
            DataType = client.DataTypeManager.GetDataType(dt) ?? new UADataType(dt);
            ValueRank = values[idx++].GetValue(ValueRanks.Any);
            if (IsProperty || config.Extraction.DataTypes.MaxArraySize != 0)
            {
                if (values[idx++].GetValue(typeof(int[])) is int[] dimVal)
                {
                    ArrayDimensions = new Collection<int>(dimVal);
                }
            }

            DataRead = true;
            return idx;
        }
    }
}
