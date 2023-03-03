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
using Opc.Ua;
using System.Collections.Generic;

namespace Cognite.OpcUa.Types
{
    /// <summary>
    /// Class containing the base attributes of an OPC-UA node,
    /// managing its fields and properties externally, so that multiple instances
    /// of <see cref="UANode"/> can share the same actual OPC-UA node.
    /// </summary>
    public class NodeAttributes
    {
        public string? Description { get; set; }
        public byte EventNotifier { get; set; }
        public UANodeType? NodeType { get; set; }
        public bool IsProperty { get; set; }
        public bool Ignore { get; set; }
        public IList<UANode>? Properties { get; set; }
        public NodeClass NodeClass { get; }
        public bool DataRead { get; set; }
        public bool ShouldSubscribeEvents { get; set; }
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
            var result = new List<uint> { Attributes.Description };
            switch (NodeClass)
            {
                case NodeClass.Object:
                    if (config.Events.Enabled && config.Events.DiscoverEmitters)
                    {
                        result.Add(Attributes.EventNotifier);
                    }
                    break;
                case NodeClass.Variable:
                    if (config.History.Enabled)
                    {
                        result.Add(Attributes.Historizing);
                    }
                    if (config.Events.Enabled && config.Events.DiscoverEmitters)
                    {
                        result.Add(Attributes.EventNotifier);
                    }
                    if (!config.Subscriptions.IgnoreAccessLevel)
                    {
                        result.Add(Attributes.UserAccessLevel);
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
        /// Call this after initializing values to calculate remaining parameters.
        /// </summary>
        /// <param name="config">Config object</param>
        public virtual void InitializeAfterRead(FullConfig config)
        {
            ShouldSubscribeEvents |= (EventNotifier & EventNotifiers.SubscribeToEvents) != 0;
        }

        /// <summary>
        /// Handle attribute read result. Should be overriden by subclasses to handle fields not found on the Object NodeClass.
        /// </summary>
        /// <param name="config">Active configuration</param>
        /// <param name="values">Full list of datavalues retrieved</param>
        /// <param name="idx">Current index in the list</param>
        /// <param name="client">UAClient this is read from</param>
        /// <returns>New index in list</returns>
        public virtual int HandleAttributeRead(
            FullConfig config,
            IList<DataValue> values,
            IEnumerable<uint> attributeIds,
            int idx,
            UAClient client)
        {
            foreach (var attr in attributeIds)
            {
                switch (attr)
                {
                    case Attributes.Description:
                        Description = values[idx].GetValue<LocalizedText?>(null)?.Text;
                        break;
                    case Attributes.EventNotifier:
                        EventNotifier = values[idx].GetValue(EventNotifiers.None);
                        break;
                }

                if (values[idx].StatusCode == StatusCodes.BadNodeIdUnknown)
                {
                    Ignore = true;
                }

                idx++;
            }

            InitializeAfterRead(config);

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
        public UADataType DataType { get; set; } = null!;
        public int ValueRank { get; set; }
        public int[]? ArrayDimensions { get; set; }
        public byte AccessLevel { get; set; }
        public bool ReadHistory { get; set; }
        public bool ShouldSubscribeData { get; set; } = true;
        public VariableAttributes(NodeClass nc) : base(nc) { }

        public override void InitializeAfterRead(FullConfig config)
        {
            if (config.Subscriptions.IgnoreAccessLevel && config.History.Enabled && config.History.Data)
            {
                ReadHistory = Historizing;
            }

            if (!config.Subscriptions.IgnoreAccessLevel)
            {
                ShouldSubscribeData = (AccessLevel & AccessLevels.CurrentRead) != 0 && config.Subscriptions.DataPoints;
                ReadHistory = (AccessLevel & AccessLevels.HistoryRead) != 0 && config.History.Enabled && config.History.Data;
            }

            if (config.Subscriptions.IgnoreAccessLevel)
            {
                ShouldSubscribeData = true;
            }

            if (config.History.RequireHistorizing)
            {
                ReadHistory &= Historizing;
            }

            base.InitializeAfterRead(config);
        }


        /// <summary>
        /// Handle attribute read result for a variable.
        /// </summary>
        /// <param name="config">Active configuration</param>
        /// <param name="values">Full list of datavalues retrieved</param>
        /// <param name="idx">Current index in the list</param>
        /// <param name="client">UAClient this is read from</param>
        /// <returns>New index in list</returns>
        public override int HandleAttributeRead(
            FullConfig config,
            IList<DataValue> values,
            IEnumerable<uint> attributeIds,
            int idx,
            UAClient client)
        {
            foreach (var attr in attributeIds)
            {
                switch (attr)
                {
                    case Attributes.Description:
                        Description = values[idx].GetValue<LocalizedText?>(null)?.Text;
                        break;
                    case Attributes.Historizing:
                        Historizing = values[idx].GetValue(false);
                        break;
                    case Attributes.EventNotifier:
                        EventNotifier = values[idx].GetValue(EventNotifiers.None);
                        break;
                    case Attributes.UserAccessLevel:
                        AccessLevel = values[idx].GetValue<byte>(0);
                        break;
                    case Attributes.DataType:
                        var dt = values[idx].GetValue(NodeId.Null);
                        DataType = client.DataTypeManager.GetDataType(dt) ?? new UADataType(dt);
                        break;
                    case Attributes.ValueRank:
                        ValueRank = values[idx].GetValue(ValueRanks.Any);
                        break;
                    case Attributes.ArrayDimensions:
                        if (values[idx].Value is int[] dimVal)
                        {
                            ArrayDimensions = dimVal;
                        }
                        break;
                }

                if (values[idx].StatusCode == StatusCodes.BadNodeIdUnknown)
                {
                    Ignore = true;
                }

                idx++;
            }

            InitializeAfterRead(config);

            DataRead = true;
            return idx;
        }
    }
}
