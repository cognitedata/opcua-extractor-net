/* Cognite Extractor for OPC-UA
Copyright (C) 2023 Cognite AS

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
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Opc.Ua;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Cognite.OpcUa.Nodes
{
    public class ObjectAttributes : BaseNodeAttributes
    {
        public byte EventNotifier { get; set; }
        public bool? SubscribeToEventsOverride { get; set; }
        public bool? ReadEventHistoryOverride { get; set; }

        public UAObjectType? TypeDefinition { get; set; }

        public ObjectAttributes(UAObjectType? objectType) : base(NodeClass.Object)
        {
            TypeDefinition = objectType;
        }

        public override IEnumerable<uint> GetAttributeSet(FullConfig config)
        {
            if (config.Events.Enabled && config.Events.DiscoverEmitters)
            {
                yield return Attributes.EventNotifier;
            }
            foreach (var attr in base.GetAttributeSet(config)) yield return attr;
        }

        public override void LoadAttribute(DataValue value, uint attributeId, TypeManager typeManager)
        {
            switch (attributeId)
            {
                case Attributes.EventNotifier:
                    EventNotifier = value.GetValue(EventNotifiers.None);
                    break;
                default:
                    base.LoadAttribute(value, attributeId, typeManager);
                    break;
            }
        }

        public bool ShouldSubscribeToEvents(FullConfig config)
        {
            if (!config.Events.Enabled || !config.Subscriptions.Events) return false;
            if (SubscribeToEventsOverride != null) return SubscribeToEventsOverride.Value;
            return (EventNotifier & EventNotifiers.SubscribeToEvents) != 0;
        }

        public bool ShouldReadEventHistory(FullConfig config)
        {
            if (!config.Events.Enabled || !config.History.Enabled || !config.Events.History) return false;
            if (ReadEventHistoryOverride != null) return ReadEventHistoryOverride.Value;
            return (EventNotifier & EventNotifiers.HistoryRead) != 0;
        }

        public override void LoadFromSavedNode(SavedNode node, TypeManager typeManager)
        {
            EventNotifier = node.InternalInfo!.EventNotifier;
            SubscribeToEventsOverride = node.InternalInfo.ShouldSubscribeEvents;
            ReadEventHistoryOverride = node.InternalInfo.ShouldReadHistoryEvents;

            base.LoadFromSavedNode(node, typeManager);
        }

        public void LoadFromNodeState(BaseObjectState state)
        {
            EventNotifier = state.EventNotifier;
            LoadFromBaseNodeState(state);
        }
    }

    public class UAObject : BaseUANode
    {
        public UAObject(NodeId id, string? displayName, QualifiedName? browseName, BaseUANode? parent, NodeId? parentId, UAObjectType? typeDefinition)
            : base(id, parent, parentId)
        {
            FullAttributes = new ObjectAttributes(typeDefinition);
            Attributes.DisplayName = displayName;
            Attributes.BrowseName = browseName;
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public ObjectAttributes FullAttributes { get; }

        public override NodeId? TypeDefinition => FullAttributes.TypeDefinition?.Id;

        public override Dictionary<string, string>? GetExtraMetadata(FullConfig config, SessionContext context, TypeConverter converter)
        {
            if (config.Extraction.NodeTypes.Metadata && FullAttributes.TypeDefinition?.Name != null)
            {
                return new Dictionary<string, string> { { "TypeDefinition", FullAttributes.TypeDefinition.Name } };
            }
            return null;
        }

        public override int GetUpdateChecksum(bool nodeTypeMetadata)
        {
            int checksum = base.GetUpdateChecksum(nodeTypeMetadata);
            unchecked
            {
                if (nodeTypeMetadata)
                {
                    checksum = checksum * 31 + (FullAttributes.TypeDefinition?.Id?.GetHashCode() ?? 0);
                }
            }
            return checksum;
        }

        public override void Format(StringBuilder builder, int indent, bool writeParent = true, bool writeProperties = true)
        {
            builder.AppendFormat(CultureInfo.InvariantCulture, "{0}Object: {1}", new string(' ', indent), Name);
            builder.AppendLine();
            base.Format(builder, indent + 4, writeParent);

            var indt = new string(' ', indent + 4);
            if (FullAttributes.EventNotifier != 0)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "{0}EventNotifier: {1}", indt, FullAttributes.EventNotifier);
                builder.AppendLine();
            }
            FullAttributes.TypeDefinition?.Format(builder, indent + 4, false, false);
        }
    }
}
