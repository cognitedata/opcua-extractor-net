using Cognite.OpcUa.Config;
using Cognite.OpcUa.NodeSources;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using Opc.Ua;
using System.Collections.Generic;

namespace Cognite.OpcUa.Nodes
{
    public class ObjectAttributes : BaseNodeAttributes
    {
        public byte EventNotifier { get; private set; }
        public bool? SubscribeToEventsOverride { get; set; }
        public bool? ReadEventHistoryOverride { get; set; }

        public UAObjectType? TypeDefinition { get; private set; }

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

        public override Dictionary<string, string>? GetExtraMetadata(FullConfig config, IUAClientAccess client)
        {
            if (config.Extraction.NodeTypes.Metadata && FullAttributes.TypeDefinition?.Attributes?.DisplayName != null)
            {
                return new Dictionary<string, string> { { "TypeDefinition", FullAttributes.TypeDefinition.Attributes.DisplayName } };
            }
            return null;
        }
    }
}
