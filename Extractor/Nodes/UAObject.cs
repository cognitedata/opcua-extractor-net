using Cognite.OpcUa.Config;
using Cognite.OpcUa.TypeCollectors;
using Opc.Ua;
using System.Collections.Generic;

namespace Cognite.OpcUa.Nodes
{
    public class ObjectAttributes : BaseNodeAttributes
    {
        public byte EventNotifier { get; private set; }
        public bool? SubscribeToEventsOverride { get; set; }
        public bool? ReadEventHistoryOverride { get; set; }

        public UAObjectType TypeDefinition { get; private set; }

        public ObjectAttributes(UAObjectType objectType) : base(NodeClass.Object)
        {
            TypeDefinition = objectType;
        }

        public override IEnumerable<uint> GetAttributeSet(FullConfig config)
        {
            yield return Attributes.Description;
            if (config.Events.Enabled && config.Events.DiscoverEmitters)
            {
                yield return Attributes.EventNotifier;
            }
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
    }

    public class UAObject : BaseUANode
    {
        public UAObject(NodeId id, string? displayName, BaseUANode? parent, NodeId? parentId, UAObjectType typeDefinition)
            : base(id, displayName, parent, parentId)
        {
            FullAttributes = new ObjectAttributes(typeDefinition);
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public ObjectAttributes FullAttributes { get; }

        public override NodeId? TypeDefinition => FullAttributes.TypeDefinition?.Id;
    }
}
