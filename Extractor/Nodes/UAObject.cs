using Cognite.OpcUa.Config;
using Opc.Ua;
using System.Collections.Generic;

namespace Cognite.OpcUa.Nodes
{
    public class ObjectAttributes : BaseNodeAttributes
    {
        public byte EventNotifier { get; private set; }
        public bool SubscribeToEventsOverride { get; set; }
        public ObjectAttributes() : base(NodeClass.Object)
        {
        }

        public override IEnumerable<uint> GetAttributeSet(FullConfig config)
        {
            yield return Attributes.Description;
            if (config.Events.Enabled && config.Events.DiscoverEmitters)
            {
                yield return Attributes.EventNotifier;
            }
        }

        public override void LoadAttribute(DataValue value, uint attributeId)
        {
            switch (attributeId)
            {
                case Attributes.EventNotifier:
                    EventNotifier = value.GetValue(EventNotifiers.None);
                    break;
                default:
                    base.LoadAttribute(value, attributeId);
                    break;
            }
        }

        public bool ShouldSubscribeToEvents =>
            SubscribeToEventsOverride || (EventNotifier & EventNotifiers.SubscribeToEvents) != 0;
    }

    public class UAObject : BaseUANode
    {
        public UAObject(NodeId id, string displayName, NodeId parentId) : base(id, displayName, parentId)
        {
            FullAttributes = new ObjectAttributes();
        }

        public override BaseNodeAttributes Attributes => FullAttributes;
        public ObjectAttributes FullAttributes { get; }
    }
}
