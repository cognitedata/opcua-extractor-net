using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Pushers.FDM;
using Cognite.OpcUa.Types;
using CogniteSdk.Alpha;
using CogniteSdk.DataModels;
using Opc.Ua;

namespace Cognite.OpcUa.Pushers.ILA
{
    public class EventContainerProperty
    {
        public ContainerPropertyDefinition Property { get; }
        public QualifiedNameCollection BrowsePath { get; }
        public string Name { get; }

        public EventContainerProperty(ContainerPropertyDefinition property, QualifiedNameCollection browsePath, string name)
        {
            Property = property;
            BrowsePath = browsePath;
            Name = name;
        }
    }

    public class EventContainer
    {
        public Container Container { get; }
        public UAObjectType EventType { get; }

        public Dictionary<string, EventContainerProperty> Properties { get; }

        public EventContainer? Parent { get; }

        private readonly ContainerIdentifier identifier;

        public EventContainer(Container container, UAObjectType eventType, EventContainer? parent)
        {
            Properties = new Dictionary<string, EventContainerProperty>();
            foreach (var field in eventType.CollectedFields)
            {
                var name = string.Join("_", field.BrowsePath.Select(n => FDMUtils.SanitizeExternalId(n.Name)));

                if (!container.Properties.TryGetValue(name, out var p))
                {
                    continue;
                }
                Properties.Add(name, new EventContainerProperty(p, field.BrowsePath, name));
            }
            EventType = eventType;
            Container = container;
            Parent = parent;
            identifier = new ContainerIdentifier(container.Space, container.ExternalId);
        }

        private InstanceData InstanceDataForEvent(UAEvent evt, DMSValueConverter converter, INodeIdConverter context)
        {
            // No metadata, send an empty object.
            var res = new Dictionary<string, IDMSValue>();
            if (evt.Values == null) return new InstanceData<Dictionary<string, IDMSValue>>
            {
                Source = identifier,
                Properties = res
            };

            foreach (var (name, prop) in Properties)
            {
                if (evt.Values.TryGetValue(new RawTypeField(prop.BrowsePath), out var value))
                {
                    var r = converter.ConvertVariant(prop.Property.Type, value.Value, context);
                    if (r != null)
                    {
                        res.Add(name, r);
                    }
                }
            }


            return new InstanceData<Dictionary<string, IDMSValue>>
            {
                Source = identifier,
                Properties = res
            };
        }

        public LogItem InstantiateFromEvent(UAEvent evt, string space, DMSValueConverter converter, INodeIdConverter context)
        {
            var sources = new List<InstanceData>();
            var ty = this;

            while (ty != null)
            {
                sources.Add(ty.InstanceDataForEvent(evt, converter, context));
                ty = ty.Parent;
            }

            return new LogItem
            {
                Space = space,
                Sources = sources
            };
        }
    }
}