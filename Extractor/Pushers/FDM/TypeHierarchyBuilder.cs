using Cognite.OpcUa.Config;
using Cognite.OpcUa.TypeCollectors;
using Cognite.OpcUa.Types;
using CogniteSdk.Beta.DataModels;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Pushers.FDM
{
    public class FDMTypeBatch
    {
        private readonly string viewVersion;
        private readonly string space;
        private readonly ILogger log;
        public FDMTypeBatch(string viewVersion, string space, ILogger log)
        {
            this.viewVersion = viewVersion;
            this.space = space;
            this.log = log;
        }

        public Dictionary<string, ContainerCreate> Containers { get; } = new();
        public Dictionary<string, ViewCreate> Views { get; } = new();
        public Dictionary<NodeId, FullUANodeType> Types { get; } = new();
        public Dictionary<string, bool> ViewIsReferenced { get; } = new();

        public void Add(ContainerCreate container, string? baseView = null)
        {
            Containers.Add(container.Name, container);
            Views.Add(container.Name, BaseDataModelDefinitions.ViewFromContainer(container, viewVersion, baseView));
        }

        public void Add(FullUANodeType type, DMSValueConverter converter)
        {
            // If the type is in views, it and all its parents are already added
            if (Views.ContainsKey(type.Node.Name!))
            {
                if (type.Node.Id == ObjectTypeIds.BaseObjectType || type.Node.Id == VariableTypeIds.BaseVariableType)
                {
                    Types[type.Node.Id] = type;
                }
                return;
            }

            if (type.Parent == null)
            {
                log.LogWarning("Found type {Name} that is not a subtype of a base type", type.Node.Name);
                return;
            }

            Types.Add(type.Node.Id, type);

            // Only create a container if the node has properties
            bool shouldCreateContainer = type.Properties.Values.Any();

            ViewCreate view;
            if (shouldCreateContainer)
            {
                var ct = new ContainerCreate
                {
                    Description = type.Node.Attributes.Description,
                    Name = type.Node.Name,
                    ExternalId = type.ExternalId,
                    UsedFor = UsedFor.node,
                    Space = space,
                    Properties = GetContainerProperties(type, converter)
                };
                Containers.Add(ct.Name!, ct);
                view = BaseDataModelDefinitions.ViewFromContainer(ct, viewVersion, type.Parent.ExternalId);
            }
            else
            {
                var baseNodeType = type.Node.NodeClass == NodeClass.VariableType ? "BaseVariableType" : "BaseObjectType";
                view = new ViewCreate
                {
                    Description = type.Node.Attributes.Description,
                    Name = type.Node.Name,
                    ExternalId = type.ExternalId,
                    Version = viewVersion,
                    Space = space,
                    Implements = new[] { new ViewIdentifier(space, type.Parent.ExternalId, viewVersion) },
                    Properties = new Dictionary<string, ICreateViewProperty>(),
                    Filter = new NestedFilter
                    {
                        Scope = new[] { space, baseNodeType, "TypeDefinition" },
                        Filter = new ContainsAnyFilter
                        {
                            Property = new[] { space, "BaseType", "TypeHierarchy" },
                            Values = new[] { new RawPropertyValue<string>(type.Node.Id.ToString()) }
                        }
                    }
                };
            }

            ViewIsReferenced[type.Parent.ExternalId] = true;

            foreach (var rf in type.References.Values)
            {
                if (rf.ModellingRule != ModellingRule.ExposesItsArray)
                {
                    view.Properties.Add(rf.ExternalId, new ConnectionDefinition
                    {
                        Description = rf.BrowseName,
                        Name = rf.BrowseName,
                        Direction = ConnectionDirection.outwards,
                        Source = new ViewIdentifier(space, rf.Type!.ExternalId, viewVersion),
                        Type = new DirectRelationIdentifier(space, rf.Reference.Type.Id.ToString())
                    });
                    ViewIsReferenced[rf.Type!.ExternalId] = true;
                }
            }
            Views.Add(view.Name!, view);
        }

        private Dictionary<string, ContainerPropertyDefinition> GetContainerProperties(FullUANodeType type, DMSValueConverter converter)
        {
            var res = new Dictionary<string, ContainerPropertyDefinition>();
            foreach (var kvp in type.Properties)
            {
                BasePropertyType typ;
                if (kvp.Value.Node.IsRawProperty)
                {
                    typ = GetPropertyType(kvp.Value, kvp.Value.Node.IsArray);
                }
                else
                {
                    typ = BasePropertyType.Text(kvp.Value.Node.IsArray);
                }
                kvp.Value.DMSType = typ;
                res[kvp.Value.ExternalId] = new ContainerPropertyDefinition
                {
                    Description = type.Node.Attributes.Description,
                    Name = kvp.Value.BrowseName,
                    Type = typ,
                    Nullable = kvp.Value.ModellingRule != ModellingRule.Mandatory || (typ is DirectRelationPropertyType),
                    DefaultValue = (typ.Type == PropertyTypeVariant.direct || kvp.Value.Node.IsArray && typ.Type != PropertyTypeVariant.json)
                        ? null : converter.ConvertVariant(typ, kvp.Value.Node.Value)
                };
            }
            return res;
        }

        private BasePropertyType GetPropertyType(NodeTypeProperty prop, bool isArray)
        {
            if (prop.Node.FullAttributes.DataType == null)
            {
                log.LogWarning("Property {Name} has unknown datatype, falling back to JSON", prop.BrowseName);
                return BasePropertyType.Create(PropertyTypeVariant.json);
            }
            var dt = prop.Node.FullAttributes.DataType;
            if (dt.Id == DataTypeIds.Byte
                || dt.Id == DataTypeIds.SByte
                || dt.Id == DataTypeIds.UInt16
                || dt.Id == DataTypeIds.Int16
                || dt.Id == DataTypeIds.Int32) return BasePropertyType.Create(PropertyTypeVariant.int32, isArray);
            if (dt.Id == DataTypeIds.Int64
                || dt.Id == DataTypeIds.UInt32
                || dt.Id == DataTypeIds.UInt64
                || dt.Id == DataTypeIds.UInteger
                || dt.Id == DataTypeIds.Integer) return BasePropertyType.Create(PropertyTypeVariant.int64, isArray);
            if (dt.Id == DataTypeIds.Float) return BasePropertyType.Create(PropertyTypeVariant.float32, isArray);
            if (dt.Id == DataTypeIds.Double
                || dt.Id == DataTypeIds.Duration
                || !dt.IsString) return BasePropertyType.Create(PropertyTypeVariant.float64, isArray);
            if (dt.Id == DataTypeIds.LocalizedText
                || dt.Id == DataTypeIds.QualifiedName
                || dt.Id == DataTypeIds.String) return BasePropertyType.Text(isArray);
            if (dt.Id == DataTypeIds.DateTime
                || dt.Id == DataTypeIds.Date
                || dt.Id == DataTypeIds.Time
                || dt.Id == DataTypeIds.UtcTime) return BasePropertyType.Create(PropertyTypeVariant.timestamp, isArray);

            if (dt.Id == DataTypeIds.NodeId || dt.Id == DataTypeIds.ExpandedNodeId)
            {
                if (isArray) return BasePropertyType.Create(PropertyTypeVariant.json);
                return BasePropertyType.Direct(new ContainerIdentifier(space, "BaseNode"));
            }

            return BasePropertyType.Create(PropertyTypeVariant.json);
        }
    }

    public class TypeHierarchyBuilder
    {
        private readonly ILogger log;
        private readonly FullConfig config;
        private readonly NodeTypeCollector nodeTypes;
        private readonly FdmDestinationConfig fdmConfig;
        private readonly DMSValueConverter converter;
        private readonly string space;
        private readonly Dictionary<NodeId, FullUANodeType> typeMap = new();
        public TypeHierarchyBuilder(ILogger log, IUAClientAccess client, DMSValueConverter converter, FullConfig config)
        {
            this.log = log;
            this.config = config;
            nodeTypes = new NodeTypeCollector(log, config);
            space = config.Cognite!.FlexibleDataModels!.Space;
            fdmConfig = config.Cognite.FlexibleDataModels!;
            this.converter = converter;
        }

        public FDMTypeBatch ConstructTypes(NodeHierarchy nodes)
        {
            var batch = new FDMTypeBatch("1", space, log);
            // Add core containers and views
            batch.Add(BaseDataModelDefinitions.BaseNode(space));
            batch.Add(BaseDataModelDefinitions.BaseType(space), "BaseNode");
            batch.Add(BaseDataModelDefinitions.BaseVariable(space), "BaseNode");
            batch.Add(BaseDataModelDefinitions.BaseObject(space), "BaseNode");
            batch.Add(BaseDataModelDefinitions.ObjectType(space), "BaseType");
            batch.Add(BaseDataModelDefinitions.VariableType(space), "BaseType");
            batch.Add(BaseDataModelDefinitions.ReferenceType(space), "BaseType");
            batch.Add(BaseDataModelDefinitions.DataType(space), "BaseType");

            nodeTypes.MapNodeTypes(nodes);

            foreach (var kvp in nodeTypes.Types)
            {
                if (nodes.KnownTypeDefinitions.Contains(kvp.Key)
                    || fdmConfig.TypesToMap == TypesToMap.All
                    || fdmConfig.TypesToMap == TypesToMap.Custom
                    && kvp.Key.NamespaceIndex > 0)
                {
                    AddType(batch, kvp.Value);
                }
            }

            return batch;
        }

        private bool IsEventType(FullUANodeType node)
        {
            if (node.Node.Id == ObjectTypeIds.BaseEventType) return true;
            if (node.Parent != null)
            {
                return IsEventType(node.Parent);
            }
            return false;
        }

        private void AddType(FDMTypeBatch batch, FullUANodeType node)
        {
            // Skip events here
            if (IsEventType(node)) return;
            if (typeMap.ContainsKey(node.Node.Id)) return;
            typeMap.Add(node.Node.Id, node);
            batch.Add(node, converter);
            if (node.Parent != null)
            {
                AddType(batch, node.Parent);
            }
            foreach (var child in node.GetAllChildren())
            {
                if (child.Node.TypeDefinition != null && !child.Node.TypeDefinition.IsNullNodeId && nodeTypes.Types.TryGetValue(child.Node.TypeDefinition, out var childTypeDef))
                {
                    AddType(batch, childTypeDef);
                }
            }
        }
    }
}
