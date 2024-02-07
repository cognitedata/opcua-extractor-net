using System;
using System.Collections.Generic;
using System.Linq;
using Cognite.Extensions.DataModels;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Pushers.FDM.Types;
using CogniteSdk.Beta.DataModels;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace Cognite.OpcUa.Pushers.FDM
{
    public class FDMTypeBatch
    {
        private readonly FdmDestinationConfig.ModelInfo modelInfo;
        private readonly ILogger log;
        private readonly NodeIdContext context;
        public FDMTypeBatch(FdmDestinationConfig.ModelInfo modelInfo, ILogger log, NodeIdContext context)
        {
            this.modelInfo = modelInfo;
            this.log = log;
            this.context = context;
        }

        public Dictionary<string, ContainerCreate> Containers { get; } = new();
        public Dictionary<string, ViewCreate> Views { get; } = new();
        public Dictionary<NodeId, FullUANodeType> Types { get; } = new();
        public Dictionary<string, bool> ViewIsReferenced { get; } = new();

        public void Add(ContainerCreate container, string? baseView = null)
        {
            Containers.Add(container.Name, container);
            Views.Add(container.Name,
                container.ToView(modelInfo.ModelVersion,
                    baseView == null
                    ? new ViewIdentifier[0]
                    : new[] { modelInfo.ViewIdentifier(baseView) }
                )
            );
        }

        public void Add(FullUANodeType type, DMSValueConverter converter, FdmDestinationConfig config)
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
            bool shouldCreateContainer = type.Properties.Values.Count != 0;

            ViewCreate view;
            if (shouldCreateContainer)
            {
                var ct = new ContainerCreate
                {
                    Description = type.Node.Attributes.Description,
                    Name = type.Node.Name,
                    ExternalId = type.ExternalId,
                    UsedFor = UsedFor.node,
                    Space = modelInfo.ModelSpace,
                    Properties = GetContainerProperties(type, converter, config)
                };
                Containers.Add(ct.Name!, ct);
                view = ct.ToView(modelInfo.ModelVersion, modelInfo.ViewIdentifier(type.Parent.ExternalId));
            }
            else
            {
                var baseNodeType = type.Node.NodeClass == NodeClass.VariableType ? "BaseVariableType" : "BaseObjectType";
                view = new ViewCreate
                {
                    Description = type.Node.Attributes.Description,
                    Name = type.Node.Name,
                    ExternalId = type.ExternalId,
                    Version = modelInfo.ModelVersion,
                    Space = modelInfo.ModelSpace,
                    Implements = new[] { modelInfo.ViewIdentifier(type.Parent.ExternalId) },
                    Properties = new Dictionary<string, ICreateViewProperty>(),
                    Filter = new NestedFilter
                    {
                        Scope = new[] { modelInfo.ModelSpace, baseNodeType, "TypeDefinition" },
                        Filter = new ContainsAnyFilter
                        {
                            Property = new[] { modelInfo.ModelSpace, "BaseType", "TypeHierarchy" },
                            Values = new[] { new RawPropertyValue<string>(context.NodeIdToString(type.Node.Id)) }
                        }
                    }
                };
            }

            ViewIsReferenced[type.Parent.ExternalId] = true;

            foreach (var rf in type.References.Values)
            {
                if (rf.ModellingRule != ModellingRule.ExposesItsArray)
                {
                    var idf = GetViewIdentifier(rf.ExternalId, type.ExternalId, rf, config);
                    view.Properties.Add(rf.ExternalId, new ConnectionDefinition
                    {
                        Description = rf.BrowseName.Name,
                        Name = rf.BrowseName.Name,
                        Direction = ConnectionDirection.outwards,
                        Source = idf,
                        Type = new DirectRelationIdentifier(modelInfo.InstanceSpace, context.NodeIdToString(rf.Reference.Type.Id))
                    });
                    ViewIsReferenced[idf.ExternalId] = true;
                }
            }
            Views.Add(view.Name!, view);
        }

        private ViewIdentifier GetViewIdentifier(string externalId, string typeName, NodeTypeReference rf, FdmDestinationConfig config)
        {
            if (config.ConnectionTargetMap != null && config.ConnectionTargetMap.TryGetValue($"{typeName}.{externalId}", out var mapped))
            {
                return modelInfo.ViewIdentifier(mapped);
            }

            if (rf.NodeClass == NodeClass.Object || rf.NodeClass == NodeClass.Variable)
            {
                string typeExternalId;
                if (rf.Type != null)
                {
                    typeExternalId = rf.Type.ExternalId;
                }
                else
                {
                    if (rf.NodeClass == NodeClass.Object)
                    {
                        typeExternalId = "BaseObjectType";
                    }
                    else
                    {
                        typeExternalId = "BaseVariableType";
                    }
                }
                return modelInfo.ViewIdentifier(typeExternalId);
            }
            else if (rf.NodeClass == NodeClass.ObjectType)
            {
                return modelInfo.ViewIdentifier("ObjectType");
            }
            else if (rf.NodeClass == NodeClass.VariableType)
            {
                return modelInfo.ViewIdentifier("VariableType");
            }
            else if (rf.NodeClass == NodeClass.ReferenceType)
            {
                return modelInfo.ViewIdentifier("ReferenceType");
            }
            else
            {
                return modelInfo.ViewIdentifier("DataType");
            }
        }

        private Dictionary<string, ContainerPropertyDefinition> GetContainerProperties(FullUANodeType type, DMSValueConverter converter, FdmDestinationConfig config)
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
                    Name = kvp.Value.BrowseName.Name,
                    Type = typ,
                    Nullable = kvp.Value.ModellingRule != ModellingRule.Mandatory || (typ is DirectRelationPropertyType) || config.IgnoreMandatory,
                    DefaultValue = (typ.Type == PropertyTypeVariant.direct || kvp.Value.Node.IsArray && typ.Type != PropertyTypeVariant.json)
                        ? null : converter.ConvertVariant(typ, kvp.Value.Node.Value, context)
                };
            }
            return res;
        }

        private BasePropertyType GetPropertyType(DMSReferenceNode prop, bool isArray)
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
                || dt.Id == DataTypeIds.UtcTime) return BasePropertyType.Create(PropertyTypeVariant.timestamp, isArray);

            if (dt.Id == DataTypeIds.NodeId || dt.Id == DataTypeIds.ExpandedNodeId)
            {
                if (isArray) return BasePropertyType.Create(PropertyTypeVariant.json);
                return BasePropertyType.Direct(modelInfo.ContainerIdentifier("BaseNode"));
            }

            return BasePropertyType.Create(PropertyTypeVariant.json);
        }
    }

    public class TypeHierarchyBuilder
    {
        private readonly ILogger log;
        private readonly FdmDestinationConfig fdmConfig;
        private readonly DMSValueConverter converter;
        private readonly Dictionary<NodeId, FullUANodeType> typeMap = new();
        private readonly NodeIdContext context;
        private readonly FdmDestinationConfig.ModelInfo modelInfo;
        public TypeHierarchyBuilder(ILogger log, DMSValueConverter converter, FullConfig config, FdmDestinationConfig.ModelInfo modelInfo, NodeIdContext context)
        {
            this.log = log;
            this.modelInfo = modelInfo;
            fdmConfig = config.Cognite!.MetadataTargets!.DataModels!;
            this.converter = converter;
            this.context = context;
        }

        public FDMTypeBatch ConstructTypes(IReadOnlyDictionary<NodeId, FullUANodeType> types)
        {
            var batch = new FDMTypeBatch(modelInfo, log, context);
            // Add core containers and views
            batch.Add(BaseDataModelDefinitions.BaseNode(modelInfo.ModelSpace));
            batch.Add(BaseDataModelDefinitions.BaseType(modelInfo.ModelSpace), "BaseNode");
            batch.Add(BaseDataModelDefinitions.BaseVariable(modelInfo.ModelSpace), "BaseNode");
            batch.Add(BaseDataModelDefinitions.BaseObject(modelInfo.ModelSpace), "BaseNode");
            batch.Add(BaseDataModelDefinitions.ObjectType(modelInfo.ModelSpace), "BaseType");
            batch.Add(BaseDataModelDefinitions.VariableType(modelInfo.ModelSpace), "BaseType");
            batch.Add(BaseDataModelDefinitions.ReferenceType(modelInfo.ModelSpace), "BaseType");
            batch.Add(BaseDataModelDefinitions.DataType(modelInfo.ModelSpace), "BaseType");
            batch.Add(BaseDataModelDefinitions.TypeMeta(modelInfo.ModelSpace));

            foreach (var type in types.Values)
            {
                type.Build(types);
                AddType(batch, type);
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
            batch.Add(node, converter, fdmConfig);
        }
    }
}
