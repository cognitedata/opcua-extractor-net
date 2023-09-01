using CogniteSdk.Beta.DataModels;
using System.Collections.Generic;

namespace Cognite.OpcUa.Pushers.FDM
{
    public static class BaseDataModelDefinitions
    {
        public static ContainerCreate BaseNode(string space)
        {
            return new ContainerCreate
            {
                Description = "Base OPC UA node type",
                ExternalId = "BaseNode",
                Name = "BaseNode",
                Space = space,
                UsedFor = UsedFor.node,
                Properties = new Dictionary<string, ContainerPropertyDefinition>
                {
                    { "NodeClass", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Create(PropertyTypeVariant.int32),
                        Name = "NodeClass",
                        Nullable = false
                    } },
                    { "DisplayName", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Text(),
                        Name = "DisplayName",
                        Nullable = true
                    } },
                    { "Description", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Text(),
                        Name = "Description",
                        Nullable = true
                    } },
                    { "BrowseName", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Text(),
                        Name = "BrowseName",
                        Nullable = true
                    } },
                    { "NodeMeta", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Create(PropertyTypeVariant.json),
                        Name = "NodeMeta",
                        Nullable = true
                    } }
                },
                Indexes = new Dictionary<string, BaseIndex>
                {
                    { "nodeClass_index", new BTreeIndex
                    {
                        IndexType = IndexType.btree,
                        Properties = new [] { "NodeClass" }
                    } }
                }
            };
        }

        public static ViewCreate ViewFromContainer(ContainerCreate container, string version, string? baseView)
        {
            var properties = new Dictionary<string, ICreateViewProperty>();
            foreach (var kvp in container.Properties)
            {
                properties[kvp.Key] = new ViewPropertyCreate
                {
                    Container = new ContainerIdentifier(container.Space, container.ExternalId),
                    Description = kvp.Value.Description,
                    Name = kvp.Value.Name,
                    ContainerPropertyIdentifier = kvp.Key,
                    Source = kvp.Value.Type is DirectRelationPropertyType dt ?
                        new ViewIdentifier(container.Space, dt.Container.ExternalId, version) : null
                };
            }

            return new ViewCreate
            {
                Description = container.Description,
                ExternalId = container.ExternalId,
                Name = container.Name,
                Space = container.Space,
                Version = version,
                Properties = properties,
                Implements = baseView is not null ? new[]
                {
                    new ViewIdentifier(container.Space, baseView, version)
                } : null
            };
        }

        public static ContainerCreate BaseVariable(string space)
        {
            return new ContainerCreate
            {
                Description = "BaseVariable type",
                ExternalId = "BaseVariableType",
                Name = "BaseVariableType",
                Space = space,
                UsedFor = UsedFor.node,
                Properties = new Dictionary<string, ContainerPropertyDefinition>
                {
                    { "DataType", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Direct(new ContainerIdentifier(space, "DataType")),
                        Name = "DataType",
                        Nullable = true
                    } },
                    { "ArrayDimensions", new ContainerPropertyDefinition
                    { 
                        Type = BasePropertyType.Create(PropertyTypeVariant.int32, true),
                        Nullable = true,
                        Name = "ArrayDimensions"
                    } },
                    { "ValueRank", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Create(PropertyTypeVariant.int32),
                        Nullable = true,
                        Name = "ValueRank"
                    } },
                    { "Value", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Create(PropertyTypeVariant.json),
                        Nullable = true,
                        Name = "Value"
                    } },
                    { "ValueTimeseries", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Text(),
                        Nullable = true,
                        Name = "ValueTimeseries"
                    } },
                    { "MinimumSamplingInterval", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Create(PropertyTypeVariant.float64),
                        Nullable = true,
                        Name = "MinimumSamplingInterval"
                    } },
                    { "TypeDefinition", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Direct(new ContainerIdentifier(space, "VariableType"))
                    } }
                },
                Indexes = new Dictionary<string, BaseIndex>
                {
                    { "typeDefinition_index", new BTreeIndex { Properties = new[] { "TypeDefinition" } } }
                }
            };
        }

        public static ContainerCreate BaseObject(string space)
        {
            return new ContainerCreate
            {
                Description = "BaseObjectType",
                ExternalId = "BaseObjectType",
                Name = "BaseObjectType",
                Space = space,
                UsedFor = UsedFor.node,
                Properties = new Dictionary<string, ContainerPropertyDefinition>
                {
                    { "EventNotifier", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Create(PropertyTypeVariant.int32)
                    } },
                    { "TypeDefinition", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Direct(new ContainerIdentifier(space, "ObjectType"))
                    } }
                },
                Indexes = new Dictionary<string, BaseIndex>
                {
                    { "typeDefinition_index", new BTreeIndex { Properties = new[] { "TypeDefinition" } } }
                }
            };
        }

        public static ContainerCreate BaseType(string space)
        {
            return new ContainerCreate
            {
                Description = "Base container for all OPC UA types",
                ExternalId = "BaseType",
                Name = "BaseType",
                Space = space,
                UsedFor = UsedFor.node,
                Properties = new Dictionary<string, ContainerPropertyDefinition>
                {
                    { "TypeHierarchy", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Text(true),
                        Nullable = false,
                        Name = "TypeHierarchy"
                    } }
                },
                Indexes = new Dictionary<string, BaseIndex>
                {
                    { "typeHierarchy_index", new BTreeIndex { Properties = new[] { "TypeHierarchy" } } }
                }
            };
        }

        public static ContainerCreate ObjectType(string space)
        {
            return new ContainerCreate
            {
                Description = "Type for OPC UA object types",
                ExternalId = "ObjectType",
                Name = "ObjectType",
                Space = space,
                UsedFor = UsedFor.node,
                Properties = new Dictionary<string, ContainerPropertyDefinition>
                {
                    { "IsAbstract", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Create(PropertyTypeVariant.boolean),
                        Nullable = false,
                        Name = "IsAbstract",
                        DefaultValue = new RawPropertyValue<bool>(false)
                    } }
                }
            };
        }

        public static ContainerCreate VariableType(string space)
        {
            return new ContainerCreate
            {
                Description = "Type for OPC UA variable types",
                ExternalId = "VariableType",
                Name = "VariableType",
                Space = space,
                UsedFor = UsedFor.node,
                Properties = new Dictionary<string, ContainerPropertyDefinition>
                {
                    { "IsAbstract", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Create(PropertyTypeVariant.boolean),
                        Nullable = false,
                        Name = "IsAbstract",
                        DefaultValue = new RawPropertyValue<bool>(false)
                    } },
                    { "DataType", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Direct(new ContainerIdentifier(space, "DataType")),
                        Name = "DataType",
                        Nullable = true
                    } },
                    { "ArrayDimensions", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Create(PropertyTypeVariant.int32, true),
                        Nullable = true,
                        Name = "ArrayDimensions"
                    } },
                    { "ValueRank", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Create(PropertyTypeVariant.int32),
                        Nullable = true,
                        Name = "ValueRank"
                    } },
                    { "Value", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Create(PropertyTypeVariant.json),
                        Nullable = true,
                        Name = "Value"
                    } },
                }
            };
        }

        public static ContainerCreate ReferenceType(string space)
        {
            return new ContainerCreate
            {
                Description = "Type for OPC UA reference types",
                ExternalId = "ReferenceType",
                Name = "ReferenceType",
                Space = space,
                UsedFor = UsedFor.node,
                Properties = new Dictionary<string, ContainerPropertyDefinition>
                {
                    { "IsAbstract", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Create(PropertyTypeVariant.boolean),
                        Nullable = false,
                        Name = "IsAbstract",
                        DefaultValue = new RawPropertyValue<bool>(false)
                    } },
                    { "InverseName", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Text(),
                        Nullable = true,
                        Name = "InverseName"
                    } },
                }
            };
        }

        public static ContainerCreate DataType(string space)
        {
            return new ContainerCreate
            {
                Description = "Type for OPC UA data types",
                ExternalId = "DataType",
                Name = "DataType",
                Space = space,
                UsedFor = UsedFor.node,
                Properties = new Dictionary<string, ContainerPropertyDefinition>
                {
                    { "IsAbstract", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Create(PropertyTypeVariant.boolean),
                        Nullable = false,
                        Name = "IsAbstract",
                        DefaultValue = new RawPropertyValue<bool>(false)
                    } },
                    { "DataTypeDefinition", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Create(PropertyTypeVariant.json),
                        Nullable = true,
                        Name = "DataTypeDefinition",
                    } }
                }
            };
        }

        public static ContainerCreate ServerMeta(string space)
        {
            return new ContainerCreate
            {
                Description = "Type for OPC UA server metadata",
                ExternalId = "ServerMeta",
                Name = "ServerMeta",
                Space = space,
                UsedFor = UsedFor.node,
                Properties = new Dictionary<string, ContainerPropertyDefinition>
                {
                    { "Namespaces", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Text(true),
                        Nullable = false
                    } }
                }
            };
        }

        public static ContainerCreate TypeMeta(string space)
        {
            return new ContainerCreate
            {
                Description = "Metadata for OPC UA types",
                ExternalId = "TypeMeta",
                Name = "TypeMeta",
                Space = space,
                UsedFor = UsedFor.node,
                Properties = new Dictionary<string, ContainerPropertyDefinition>
                {
                    { "properties", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Create(PropertyTypeVariant.json),
                        Nullable = false
                    } },
                    { "nodeId", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Text(),
                        Nullable = false
                    } },
                    { "isSimple", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Create(PropertyTypeVariant.boolean),
                        Nullable = false
                    } },
                    { "parent", new ContainerPropertyDefinition
                    {
                        Type = BasePropertyType.Text(),
                        Nullable = true
                    } }
                }
            };
        }
    }
}
