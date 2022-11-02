/* Cognite Extractor for OPC-UA
Copyright (C) 2022 Cognite AS

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

using Cognite.Extractor.Common;
using Cognite.OpcUa.Types;
using CogniteSdk.Beta;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;

namespace Cognite.OpcUa.Pushers.FDM
{
    internal static class FDMDataModel
    {
        public const string Space = "opcua4";
        private static readonly ModelIdentifier baseModel = new ModelIdentifier(Space, "Node");

        public static IEnumerable<ModelCreate> GetModels()
        {
            return new[]
            {
                BaseModel, ReferenceType, BaseType, BaseInstance, Variable,
                VariableType, Reference, DataType, Server
            };
        }

        private static ModelCreate BaseModel =>
            new ModelCreate
            {
                Indexes = new ModelIndexes
                {
                    BTreeIndex = new Dictionary<string, ModelIndexProperty>
                    {
                        { "nodeClass", new ModelIndexProperty { Properties = new List<string> { "nodeClass" }} }
                    }
                },
                ExternalId = "Node",
                Properties = new Dictionary<string, ModelProperty>
                {
                    { "nodeClass", new ModelProperty { Nullable = false, Type = "int32" } },
                    { "browseName", new ModelProperty { Nullable = false, Type = "text" } },
                    { "uaNodeId", new ModelProperty { Nullable = false, Type = "text" } },
                    { "name", new ModelProperty { Nullable = true, Type = "text" } },
                    { "description", new ModelProperty { Nullable = true, Type = "text" } }
                }
            };

        private static ModelCreate ReferenceType =>
            new ModelCreate
            {
                Extends = new[] { new ModelIdentifier(Space, "BaseType") },
                ExternalId = "ReferenceType",
                Properties = new Dictionary<string, ModelProperty>
                {
                    { "symmetric", new ModelProperty { Nullable = false, Type = "boolean" } },
                    { "inverseName", new ModelProperty { Nullable = true, Type = "text" } }
                }
            };

        private static ModelCreate BaseType =>
            new ModelCreate
            {
                Extends = new[] { baseModel },
                ExternalId = "BaseType",
                Properties = new Dictionary<string, ModelProperty>
                {
                    { "isAbstract", new ModelProperty { Nullable = false, Type = "boolean" } },
                    { "parents", new ModelProperty { Nullable = true, Type = "text[]" } }
                }
            };

        private static ModelCreate BaseInstance =>
            new ModelCreate
            {
                Extends = new[] { baseModel },
                ExternalId = "BaseInstance",
                Properties = new Dictionary<string, ModelProperty>
                {
                    { "typeDefinition", new ModelProperty {
                        Nullable = true,
                        Type = "direct_relation",
                        TargetModel = new ModelIdentifier(Space, "BaseType")
                    } }
                },
                Indexes = new ModelIndexes
                {
                    BTreeIndex = new Dictionary<string, ModelIndexProperty>
                    {
                        { "typeDefinition", new ModelIndexProperty { Properties = new List<string> { "typeDefinition" } } }
                    }
                }
            };
        
        private static ModelCreate Variable =>
            new ModelCreate
            {
                Extends = new[] { new ModelIdentifier(Space, "BaseInstance") },
                ExternalId = "Variable",
                Properties = new Dictionary<string, ModelProperty>
                {
                    { "value", new ModelProperty { Nullable = true, Type = "json" } },
                    // TODO: Should be actual time series reference once that is possible
                    { "timeSeries", new ModelProperty { Nullable = true, Type = "text" } },
                    { "dataType", new ModelProperty {
                        Nullable = true,
                        Type = "direct_relation",
                        TargetModel = new ModelIdentifier(Space, "DataType")
                    } },
                    { "valueRank", new ModelProperty { Nullable = false, Type = "int32" } },
                    { "arrayDimensions", new ModelProperty { Nullable = true, Type = "int32[]" } },
                    { "historizing", new ModelProperty { Nullable = true, Type = "boolean" } }
                }
            };

        private static ModelCreate VariableType =>
            new ModelCreate
            {
                Extends = new[] { new ModelIdentifier(Space, "BaseType") },
                ExternalId = "VariableType",
                Properties = new Dictionary<string, ModelProperty>
                {
                    { "value", new ModelProperty { Nullable = true, Type = "json" } },
                    { "dataType", new ModelProperty {
                        Nullable = true,
                        Type = "direct_relation",
                        TargetModel = new ModelIdentifier(Space, "DataType")
                    } },
                    { "valueRank", new ModelProperty { Nullable = false, Type = "int32" } },
                    { "arrayDimensions", new ModelProperty { Nullable = true, Type = "int32[]" } },
                }
            };

        private static ModelCreate DataType =>
            new ModelCreate
            {
                Extends = new[] { new ModelIdentifier(Space, "BaseType") },
                ExternalId = "DataType",
                Properties = new Dictionary<string, ModelProperty>
                {
                    { "dataTypeDefinition", new ModelProperty { Nullable = true, Type = "json" } }
                }
            };

        private static ModelCreate Server =>
            new ModelCreate
            {
                ExternalId = "Server",
                Properties = new Dictionary<string, ModelProperty>
                {
                    { "root", new ModelProperty {
                        Nullable = true,
                        Type = "direct_relation",
                        TargetModel = new ModelIdentifier(Space, "BaseInstance")
                    } },
                    { "namespaces", new ModelProperty { Nullable = false, Type = "text[]" } },
                    { "namespaceMap", new ModelProperty { Nullable = false, Type = "json" } },
                    { "hierarchyUpdateTimestamp", new ModelProperty { Nullable = false, Type = "timestamp" } }
                }
            };

        private static ModelCreate Reference =>
            new ModelCreate
            {
                ExternalId = "Reference",
                AllowEdge = true,
                AllowNode = false,
                Properties = new Dictionary<string, ModelProperty>
                {
                    { "isHierarchical", new ModelProperty { Type = "boolean", Nullable = false } }
                }
            };
    }

    class FDMBaseNode : BaseNode
    {
        public string? Name { get; set; }
        public string? Description { get; set; }
        public int NodeClass { get; set; }
        public string? BrowseName { get; set; }
        public string? UaNodeId { get; set; }

        public FDMBaseNode(IUAClientAccess client, UANode node)
        {
            Name = node.DisplayName;
            Description = node.Description;
            NodeClass = (int)node.NodeClass;
            BrowseName = node.BrowseName;
            UaNodeId = node.Id.ToString();
            ExternalId = client.GetUniqueId(node.Id);
        }

        public FDMBaseNode() { }
    }

    class FDMBaseType : FDMBaseNode
    {
        public bool IsAbstract { get; set; }
        public IEnumerable<string>? Parents { get; set; }

        public FDMBaseType(IUAClientAccess client, NodeId root, UANode node) : base(client, node)
        {
            var parents = new List<string?>();
            parents.Add(client.GetUniqueId(node.Id));
            while (node.Parent != null && node.Id != root)
            {
                parents.Add(client.GetUniqueId(node.Parent.Id));
                node = node.Parent;
            }
            IsAbstract = node.Attributes.TypeAttributes?.IsAbstract ?? false;
        }

        public FDMBaseType() { }
    }

    class FDMReferenceType : FDMBaseType
    {
        public bool Symmetric { get; set; }
        public string? InverseName { get; set; }

        public FDMReferenceType(IUAClientAccess client, NodeId root, UANode node) : base(client, root, node)
        {
            Symmetric = node.Attributes.TypeAttributes?.Symmetric ?? false;
            InverseName = node.Attributes.TypeAttributes?.InverseName;
        }

        public FDMReferenceType() { }
    }

    class FDMBaseInstance : FDMBaseNode
    {
        public DirectRelationIdentifier? TypeDefinition { get; set; }

        public FDMBaseInstance(string space, IUAClientAccess client, UANode node) : base(client, node)
        {
            if (node.NodeType != null)
            {
                TypeDefinition = new DirectRelationIdentifier(space, client.GetUniqueId(node.NodeType.Id));
            }
        }

        public FDMBaseInstance() { }
    }

    class FDMVariable : FDMBaseInstance
    {
        public JsonElement? Value { get; set; }
        public string? TimeSeries { get; set; }
        public DirectRelationIdentifier? DataType { get; set; }
        public int ValueRank { get; set; }
        public int[]? ArrayDimensions { get; set; }
        public bool Historizing { get; set; }

        public FDMVariable(string space, IUAClientAccess client, UANode node) : base(space, client, node)
        {
            if (node is not UAVariable variable) throw new InvalidOperationException("Node with class Variable passed as UANode");
            if (variable.IsProperty)
            {
                try
                {
                    Value = JsonDocument.Parse(client.StringConverter.ConvertToString(variable.Value, variable.DataType?.EnumValues, null, StringConverterMode.ReversibleJson)).RootElement;
                } catch
                {
                    Console.WriteLine(client.StringConverter.ConvertToString(variable.Value, variable.DataType?.EnumValues, null, StringConverterMode.ReversibleJson));
                    throw;
                }
            }
            else
            {
                TimeSeries = client.GetUniqueId(node.Id);
            }

            if (variable.DataType?.Raw != null && !variable.DataType.Raw.IsNullNodeId)
                DataType = new DirectRelationIdentifier(space, client.GetUniqueId(variable.DataType?.Raw));
            ValueRank = variable.ValueRank;
            ArrayDimensions = variable.ArrayDimensions;
            Historizing = variable.VariableAttributes.Historizing;
        }

        public FDMVariable() { }
    }

    class FDMVariableType : FDMBaseType
    {
        public JsonElement? Value { get; set; }
        public DirectRelationIdentifier? DataType { get; set; }
        public int ValueRank { get; set; }
        public int[]? ArrayDimensions { get; set; }

        public FDMVariableType(string space, IUAClientAccess client, NodeId root, UANode node) : base(client, root, node)
        {
            if (node is not UAVariable variable) throw new InvalidOperationException("Node with class VariableType passed as UANode");
            Value = JsonDocument.Parse(client.StringConverter.ConvertToString(variable.Value, variable.DataType?.EnumValues, null, StringConverterMode.ReversibleJson)).RootElement;

            if (variable.DataType?.Raw != null && !variable.DataType.Raw.IsNullNodeId)
                DataType = new DirectRelationIdentifier(space, client.GetUniqueId(variable.DataType?.Raw));
            ValueRank = variable.ValueRank;
            ArrayDimensions = variable.ArrayDimensions;
        }

        public FDMVariableType() { }
    }

    class FDMDataType : FDMBaseType
    {
        public JsonElement? DataTypeDefinition { get; set; }

        public FDMDataType(IUAClientAccess client, NodeId root, UANode node) : base(client, root, node)
        {
            DataTypeDefinition = JsonDocument.Parse(client.StringConverter.ConvertToString(node.Attributes.TypeAttributes!.DataTypeDefinition, null, null, StringConverterMode.ReversibleJson)).RootElement;
        }

        public FDMDataType() { }
    }

    class FDMServer : BaseNode
    {
        public DirectRelationIdentifier? Root { get; set; }
        public string[]? Namespaces { get; set; }
        public Dictionary<string, string>? NamespaceMap { get; set; }
        public string? HierarchyUpdateTimestamp { get; set; }

        public FDMServer(string space, string prefix, IUAClientAccess client, NodeId root, NamespaceTable namespaces, Dictionary<string, string> namespaceMap)
        {
            HierarchyUpdateTimestamp = DateTime.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss.fffK", CultureInfo.InvariantCulture);
            Namespaces = namespaces.ToArray();
            NamespaceMap = Namespaces.ToDictionary(a => a, a => namespaceMap.GetValueOrDefault(a) ?? a);
            ExternalId = $"{prefix}ServerMetadata";
            Root = new DirectRelationIdentifier(space, client.GetUniqueId(root));
        }

        public FDMServer() { }
    }

    class FDMReference : BaseEdge
    {
        public bool IsHierarchical { get; set; }

        public FDMReference(string prefix, string space, IUAClientAccess client, UAReference reference)
        {
            IsHierarchical = reference.IsHierarchical;
            Type = new DirectRelationIdentifier(space, client.GetUniqueId(reference.Type?.Id ?? NodeId.Null));
            StartNode = new DirectRelationIdentifier(space, client.GetUniqueId(reference.Source.Id));
            EndNode = new DirectRelationIdentifier(space, client.GetUniqueId(reference.Target.Id));
            ExternalId = $"{prefix}{reference.Type?.Id?.ToString()};{reference.Source.Id};{reference.Target.Id}";
        }

        public FDMReference() { }
    }
}
