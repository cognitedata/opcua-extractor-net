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
using CogniteSdk;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace Cognite.OpcUa.Nodes
{
    public abstract class BaseNodeAttributes
    {
        /// <summary>
        /// BrowseName attribute
        /// </summary>
        public QualifiedName? BrowseName { get; set; }
        /// <summary>
        /// DisplayName attribute
        /// </summary>
        public string? DisplayName { get; set; }
        /// <summary>
        /// OPC-UA Description attribute
        /// </summary>
        public string? Description { get; set; }
        /// <summary>
        /// True if this attribute collection has had its data populated at some point.
        /// </summary>
        public bool IsDataRead { get; set; }
        /// <summary>
        /// List of properties belonging to this node.
        /// </summary>
        public IList<BaseUANode>? Properties { get; set; }
        /// <summary>
        /// NodeClass of this node
        /// </summary>
        public NodeClass NodeClass { get; }

        public BaseNodeAttributes(NodeClass nodeClass)
        {
            NodeClass = nodeClass;
        }

        public virtual void LoadAttribute(DataValue value, uint attributeId, TypeManager typeManager)
        {
            switch (attributeId)
            {
                case Attributes.Description:
                    Description = value.GetValue<LocalizedText?>(null)?.Text;
                    break;
                case Attributes.DisplayName:
                    DisplayName = value.GetValue<LocalizedText?>(null)?.Text;
                    break;
                case Attributes.BrowseName:
                    BrowseName = value.GetValue<QualifiedName?>(null);
                    break;
                default:
                    throw new InvalidOperationException($"Got unexpected unmatched attributeId, this is a bug: {attributeId}");
            }
        }

        public virtual IEnumerable<uint> GetAttributeSet(FullConfig config)
        {
            yield return Attributes.Description;
            if (DisplayName == null) yield return Attributes.DisplayName;
            if (BrowseName == null) yield return Attributes.BrowseName;
        }

        /// <summary>
        /// Add property to list, creating the list if it does not exist.
        /// </summary>
        /// <param name="prop">Property to add</param>
        public void AddProperty(BaseUANode prop)
        {
            if (Properties == null)
            {
                Properties = new List<BaseUANode> { prop };
                return;
            }
            if (Properties.Any(oldProp => oldProp.Id == prop.Id)) return;
            Properties.Add(prop);
        }

        public virtual void LoadFromSavedNode(SavedNode node, TypeManager typeManager)
        {
        }

        protected void LoadFromBaseNodeState(NodeState state)
        {
            Description = state.Description?.Text;
        }
    }

    public abstract class BaseUANode
    {
        public abstract BaseNodeAttributes Attributes { get; }

        public NodeClass NodeClass => Attributes.NodeClass;
        public IEnumerable<BaseUANode>? Properties => Attributes.Properties;
        public NodeId Id { get; }
        protected NodeId? FallbackParentId { get; set; }
        public NodeId ParentId => Parent?.Id ?? FallbackParentId ?? NodeId.Null;
        public BaseUANode? Parent { get; set; }
        public NodeSource Source { get; set; } = NodeSource.OPCUA;
        public virtual string? Name => Attributes.DisplayName;

        public bool Ignore { get; set; }
        public bool IsRawProperty { get; set; }
        public bool IsChildOfType { get; set; }
        public bool IsProperty => IsRawProperty || IsChildOfType && NodeClass == NodeClass.Variable;
        public bool IsType =>
            NodeClass == NodeClass.ObjectType
            || NodeClass == NodeClass.VariableType
            || NodeClass == NodeClass.ReferenceType
            || NodeClass == NodeClass.DataType;
        public bool Changed { get; set; }

        public IEnumerable<BaseUANode> GetAllProperties()
        {
            if (Properties == null) return Enumerable.Empty<BaseUANode>();
            var result = new List<BaseUANode>();
            result.AddRange(Properties);
            foreach (var prop in Properties)
            {
                result.AddRange(prop.GetAllProperties());
            }
            return result;
        }

        public BaseUANode(NodeId id, BaseUANode? parent, NodeId? parentId)
        {
            Id = id;
            Parent = parent;
            FallbackParentId = parentId;
        }

        public IEnumerable<BaseUANode> EnumerateAncestors()
        {
            var parent = Parent;
            while (parent != null)
            {
                yield return parent;
                parent = parent.Parent;
            }
        }

        public IEnumerable<T> EnumerateTypedAncestors<T>() where T : BaseUANode
        {
            var parent = Parent;
            while (parent != null && parent is T tP)
            {
                yield return tP;
                parent = parent.Parent;
            }
        }

        public virtual string? GetUniqueId(IUAClientAccess client)
        {
            return client.GetUniqueId(Id);
        }

        public virtual bool AllowValueRead(ILogger logger, DataTypeConfig config)
        {
            return false;
        }

        public virtual void Format(StringBuilder builder, int indent, bool writeParent = true, bool writeProperties = true)
        {
            var indt = new string(' ', indent);
            //builder.AppendFormat(CultureInfo.InvariantCulture, "{0}Variable: {1}", indt, Name);
            //builder.AppendLine();
            builder.AppendFormat(CultureInfo.InvariantCulture, "{0}Id: {1}", indt, Id);
            builder.AppendLine();
            if (Parent != null && writeParent)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "{0}Parent: {1} {2}", indt, Parent.Name, Parent.Id);
                builder.AppendLine();
            }
            else if (writeParent && !ParentId.IsNullNodeId)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "{0}Parent: {1}", indt, ParentId);
                builder.AppendLine();
            }
            if (Attributes.Description != null)
            {
                builder.AppendFormat(CultureInfo.InvariantCulture, "{0}Description: {1}", indt, Attributes.Description);
                builder.AppendLine();
            }
            if (Properties != null && Properties.Any() && writeProperties)
            {
                builder.AppendFormat("{0}Properties:", indt);
                builder.AppendLine();
                foreach (var prop in Properties)
                {
                    prop.Format(builder, indent + 4, false);
                }
            }
        }

        public override string ToString()
        {
            var builder = new StringBuilder();
            Format(builder, 0);
            return builder.ToString();
        }

        public virtual NodeId? TypeDefinition => null;

        public static BaseUANode? Create(ReferenceDescription node, NodeId? parentId, BaseUANode? parent, UAClient client, TypeManager typeManager)
        {
            var id = client.ToNodeId(node.NodeId);

            switch (node.NodeClass)
            {
                case NodeClass.Object:
                    return new UAObject(id, node.DisplayName?.Text, node.BrowseName, parent, parentId, typeManager.GetObjectType(client.ToNodeId(node.TypeDefinition)));
                case NodeClass.Variable:
                    return new UAVariable(id, node.DisplayName?.Text, node.BrowseName, parent, parentId, typeManager.GetVariableType(client.ToNodeId(node.TypeDefinition)));
                case NodeClass.ObjectType:
                    var objType = typeManager.GetObjectType(id);
                    objType.Initialize(node.DisplayName?.Text, node.BrowseName, parent, parentId);
                    return objType;
                case NodeClass.VariableType:
                    var varType = typeManager.GetVariableType(id);
                    varType.Initialize(node.DisplayName?.Text, node.BrowseName, parent, parentId);
                    return varType;
                case NodeClass.ReferenceType:
                    var refType = typeManager.GetReferenceType(id);
                    refType.Initialize(node.DisplayName?.Text, node.BrowseName, parent, parentId);
                    return refType;
                case NodeClass.DataType:
                    var dtType = typeManager.GetDataType(id);
                    dtType.Initialize(node.DisplayName?.Text, node.BrowseName, parent, parentId);
                    return dtType;
                default:
                    return null;
            }
        }

        public static BaseUANode? FromSavedNode(SavedNode node, TypeManager typeManager)
        {
            if (node.NodeId == null || node.NodeId.IsNullNodeId) return null;
            string? name = node.Name;
            if (name == null || node.InternalInfo == null) return null;
            // If this is an array element, we need to strip the postfix from the name, since we are treating it
            // as its parent.
            if (node.InternalInfo.ArrayDimensions != null && node.InternalInfo.Index >= 0)
            {
                var postfix = $"[{node.InternalInfo.Index}]";
                name = name.Substring(0, name.Length - postfix.Length);
            }
            var id = node.NodeId;

            BaseUANode res;
            switch (node.InternalInfo.NodeClass)
            {
                case NodeClass.Object:
                    res = new UAObject(id, name, null, null, node.ParentNodeId, node.InternalInfo.TypeDefinition == null
                        ? null : typeManager.GetObjectType(node.InternalInfo.TypeDefinition));
                    break;
                case NodeClass.Variable:
                    res = new UAVariable(id, name, null, null, node.ParentNodeId, node.InternalInfo.TypeDefinition == null
                        ? null : typeManager.GetVariableType(node.InternalInfo.TypeDefinition));
                    break;
                case NodeClass.ObjectType:
                    var objType = typeManager.GetObjectType(id);
                    objType.Initialize(name, null, null, node.ParentNodeId);
                    res = objType;
                    break;
                case NodeClass.VariableType:
                    var varType = typeManager.GetVariableType(id);
                    varType.Initialize(name, null, null, node.ParentNodeId);
                    res = varType;
                    break;
                case NodeClass.ReferenceType:
                    var refType = typeManager.GetReferenceType(id);
                    refType.Initialize(name, null, null, node.ParentNodeId);
                    res = refType;
                    break;
                case NodeClass.DataType:
                    var dtType = typeManager.GetDataType(id);
                    dtType.Initialize(name, null, null, node.ParentNodeId);
                    res = dtType;
                    break;
                default:
                    return null;
            }

            res.Attributes.LoadFromSavedNode(node, typeManager);
            res.Source = NodeSource.CDF;
            return res;
        }

        public static BaseUANode? FromNodeState(NodeState node, NodeId? parentId, TypeManager typeManager)
        {
            var id = node.NodeId;
            if (node is BaseObjectState objState)
            {
                var obj = new UAObject(id, node.DisplayName?.Text, node.BrowseName, null, parentId, typeManager.GetObjectType(objState.TypeDefinitionId));
                obj.FullAttributes.LoadFromNodeState(objState);
                return obj;
            }
            if (node is BaseVariableState varState)
            {
                var vr = new UAVariable(id, node.DisplayName?.Text, node.BrowseName, null, parentId, typeManager.GetVariableType(varState.TypeDefinitionId));
                vr.FullAttributes.LoadFromNodeState(varState, typeManager);
                return vr;
            }
            if (node is BaseObjectTypeState objTState)
            {
                var objType = typeManager.GetObjectType(id);
                objType.Initialize(node.DisplayName?.Text, node.BrowseName, null, parentId ?? objTState.SuperTypeId);
                objType.FullAttributes.LoadFromNodeState(objTState);
                return objType;
            }
            if (node is BaseVariableTypeState varTState)
            {
                var varType = typeManager.GetVariableType(id);
                varType.Initialize(node.DisplayName?.Text, node.BrowseName, null, parentId ?? varTState.SuperTypeId);
                varType.FullAttributes.LoadFromNodeState(varTState, typeManager);
                return varType;
            }
            if (node is DataTypeState dataTState)
            {
                var dataType = typeManager.GetDataType(id);
                dataType.Initialize(node.DisplayName?.Text, node.BrowseName, null, parentId ?? dataTState.SuperTypeId);
                dataType.FullAttributes.LoadFromNodeState(dataTState);
                return dataType;
            }
            if (node is ReferenceTypeState refTState)
            {
                var refType = typeManager.GetReferenceType(id);
                refType.Initialize(node.DisplayName?.Text, node.BrowseName, null, parentId ?? refTState.SuperTypeId);
                refType.FullAttributes.LoadFromNodeState(refTState);
                return refType;
            }

            return null;
        }

        public bool UpdateFromNodeState(NodeState state, TypeManager typeManager)
        {
            if (Attributes.IsDataRead) return true;

            if (state is BaseObjectState objState)
            {
                if (this is not UAObject obj) return false;
                obj.FullAttributes.LoadFromNodeState(objState);
            }
            else if (state is BaseVariableState varState)
            {
                if (this is not UAVariable vr) return false;
                vr.FullAttributes.LoadFromNodeState(varState, typeManager);
            }
            else if (state is BaseObjectTypeState objTState)
            {
                if (this is not UAObjectType objType) return false;
                objType.FullAttributes.LoadFromNodeState(objTState);
            }
            else if (state is BaseVariableTypeState varTState)
            {
                if (this is not UAVariableType varType) return false;
                varType.FullAttributes.LoadFromNodeState(varTState, typeManager);
            }
            else if (state is DataTypeState dataTState)
            {
                if (this is not UADataType dataType) return false;
                dataType.FullAttributes.LoadFromNodeState(dataTState);
            }
            else if (state is ReferenceTypeState refTState)
            {
                if (this is not UAReferenceType refType) return false;
                refType.FullAttributes.LoadFromNodeState(refTState);
            }

            return true;
        }

        public virtual Dictionary<string, string>? GetExtraMetadata(FullConfig config, IUAClientAccess client)
        {
            return null;
        }

        public virtual int GetUpdateChecksum(TypeUpdateConfig update, bool dataTypeMetadata, bool nodeTypeMetadata)
        {
            if (update == null || !update.AnyUpdate) return 0;
            int checksum = 0;
            unchecked
            {
                if (update.Context)
                {
                    checksum += (ParentId?.GetHashCode() ?? 0);
                }
                if (update.Description)
                {
                    checksum = checksum * 31 + (Attributes.Description?.GetHashCode(StringComparison.InvariantCulture) ?? 0);
                }
                if (update.Name)
                {
                    checksum = checksum * 31 + (Name?.GetHashCode(StringComparison.InvariantCulture) ?? 0);
                }
                if (update.Metadata)
                {
                    int metaHash = 0;
                    if (Properties != null)
                    {
                        foreach (var prop in Properties.OrderBy(prop => prop.Name))
                        {
                            metaHash *= 31;
                            if (prop.Name == null) continue;
                            if (prop is UAVariable propVariable)
                            {
                                metaHash += (prop.Name, propVariable.Value?.Value).GetHashCode();
                            }
                            if (prop.Properties?.Any() ?? false)
                            {
                                metaHash += prop.GetUpdateChecksum(new TypeUpdateConfig { Metadata = true }, false, false);
                            }
                        }
                    }
                    checksum = checksum * 31 + metaHash;
                }
            }
            return checksum;
        }

        #region serialization
        public JsonDocument? ToJson(ILogger log, StringConverter converter, ConverterType type)
        {
            var options = new JsonSerializerOptions();
            converter.AddConverters(options, type);

            using var stream = new MemoryStream();
            JsonSerializer.Serialize(stream, this, options);
            stream.Seek(0, SeekOrigin.Begin);

            try
            {
                return JsonDocument.Parse(stream);
            }
            catch (Exception ex)
            {
                stream.Seek(0, SeekOrigin.Begin);
                log.LogError("Failed to parse JSON data: {Message}, invalid JSON: {Data}", ex.Message, Encoding.UTF8.GetString(stream.ToArray()));
                return null;
            }
        }

        private void PopulateAssetCreate(
            IUAClientAccess client,
            long? dataSetId,
            Dictionary<string, string>? metaMap,
            AssetCreate asset)
        {
            var id = GetUniqueId(client);
            asset.Description = Attributes.Description;
            asset.ExternalId = id;
            asset.Name = string.IsNullOrEmpty(Name) ? id : Name;
            asset.DataSetId = dataSetId;
            if (!ParentId.IsNullNodeId)
            {
                asset.ParentExternalId = client.GetUniqueId(ParentId);
            }
            if (Properties != null && Properties.Any() && (metaMap?.Any() ?? false))
            {
                foreach (var prop in Properties)
                {
                    if (prop is not UAVariable propVar) continue;
                    if (metaMap.TryGetValue(prop.Name ?? "", out var mapped))
                    {
                        var value = client.StringConverter.ConvertToString(propVar.Value, propVar.FullAttributes.DataType.EnumValues);
                        if (string.IsNullOrWhiteSpace(value)) continue;
                        switch (mapped)
                        {
                            case "description": asset.Description = value; break;
                            case "name": asset.Name = value; break;
                            case "parentId": asset.ParentExternalId = value; break;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Convert to CDF Asset.
        /// </summary>
        /// <param name="config">Active configuration object</param>
        /// <param name="client">Access to OPC-UA session</param>
        /// <param name="converter">StringConverter for converting fields</param>
        /// <param name="dataSetId">Optional dataSetId</param>
        /// <param name="metaMap">Map from metadata to asset attributes.</param>
        /// <returns></returns>
        public AssetCreate ToCDFAsset(
            FullConfig config,
            IUAClientAccess client,
            long? dataSetId,
            Dictionary<string, string>? metaMap)
        {
            var asset = new AssetCreate();
            PopulateAssetCreate(client, dataSetId, metaMap, asset);
            asset.Metadata = BuildMetadata(config, client, true);

            return asset;
        }
        protected Dictionary<string, string> BuildMetadataBase(Dictionary<string, string>? extras, IUAClientAccess client)
        {
            var result = extras ?? new Dictionary<string, string>();

            if (Properties == null) return result;

            foreach (var prop in Properties)
            {
                if (prop != null && !string.IsNullOrEmpty(prop.Name))
                {
                    if (prop is UAVariable variable)
                    {
                        result[prop.Name] = client.StringConverter.ConvertToString(variable.Value, variable.FullAttributes.DataType?.EnumValues)
                            ?? variable.Value.ToString();
                    }

                    if (prop.Properties != null)
                    {
                        var nestedProperties = prop.BuildMetadataBase(null, client);
                        foreach (var sprop in nestedProperties)
                        {
                            result[$"{prop.Name}_{sprop.Key}"] = sprop.Value;
                        }
                    }
                }
            }
            return result;
        }


        /// <summary>
        /// Return a dictionary of metadata fields for this node.
        /// </summary>
        /// <param name="config">Extraction config object</param>
        /// <param name="manager">DataTypeManager used to get information about the datatype</param>
        /// <param name="converter">StringConverter used for building metadata</param>
        /// <param name="getExtras">True to get extra metadata</param>
        /// <returns>Created metadata dictionary.</returns>
        public Dictionary<string, string> BuildMetadata(FullConfig config, IUAClientAccess client, bool getExtras)
        {
            Dictionary<string, string>? extras = null;
            if (getExtras)
            {
                extras = GetExtraMetadata(config, client);
            }
            return BuildMetadataBase(extras, client);
        }
        #endregion
    }
}
