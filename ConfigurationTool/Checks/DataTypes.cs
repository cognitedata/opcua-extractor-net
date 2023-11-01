/* Cognite Extractor for OPC-UA
Copyright (C) 2021 Cognite AS

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

using Cognite.OpcUa.Nodes;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.Config
{
    public partial class UAServerExplorer : UAClient
    {
        private readonly Dictionary<NodeId, ProtoDataType> customNumericTypes = new Dictionary<NodeId, ProtoDataType>();

        /// <summary>
        /// Returns true if the id is for a custom object. Tested by checking for non-integer identifiertype, or >0 namespaceUri.
        /// </summary>
        /// <param name="id">Id to test</param>
        /// <returns>True if id is a custom object</returns>
        private static bool IsCustomObject(NodeId id)
        {
            return id.NamespaceIndex != 0 || id.IdType != IdType.Numeric;
        }

        /// <summary>
        /// Try to identify the given UANode as a datatype, updating the summary and config
        /// based on the outcome.
        /// The goal is to identify enumerations and custom types, to determine whether
        /// custom datatype configuration is needed.
        /// </summary>
        /// <param name="type">Type to test.</param>
        private void TestDataType(BaseUANode type)
        {
            if (!IsCustomObject(type.Id)) return;
            uint dataTypeSwitch = 0;
            bool inHierarchy = false;

            // The datatype may be placed correctly in the datatype hierarchy.
            if (ToolUtil.IsChildOf(dataTypes, type, DataTypes.Number))
            {
                dataTypeSwitch = DataTypes.Number;
                inHierarchy = true;
            }
            else if (ToolUtil.IsChildOf(dataTypes, type, DataTypes.Boolean))
            {
                dataTypeSwitch = DataTypes.Boolean;
                inHierarchy = true;
            }
            else if (ToolUtil.IsChildOf(dataTypes, type, DataTypes.Enumeration))
            {
                dataTypeSwitch = DataTypes.Enumeration;
                inHierarchy = true;
            }
            // If not, it may be placed incorrectly but contain naming that indicates what type it is.
            if (dataTypeSwitch == 0)
            {
                if (ToolUtil.NodeNameContains(type, "real")
                    || ToolUtil.NodeNameContains(type, "integer")
                    || ToolUtil.NodeNameStartsWith(type, "int")
                    || ToolUtil.NodeNameContains(type, "number"))
                {
                    dataTypeSwitch = DataTypes.Number;
                }
                else if (ToolUtil.NodeNameContains(type, "bool"))
                {
                    dataTypeSwitch = DataTypes.Boolean;
                }
                else if (ToolUtil.NodeNameContains(type, "enum"))
                {
                    dataTypeSwitch = DataTypes.Enumeration;
                }
            }
            // Finally, log the results and update the summary.
            switch (dataTypeSwitch)
            {
                case DataTypes.Number:
                    log.LogInformation("Found potential numeric type: {Id}", type.Id);
                    break;
                case DataTypes.Boolean:
                    log.LogInformation("Found potential boolean type: {Id}", type.Id);
                    break;
                case DataTypes.Enumeration:
                    log.LogInformation("Found potential enum type: {Id}, consider turning on extraction.enum-as-strings", type.Id);
                    Summary.DataTypes.Enums = true;
                    break;
            }
            // Update configuration based on whether or not the node was found in hierarchy.
            if (dataTypeSwitch > 0)
            {
                if (inHierarchy)
                {
                    log.LogInformation("DataType {Id} is correctly in hierarchy, auto discovery can be used instead", type.Id);
                    baseConfig.Extraction.DataTypes.AutoIdentifyTypes = true;
                }
                else
                {
                    customNumericTypes[type.Id] = new ProtoDataType
                    {
                        IsStep = dataTypeSwitch == DataTypes.Boolean,
                        Enum = dataTypeSwitch == DataTypes.Enumeration,
                        NodeId = NodeIdToProto(type.Id)
                    };
                }
                if (dataTypeSwitch == DataTypes.Enumeration)
                {
                    log.LogInformation("DataType {Id} is enum, and auto discovery should be enabled to discover labels", type.Id);
                    baseConfig.Extraction.DataTypes.AutoIdentifyTypes = true;
                }
            }
        }

        /// <summary>
        /// Browse the datatype hierarchy, checking for custom numeric datatypes.
        /// </summary>
        public async Task ReadCustomTypes(CancellationToken token)
        {
            if (Session == null || !Session.Connected)
            {
                await Run(token, 0);
                await LimitConfigValues(token);
            }
            await PopulateDataTypes(token);

            customNumericTypes.Clear();
            foreach (var type in dataTypes)
            {
                TestDataType(type);
            }

            if (!Summary.DataTypes.Enums && dataTypes.Any(type => ToolUtil.IsChildOf(dataTypes, type, DataTypes.Enumeration))) Summary.DataTypes.Enums = true;

            log.LogInformation("Found {Count} custom datatypes outside of normal hierarchy", customNumericTypes.Count);
            Summary.DataTypes.CustomNumTypesCount = customNumericTypes.Count;
            baseConfig.Extraction.DataTypes.CustomNumericTypes = customNumericTypes.Values;
        }

        /// <summary>
        /// Look through the node hierarchy to find arrays and strings, setting MaxArraySize and StringVariables
        /// </summary>
        public async Task IdentifyDataTypeSettings(CancellationToken token)
        {
            var roots = Config.Extraction.GetRootNodes(Context);

            int oldArraySize = Config.Extraction.DataTypes.MaxArraySize;
            int arrayLimit = Config.Extraction.DataTypes.MaxArraySize == 0 ? 10 : Config.Extraction.DataTypes.MaxArraySize;
            if (arrayLimit < 0) arrayLimit = int.MaxValue;

            Config.Extraction.DataTypes.MaxArraySize = 10;

            await PopulateNodes(token);
            await PopulateDataTypes(token);
            await ReadNodeData(token);

            log.LogInformation("Mapping out variable datatypes");

            var variables = nodeList
                .Where(node => (node is UAVariable variable) && !variable.IsProperty)
                .Select(node => node as UAVariable)
                .Where(node => node != null);

            bool history = false;
            bool stringVariables = false;
            int maxLimitedArrayLength = 0;

            var identifiedTypes = new Dictionary<NodeId, BaseUANode>();
            var missingTypes = new HashSet<NodeId>();

            // Look for ArrayDimensions fields on values, we use these to identify arrays.
            foreach (var variable in variables)
            {
                if (variable == null) continue;
                if (variable.ArrayDimensions != null
                    && variable.ArrayDimensions.Length == 1
                    && variable.ArrayDimensions[0] <= arrayLimit
                    && variable.ArrayDimensions[0] > maxLimitedArrayLength)
                {
                    maxLimitedArrayLength = variable.ArrayDimensions[0];
                }
                else if (variable.ValueRank == ValueRanks.Any
                    || variable.ValueRank == ValueRanks.OneOrMoreDimensions)
                {
                    Summary.DataTypes.UnspecificDimensions = true;
                    continue;
                }
                else if (variable.ValueRank >= ValueRanks.TwoDimensions || variable.ArrayDimensions != null && variable.ArrayDimensions.Length > 1)
                {
                    Summary.DataTypes.HighDimensions = true;
                    continue;
                }
                else if (variable.ArrayDimensions != null
                         && variable.ArrayDimensions.Length == 1
                         && variable.ArrayDimensions[0] > arrayLimit)
                {
                    Summary.DataTypes.MaxUnlimitedArraySize = variable.ArrayDimensions[0];
                    Summary.DataTypes.LargeArrays = true;
                    continue;
                }

                if (variable.FullAttributes.ShouldReadHistory(Config))
                {
                    history = true;
                }

                if (variable.FullAttributes.DataType == null || variable.FullAttributes.DataType.Id == null || variable.FullAttributes.DataType.Id.IsNullNodeId)
                {
                    Summary.DataTypes.NullDataType = true;
                    log.LogWarning("Variable datatype is null on id: {Id}", variable.Id);
                    continue;
                }

                var dataType = dataTypes.FirstOrDefault(type => type.Id == variable.FullAttributes.DataType.Id);

                if (dataType == null)
                {
                    if (missingTypes.Add(variable.FullAttributes.DataType.Id))
                    {
                        Summary.DataTypes.MissingDataType = true;
                        log.LogWarning("DataType found on node but not in hierarchy, " +
                                    "this may mean that some datatypes are defined outside of the main datatype hierarchy: {Type}", variable.FullAttributes.DataType);
                    }
                    continue;
                }

                identifiedTypes.TryAdd(dataType.Id, dataType);
            }

            log.LogInformation("Found {Count} distinct data types in detected variables", identifiedTypes.Count);

            foreach (var dataType in identifiedTypes.Values)
            {
                if (dataType.Id.NamespaceIndex == 0)
                {
                    uint identifier = (uint)dataType.Id.Identifier;
                    if ((identifier < DataTypes.Boolean || identifier > DataTypes.Double)
                           && identifier != DataTypes.Integer && identifier != DataTypes.UInteger)
                    {
                        stringVariables = true;
                    }
                }
                else
                {
                    if (!customNumericTypes.ContainsKey(dataType.Id))
                    {
                        stringVariables = true;
                    }
                }
            }

            if (stringVariables)
            {
                log.LogInformation("Variables with string datatype were discovered, and the AllowStringVariables config option " +
                                "will be set to true");
            }
            else if (!baseConfig.Extraction.DataTypes.AllowStringVariables)
            {
                log.LogInformation("No string variables found and the AllowStringVariables option will be set to false");
            }

            if (maxLimitedArrayLength > 0)
            {
                log.LogInformation("Arrays of length {Max} were found, which will be used to set the MaxArraySize option", maxLimitedArrayLength);
            }
            else
            {
                log.LogInformation("No arrays were found, MaxArraySize remains at its current setting, or 0 if unset");
            }

            if (history)
            {
                log.LogInformation("Historizing variables were found, tests on history chunkSizes will be performed later");
            }
            else
            {
                log.LogInformation("No historizing variables were found, tests on history chunkSizes will be skipped");
            }

            Config.Extraction.DataTypes.MaxArraySize = oldArraySize;

            baseConfig.Extraction.DataTypes.AllowStringVariables = baseConfig.Extraction.DataTypes.AllowStringVariables || stringVariables;
            baseConfig.Extraction.DataTypes.MaxArraySize = maxLimitedArrayLength > 0 ? maxLimitedArrayLength : oldArraySize;

            Summary.DataTypes.StringVariables = stringVariables;
            Summary.DataTypes.MaxArraySize = maxLimitedArrayLength;
        }
    }
}
