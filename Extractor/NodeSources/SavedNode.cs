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

using Opc.Ua;
using System.Text.Json.Serialization;

namespace Cognite.OpcUa.NodeSources
{
    public class SavedNode
    {
        public NodeId? NodeId { get; set; }
        public NodeId? ParentNodeId { get; set; }
        [JsonPropertyName("name")]
        public string? Name { get; set; }
        public NodeId? DataTypeId { get; set; }
        public InternalInfo? InternalInfo { get; set; }
    }
    public class InternalInfo
    {
        public byte EventNotifier { get; set; }
        public bool ShouldSubscribeData { get; set; }
        public bool ShouldSubscribeEvents { get; set; }
        public bool AsEvents { get; set; }
        public byte AccessLevel { get; set; }
        public bool Historizing { get; set; }
        public int ValueRank { get; set; }
        public int[]? ArrayDimensions { get; set; }
        public int Index { get; set; }
        public NodeClass NodeClass { get; set; }
        public NodeId? TypeDefinition { get; set; }
    }
}
