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

using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Opc.Ua;

namespace Cognite.OpcUa.Types
{
    public class MappedNode
    {
        public NodeId Id { get; }
        public int Checksum { get; }
        public bool IsProperty { get; }
        public bool IsObject { get; }
        public NodeClass NodeClass { get; }

        public MappedNode(BaseUANode node, TypeUpdateConfig update, bool dataTypeMetadata, bool nodeTypeMetadata)
        {
            Id = node.Id;
            Checksum = node.GetUpdateChecksum(update, dataTypeMetadata, nodeTypeMetadata);
            IsProperty = node.IsProperty;
            IsObject = node is not UAVariable variable || variable.IsObject;
        }
    }
}
