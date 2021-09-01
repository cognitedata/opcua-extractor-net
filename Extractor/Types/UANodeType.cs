/* Cognite Extractor for OPC-UA
Copyright (C) 2020 Cognite AS

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

namespace Cognite.OpcUa.Types
{
    /// <summary>
    /// Represents a node type in the OPC-UA type hierarchy.
    /// </summary>
    public class UANodeType
    {
        public NodeId Id { get; }
        public string? Name { get; set; }
        public bool IsVariableType { get; }
        public UANodeType(NodeId id, bool isVariableType)
        {
            Id = id;
            IsVariableType = isVariableType;
        }

    }
}
