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
    public class UAReferenceType
    {
        public NodeId Id { get; }
        private string name;
        private string inverseName;
        public bool HasName => name != null;
        public UAReferenceType(NodeId id)
        {
            Id = id;
        }
        public void SetNames(string name, string inverseName)
        {
            this.name = name;
            this.inverseName = inverseName;
        }
        public string GetName(bool isInverse)
        {
            if (isInverse && !string.IsNullOrEmpty(inverseName)) return inverseName;
            return name;
        }
    }
}
