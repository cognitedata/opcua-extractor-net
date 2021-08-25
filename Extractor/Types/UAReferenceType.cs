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
using System.Diagnostics.CodeAnalysis;

namespace Cognite.OpcUa.Types
{
    /// <summary>
    /// Represents a ReferenceType in the OPC-UA type hierarchy.
    /// </summary>
    public class UAReferenceType
    {
        public NodeId Id { get; }
        [MaybeNull, AllowNull]
        private string name;
        [MaybeNull, AllowNull]
        private string inverseName;
        public bool HasName => name != null;
        public UAReferenceType(NodeId id)
        {
            Id = id;
        }
        /// <summary>
        /// Set the name and inverseName of this reference type.
        /// </summary>
        /// <param name="name">Forward name</param>
        /// <param name="inverseName">Inverse name</param>
        public void SetNames([AllowNull] string name, [AllowNull] string inverseName)
        {
            this.name = name;
            this.inverseName = inverseName;
        }
        /// <summary>
        /// Retrieve a descriptor for this reference type.
        /// </summary>
        /// <param name="isInverse">True to get the inverse name of this reference type.</param>
        /// <returns>Descriptor for this reference, if set.</returns>
        [return: MaybeNull]
        public string GetName(bool isInverse)
        {
            if (isInverse && !string.IsNullOrEmpty(inverseName)) return inverseName;
            return name;
        }
    }
}
