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

using Cognite.OpcUa.Types;
using System.Collections.Generic;

namespace Cognite.OpcUa.NodeSources
{
    /// <summary>
    /// Contains the result of obtaining references from source systems.
    /// </summary>
    public class NodeSourceResult
    {
        public NodeSourceResult(
            IEnumerable<UANode> sourceObjects,
            IEnumerable<UAVariable> sourceVariables,
            IEnumerable<UANode> destinationObjects,
            IEnumerable<UAVariable> destinationVariables,
            IEnumerable<UAReference> destinationReferences,
            bool canBeUsedForDeletes)
        {
            SourceObjects = sourceObjects;
            SourceVariables = sourceVariables;
            DestinationObjects = destinationObjects;
            DestinationVariables = destinationVariables;
            DestinationReferences = destinationReferences;
            CanBeUsedForDeletes = canBeUsedForDeletes;
        }
        public IEnumerable<UANode> SourceObjects { get; }
        public IEnumerable<UAVariable> SourceVariables { get; }
        public IEnumerable<UANode> DestinationObjects { get; }
        public IEnumerable<UAVariable> DestinationVariables { get; }
        public IEnumerable<UAReference> DestinationReferences { get; }

        public bool CanBeUsedForDeletes { get; }
    }
}
