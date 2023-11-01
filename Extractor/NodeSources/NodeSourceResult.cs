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

using Cognite.Extractor.Common;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.Nodes;
using Cognite.OpcUa.Types;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.OpcUa.NodeSources
{
    /// <summary>
    /// Contains the result of obtaining references from source systems.
    /// </summary>
    public class NodeSourceResult
    {
        public NodeSourceResult(
            IEnumerable<BaseUANode> sourceObjects,
            IEnumerable<UAVariable> sourceVariables,
            IEnumerable<BaseUANode> destinationObjects,
            IEnumerable<UAVariable> destinationVariables,
            IEnumerable<UAReference> destinationReferences,
            bool canBeUsedForDeletes,
            bool shouldBackgroundBrowse)
        {
            SourceObjects = sourceObjects;
            SourceVariables = sourceVariables;
            DestinationObjects = destinationObjects;
            DestinationVariables = destinationVariables;
            DestinationReferences = destinationReferences;
            CanBeUsedForDeletes = canBeUsedForDeletes;
            ShouldBackgroundBrowse = shouldBackgroundBrowse;
        }
        public IEnumerable<BaseUANode> SourceObjects { get; }
        public IEnumerable<UAVariable> SourceVariables { get; }
        public IEnumerable<BaseUANode> DestinationObjects { get; }
        public IEnumerable<UAVariable> DestinationVariables { get; }
        public IEnumerable<UAReference> DestinationReferences { get; }

        public bool CanBeUsedForDeletes { get; }
        public bool ShouldBackgroundBrowse { get; }
    }

    /// <summary>
    /// Data passed to pushers
    /// </summary>
    public class PusherInput
    {
        public IEnumerable<BaseUANode> Objects { get; }
        public IEnumerable<UAVariable> Variables { get; }
        public IEnumerable<UAReference> References { get; }
        public DeletedNodes? Deletes { get; }

        public PusherInput(IEnumerable<BaseUANode> objects, IEnumerable<UAVariable> variables, IEnumerable<UAReference> references, DeletedNodes? deletes)
        {
            Objects = objects;
            Variables = variables;
            References = references;
            Deletes = deletes;
        }

        public static async Task<PusherInput> FromNodeSourceResult(NodeSourceResult result, SessionContext context, DeletesManager? deletesManager, CancellationToken token)
        {
            DeletedNodes? deleted = null;
            if (deletesManager != null)
            {
                deleted = await deletesManager.GetDiffAndStoreIds(result, context, token);
            }
            return new PusherInput(result.DestinationObjects, result.DestinationVariables, result.DestinationReferences, deleted);
        }

        public PusherInput Merge(PusherInput other)
        {
            var objects = Objects.Concat(other.Objects).DistinctBy(n => n.Id).ToList();
            var variables = Variables.Concat(other.Variables).DistinctBy(n => n.DestinationId()).ToList();
            var references = References.Concat(other.References).DistinctBy(n => (n.Source.Id, n.Target.Id, n.Type.Id)).ToList();
            var deleted = Deletes?.Merge(other.Deletes!);

            return new PusherInput(objects, variables, references, deleted);
        }

        public PusherInput Filter(FullPushResult result, FullConfig config)
        {
            var objects = result.Objects ? Enumerable.Empty<BaseUANode>() : Objects;
            var variables = result.Variables ? Enumerable.Empty<UAVariable>() : Variables;
            var references = result.References ? Enumerable.Empty<UAReference>() : References;

            if (result.Variables && !result.Ranges)
            {
                variables = Variables.Where(v => v.FullAttributes.ShouldReadHistory(config)).DistinctBy(n => n.Id).ToList();
            }

            var deleted = result.Deletes ? null : Deletes;

            return new PusherInput(objects, variables, references, deleted);
        }
    }

    /// <summary>
    /// Class containing a summary of the result of pushing to a destination.
    /// </summary>
    public class FullPushResult
    {
        public bool Objects { get; set; }
        public bool Variables { get; set; }
        public bool References { get; set; }
        public bool Deletes { get; set; }
        public bool Ranges { get; set; }

        public void Apply(PushResult result)
        {
            Objects = result.Objects;
            Variables = result.Variables;
            References = result.References;
        }
    }
}
