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
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System.Collections.Generic;
using System.Text;

namespace Cognite.OpcUa
{
    /// <summary>
    /// Describes a transformation to the source hierarchy. Consists of a filter and a transformation type.
    /// </summary>
    public class NodeTransformation
    {
        public NodeFilter Filter { get; }
        public TransformationType Type { get; }
        private readonly int index;
        public NodeTransformation(RawNodeTransformation raw, int index)
        {
            Filter = raw.Filter ?? new NodeFilter();
            Type = raw.Type;
            this.index = index;
        }

        private bool ShouldSkip(BaseUANode node, FullConfig config)
        {
            if (node == null) return true;

            switch (Type)
            {
                case TransformationType.Property:
                    // Already a property
                    return node.IsRawProperty;
                case TransformationType.TimeSeries:
                    // No need to transform to timeseries if node is not property, or node cannot be a timeseries.
                    return !node.IsRawProperty || node.NodeClass != NodeClass.Variable;
                case TransformationType.DropSubscriptions:
                    // No need to drop subscriptions if we are not subscribing to this node.
                    return !(node is UAVariable variable && variable.FullAttributes.ShouldSubscribe(config))
                        && !(node is UAObject obj && obj.FullAttributes.ShouldSubscribeToEvents(config));
                case TransformationType.AsEvents:
                    // If not a variable or already publishing values as events we skip here.
                    return node is not UAVariable evtvariable || evtvariable.AsEvents;
            }

            return false;
        }

        /// <summary>
        /// Modify the given node if it passes the filter.
        /// </summary>
        /// <param name="node">Node to test</param>
        /// <param name="ns">Active NamespaceTable</param>
        public void ApplyTransformation(ILogger log, BaseUANode node, NamespaceTable ns, FullConfig config)
        {
            if (ShouldSkip(node, config)) return;

            if (Filter.IsMatch(node, ns))
            {
                switch (Type)
                {
                    case TransformationType.Include:
                        node.Ignore = false;
                        log.LogTrace("Including node {Name} {Id} due to matching include filter {Idx}", node.Name, node.Id, index);
                        break;
                    case TransformationType.Ignore:
                        node.Ignore = true;
                        log.LogTrace("Ignoring node {Name} {Id} due to matching ignore filter {Idx}", node.Name, node.Id, index);
                        break;
                    case TransformationType.Property:
                        node.IsRawProperty = true;
                        log.LogTrace("Treating node {Name} {Id} as property due to matching filter {Idx}", node.Name, node.Id, index);
                        break;
                    case TransformationType.DropSubscriptions:
                        if (node is UAVariable variable)
                        {
                            variable.FullAttributes.ShouldSubscribeOverride = false;
                        }
                        else if (node is UAObject obj)
                        {
                            obj.FullAttributes.SubscribeToEventsOverride = false;
                        }
                        log.LogDebug("Dropping subscriptions on node {Name} {Id} due to matching filter {Idx}", node.Name, node.Id, index);
                        break;
                    case TransformationType.TimeSeries:
                        node.IsRawProperty = false;
                        log.LogTrace("Treating node {Name} {Id} as timeseries due to matching filter {Idx}", node.Name, node.Id, index);
                        break;
                    case TransformationType.AsEvents:
                        if (node is UAVariable evtvariable)
                        {
                            evtvariable.AsEvents = true;
                        }
                        break;
                }
            }
        }
        public override string ToString()
        {
            var builder = new StringBuilder();
            builder.AppendFormat("Transformation {0}:", index);
            builder.AppendLine();
            builder.AppendFormat("Type: {0}", Type);
            builder.AppendLine();
            builder.Append("Filter:");
            builder.AppendLine();
            Filter.Format(builder, 0);
            return builder.ToString();
        }
    }

    public class TransformationCollection
    {
        private readonly List<NodeTransformation> transformations;
        private readonly bool hasInclude;
        private readonly bool hasNonBasicInclude;
        private readonly bool hasNonBasicIncludeAfterIgnore;

        public TransformationCollection(List<NodeTransformation> transformations)
        {
            bool seenIgnore = false;
            foreach (var tf in transformations)
            {
                if (tf.Type == TransformationType.Include)
                {
                    hasInclude = true;
                    if (!tf.Filter.IsBasic)
                    {
                        hasNonBasicInclude = true;
                        if (seenIgnore)
                        {
                            hasNonBasicIncludeAfterIgnore = true;
                        }
                    }
                }
                else if (tf.Type == TransformationType.Ignore)
                {
                    seenIgnore = true;
                }
            }
            this.transformations = transformations;
        }

        public void ApplyTransformations(ILogger log, BaseUANode node, NamespaceTable ns, FullConfig config)
        {
            if (hasInclude)
            {
                node.Ignore = true;
            }

            foreach (var tf in transformations)
            {
                tf.ApplyTransformation(log, node, ns, config);
            }
        }

        public bool NoEarlyFiltering => hasInclude && hasNonBasicIncludeAfterIgnore;

        public bool ShouldIncludeBasic(string displayName, NodeId id, NodeId typeDefinition, NamespaceTable namespaces, NodeClass nc)
        {
            bool include = true;
            if (NoEarlyFiltering)
            {
                // Since we have a non-trivial include after an ignore filter we cannot actually
                // ignore _any_ nodes here, since it may be un-ignored later on.
                // We log a warning about this since it may make extraction orders of magnitude
                // more expensive.
                return true;
            }
            else if (hasInclude && !hasNonBasicInclude)
            {
                // We only ignore by default if all includes are trivial.
                include = false;
            }

            foreach (var tf in transformations)
            {
                if (!tf.Filter.IsBasic) continue;

                if (tf.Type == TransformationType.Include)
                {
                    include |= tf.Filter.IsBasicMatch(displayName, id, typeDefinition, namespaces, nc);
                }
                else if (tf.Type == TransformationType.Ignore)
                {
                    include &= !tf.Filter.IsBasicMatch(displayName, id, typeDefinition, namespaces, nc);
                }
            }
            return include;
        }
    }

    public enum TransformationType
    {
        Ignore,
        Property,
        DropSubscriptions,
        TimeSeries,
        AsEvents,
        Include
    }
}
