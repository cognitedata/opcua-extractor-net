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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Cognite.OpcUa.Config;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace Cognite.OpcUa.Utils
{
    /// <summary>
    /// Helper class for MQTT grouping logic with advanced selector and exclusion capabilities.
    /// </summary>
    public static class MqttGroupingHelper
    {
        /// <summary>
        /// Finds the appropriate group name for a given node ID based on publish groups configuration.
        /// </summary>
        /// <param name="nodeId">The node ID to find a group for</param>
        /// <param name="publishGroups">List of publish groups with their selectors</param>
        /// <param name="usePrefixMatching">
        /// If true (ROOT_NODE_BASED): Uses "most specific rule wins" logic - longest prefix/pattern wins.
        /// If false (TAG_LIST_BASED): Uses "first matching rule wins" logic - first group in order wins.
        /// </param>
        /// <param name="namespaceTable">Optional namespace table for automatic namespace resolution</param>
        /// <param name="logger">Logger for debugging</param>
        /// <returns>Group name if found, null if no matching group</returns>
        public static string? FindGroupNameForNode(
            string nodeId, 
            List<PublishGroup> publishGroups, 
            bool usePrefixMatching,
            NamespaceTable? namespaceTable = null,
            ILogger? logger = null)
        {
            if (string.IsNullOrEmpty(nodeId) || publishGroups == null || !publishGroups.Any())
            {
                return null;
            }

            logger?.LogTrace("[MqttGroupingHelper] Finding group for node: {NodeId}, usePrefixMatching: {UsePrefixMatching}", 
                nodeId, usePrefixMatching);

            // Resolve short-form string identifiers to full namespace format if needed
            var resolvedNodeId = ResolveNodeIdNamespace(nodeId, namespaceTable, logger);

            if (usePrefixMatching)
            {
                // ROOT_NODE_BASED: "Most specific rule wins" - find the longest matching prefix/pattern
                return FindMostSpecificMatch(resolvedNodeId, publishGroups, logger);
            }
            else
            {
                // TAG_LIST_BASED: "First matching rule wins" - use the first group that matches
                return FindFirstMatch(resolvedNodeId, publishGroups, logger);
            }
        }

        /// <summary>
        /// ROOT_NODE_BASED logic: Find the most specific (longest) matching rule.
        /// </summary>
        private static string? FindMostSpecificMatch(string nodeId, List<PublishGroup> publishGroups, ILogger? logger)
        {
            string? bestMatch = null;
            int bestMatchLength = 0;

            foreach (var group in publishGroups)
            {
                if (string.IsNullOrEmpty(group.Name) || group.Selectors == null)
                    continue;

                foreach (var selector in group.Selectors)
                {
                    var matchResult = EvaluateSelector(nodeId, selector, logger);
                    if (matchResult.IsMatch && matchResult.MatchLength > bestMatchLength)
                    {
                        bestMatch = group.Name;
                        bestMatchLength = matchResult.MatchLength;
                        logger?.LogTrace("[ROOT_NODE_BASED] Better match found: Group '{GroupName}', Length: {Length}", 
                            group.Name, matchResult.MatchLength);
                    }
                }
            }

            logger?.LogTrace("[ROOT_NODE_BASED] Final match: {GroupName}", bestMatch);
            return bestMatch;
        }

        /// <summary>
        /// TAG_LIST_BASED logic: Find the first matching rule in order.
        /// </summary>
        private static string? FindFirstMatch(string nodeId, List<PublishGroup> publishGroups, ILogger? logger)
        {
            foreach (var group in publishGroups)
            {
                if (string.IsNullOrEmpty(group.Name) || group.Selectors == null)
                    continue;

                foreach (var selector in group.Selectors)
                {
                    var matchResult = EvaluateSelector(nodeId, selector, logger);
                    if (matchResult.IsMatch)
                    {
                        logger?.LogTrace("[TAG_LIST_BASED] First match found: Group '{GroupName}'", group.Name);
                        return group.Name;
                    }
                }
            }

            logger?.LogTrace("[TAG_LIST_BASED] No match found");
            return null;
        }

        /// <summary>
        /// Evaluates whether a node ID matches a selector's criteria and is not excluded.
        /// </summary>
        private static (bool IsMatch, int MatchLength) EvaluateSelector(string nodeId, SelectorConfig selector, ILogger? logger)
        {
            bool isMatch = false;
            int matchLength = 0;

            // Check inclusion criteria
            if (!string.IsNullOrEmpty(selector.Prefix))
            {
                if (nodeId.StartsWith(selector.Prefix, StringComparison.OrdinalIgnoreCase))
                {
                    isMatch = true;
                    matchLength = selector.Prefix.Length;
                    logger?.LogTrace("[Selector] Prefix match: '{Prefix}' -> {NodeId}", selector.Prefix, nodeId);
                }
            }
            else if (selector.Tags != null && selector.Tags.Any())
            {
                if (selector.Tags.Contains(nodeId, StringComparer.OrdinalIgnoreCase))
                {
                    isMatch = true;
                    matchLength = nodeId.Length; // Exact match gets full length
                    logger?.LogTrace("[Selector] Tag exact match: {NodeId}", nodeId);
                }
            }
            else if (!string.IsNullOrEmpty(selector.Pattern))
            {
                try
                {
                    var regex = new Regex(selector.Pattern, RegexOptions.IgnoreCase | RegexOptions.Compiled);
                    var match = regex.Match(nodeId);
                    if (match.Success)
                    {
                        isMatch = true;
                        matchLength = match.Length;
                        logger?.LogTrace("[Selector] Pattern match: '{Pattern}' -> {NodeId}", selector.Pattern, nodeId);
                    }
                }
                catch (Exception ex)
                {
                    logger?.LogWarning(ex, "[Selector] Invalid regex pattern: {Pattern}", selector.Pattern);
                }
            }

            // Check exclusion criteria if matched
            if (isMatch && selector.Exclude != null)
            {
                bool isExcluded = false;

                // Check excluded tags
                if (selector.Exclude.Tags != null && 
                    selector.Exclude.Tags.Contains(nodeId, StringComparer.OrdinalIgnoreCase))
                {
                    isExcluded = true;
                    logger?.LogTrace("[Selector] Excluded by tag: {NodeId}", nodeId);
                }

                // Check excluded patterns
                if (!isExcluded && selector.Exclude.Patterns != null)
                {
                    foreach (var excludePattern in selector.Exclude.Patterns)
                    {
                        try
                        {
                            var excludeRegex = new Regex(excludePattern, RegexOptions.IgnoreCase | RegexOptions.Compiled);
                            if (excludeRegex.IsMatch(nodeId))
                            {
                                isExcluded = true;
                                logger?.LogTrace("[Selector] Excluded by pattern: '{Pattern}' -> {NodeId}", 
                                    excludePattern, nodeId);
                                break;
                            }
                        }
                        catch (Exception ex)
                        {
                            logger?.LogWarning(ex, "[Selector] Invalid exclude regex pattern: {Pattern}", excludePattern);
                        }
                    }
                }

                if (isExcluded)
                {
                    isMatch = false;
                    matchLength = 0;
                }
            }

            return (isMatch, matchLength);
        }

        /// <summary>
        /// Resolves short-form string identifiers (s=) to full namespace format (ns=X;s=) if needed.
        /// Other identifier types (i=, g=, b=) or already full ns= identifiers are returned as-is.
        /// </summary>
        private static string ResolveNodeIdNamespace(string nodeId, NamespaceTable? namespaceTable, ILogger? logger)
        {
            if (string.IsNullOrEmpty(nodeId) || namespaceTable == null)
            {
                return nodeId;
            }

            // If already has namespace prefix or is not a string identifier, return as-is
            if (nodeId.StartsWith("ns=") || !nodeId.StartsWith("s="))
            {
                return nodeId;
            }

            try
            {
                // Parse the node ID to see if we can resolve its namespace
                var parsedNodeId = NodeId.Parse(nodeId);
                if (parsedNodeId.NamespaceIndex > 0)
                {
                    // Node ID already has a namespace index, construct full format
                    var namespaceUri = namespaceTable.GetString(parsedNodeId.NamespaceIndex);
                    if (!string.IsNullOrEmpty(namespaceUri))
                    {
                        var resolved = $"ns={parsedNodeId.NamespaceIndex};{nodeId}";
                        logger?.LogTrace("[Namespace] Resolved {Original} -> {Resolved}", nodeId, resolved);
                        return resolved;
                    }
                }
            }
            catch (Exception ex)
            {
                logger?.LogTrace(ex, "[Namespace] Could not parse node ID for namespace resolution: {NodeId}", nodeId);
            }

            return nodeId;
        }
    }
}
