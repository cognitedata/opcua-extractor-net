using Cognite.OpcUa.Config;
using Cognite.OpcUa.Types;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.OpcUa.Utils
{
    /// <summary>
    /// Utility class for grouping UADataPoints based on MQTT transmission strategy
    /// </summary>
    public class MqttTransmissionGrouper
    {
        private readonly MqttPusherConfig config;
        private readonly ILogger logger;
        private readonly IUAClientAccess client;

        public MqttTransmissionGrouper(MqttPusherConfig config, ILogger logger, IUAClientAccess client)
        {
            this.config = config;
            this.logger = logger;
            this.client = client;
        }

        /// <summary>
        /// Groups UADataPoints based on the configured transmission strategy
        /// </summary>
        /// <param name="dataPoints">Collection of UADataPoints to group</param>
        /// <returns>Grouped data points as key-value pairs</returns>
        public IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>> GroupDataPoints(
            IEnumerable<UADataPoint> dataPoints)
        {
            var dataPointsList = dataPoints.ToList();
            if (!dataPointsList.Any())
            {
                return Enumerable.Empty<KeyValuePair<string, IEnumerable<UADataPoint>>>();
            }

            logger.LogInformation("[MqttTransmissionGrouper] Grouping {Count} datapoints using strategy: {Strategy}", 
                dataPointsList.Count, config.GetEffectiveTransmissionStrategy());
            
            // Log first 10 datapoint IDs to understand the input data structure
            var firstTenIds = dataPointsList.Take(10).Select(dp => dp.Id).ToList();
            logger.LogInformation("[INPUT DEBUG] First 10 datapoint IDs: {Ids}", string.Join(", ", firstTenIds));
            
            // Count unique IDs in input
            var uniqueInputIds = dataPointsList.Select(dp => dp.Id).Distinct().Count();
            logger.LogInformation("[INPUT DEBUG] Total input datapoints: {Total}, Unique IDs: {Unique}", 
                dataPointsList.Count, uniqueInputIds);

            return config.GetEffectiveTransmissionStrategy() switch
            {
                MqttTransmissionStrategy.ROOT_NODE_BASED => GroupByRootNode(dataPointsList),
                MqttTransmissionStrategy.CHUNK_BASED => GroupByChunk(dataPointsList),
                MqttTransmissionStrategy.TAG_LIST_BASED => GroupByTagList(dataPointsList),
                MqttTransmissionStrategy.TAG_CHANGE_BASED => GroupByTagChange(dataPointsList),
                _ => GroupByChunk(dataPointsList) // Default fallback
            };
        }

        /// <summary>
        /// Groups data points by their root node based on extraction.root-nodes configuration
        /// </summary>
        private IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>> GroupByRootNode(
            IList<UADataPoint> dataPoints)
        {
            // For now, we'll use a simple prefix-based grouping approach
            // This can be enhanced later with proper node hierarchy traversal
            var rootNodeMap = new Dictionary<string, List<UADataPoint>>();

            logger.LogTrace("[ROOT_NODE_BASED] Processing {Count} datapoints for root node grouping", 
                dataPoints.Count);

            // PRESERVE ALL DATAPOINTS: No deduplication to maintain multiple values per tag
            logger.LogInformation("[ROOT_NODE_BASED] Processing all {Count} datapoints without deduplication", 
                dataPoints.Count);

            // Group datapoints by root node and count them
            var rootNodeCounts = new Dictionary<string, int>();
            
            foreach (var dataPoint in dataPoints)
            {
                var rootNodeKey = ExtractRootNodeFromId(dataPoint.Id);
                
                if (!rootNodeMap.ContainsKey(rootNodeKey))
                {
                    rootNodeMap[rootNodeKey] = new List<UADataPoint>();
                    rootNodeCounts[rootNodeKey] = 0;
                }
                
                rootNodeMap[rootNodeKey].Add(dataPoint);
                rootNodeCounts[rootNodeKey]++;
            }

            logger.LogInformation("[ROOT_NODE_BASED] Grouped into {GroupCount} root node groups", rootNodeMap.Count);
            
            // Log root node summary with counts
            foreach (var kvp in rootNodeCounts.OrderBy(x => x.Key))
            {
                logger.LogInformation("[ROOT_NODE_BASED] Root node: {RootNode}, datapoints: {DataPointCount}", 
                    kvp.Key, kvp.Value);
            }
            return rootNodeMap.Select(kvp => new KeyValuePair<string, IEnumerable<UADataPoint>>(kvp.Key, kvp.Value));
        }

        /// <summary>
        /// Groups data points by specified tag lists
        /// </summary>
        private IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>> GroupByTagList(
            IList<UADataPoint> dataPoints)
        {
            // Try new format first (with custom names)
            var effectiveTagListsWithNames = config.TransmissionStrategyConfig?.GetEffectiveTagListsWithNames();
            if (effectiveTagListsWithNames != null && effectiveTagListsWithNames.Any())
            {
                return GroupByTagListWithNames(dataPoints, effectiveTagListsWithNames);
            }

            // Fall back to legacy format
            var effectiveTagLists = config.GetEffectiveTagLists();
            if (effectiveTagLists == null || !effectiveTagLists.Any())
            {
                logger.LogWarning("[TAG_LIST_BASED] No tag lists configured, falling back to chunk-based grouping");
                return GroupByChunk(dataPoints);
            }

            var tagListGroups = new Dictionary<string, List<UADataPoint>>();
            var processedDataPoints = new HashSet<string>();

            logger.LogTrace("[TAG_LIST_BASED] Processing {Count} datapoints against {ListCount} tag lists (legacy format)", 
                dataPoints.Count, effectiveTagLists.Count);

            // Process each tag list
            for (int i = 0; i < effectiveTagLists.Count; i++)
            {
                var tagList = effectiveTagLists[i];
                var groupDataPoints = new List<UADataPoint>();

                foreach (var tag in tagList)
                {
                    var matchingDataPoints = dataPoints.Where(dp => 
                        dp.Id.Equals(tag, StringComparison.OrdinalIgnoreCase) && 
                        !processedDataPoints.Contains(dp.Id)).ToList();

                    foreach (var dp in matchingDataPoints)
                    {
                        groupDataPoints.Add(dp);
                        processedDataPoints.Add(dp.Id);
                    }
                }

                if (groupDataPoints.Any())
                {
                    // Create meaningful group key for tag list
                    var groupKey = $"tag_list_{i + 1}";
                    tagListGroups[groupKey] = groupDataPoints;
                }
            }

            // Handle unassigned data points
            var unassignedDataPoints = dataPoints.Where(dp => !processedDataPoints.Contains(dp.Id)).ToList();
            if (unassignedDataPoints.Any())
            {
                tagListGroups["unassigned"] = unassignedDataPoints;
            }

            logger.LogTrace("[TAG_LIST_BASED] Grouped into {GroupCount} tag list groups", tagListGroups.Count);
            return tagListGroups.Select(kvp => new KeyValuePair<string, IEnumerable<UADataPoint>>(kvp.Key, kvp.Value));
        }

        /// <summary>
        /// Groups data points by specified tag lists with custom names
        /// </summary>
        private IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>> GroupByTagListWithNames(
            IList<UADataPoint> dataPoints, Dictionary<string, List<string>> tagListsWithNames)
        {
            var tagListGroups = new Dictionary<string, List<UADataPoint>>();
            var processedDataPoints = new HashSet<string>();

            logger.LogTrace("[TAG_LIST_BASED] Processing {Count} datapoints against {ListCount} tag lists with custom names", 
                dataPoints.Count, tagListsWithNames.Count);

            // Process each named tag list
            foreach (var kvp in tagListsWithNames)
            {
                var groupName = kvp.Key;
                var tagList = kvp.Value;
                var groupDataPoints = new List<UADataPoint>();

                foreach (var tag in tagList)
                {
                    var matchingDataPoints = dataPoints.Where(dp => 
                        dp.Id.Equals(tag, StringComparison.OrdinalIgnoreCase) && 
                        !processedDataPoints.Contains(dp.Id)).ToList();

                    foreach (var dp in matchingDataPoints)
                    {
                        groupDataPoints.Add(dp);
                        processedDataPoints.Add(dp.Id);
                    }
                }

                if (groupDataPoints.Any())
                {
                    tagListGroups[groupName] = groupDataPoints;
                    logger.LogTrace("[TAG_LIST_BASED] Group '{GroupName}' contains {Count} datapoints", 
                        groupName, groupDataPoints.Count);
                }
            }

            // Handle unassigned data points
            var unassignedDataPoints = dataPoints.Where(dp => !processedDataPoints.Contains(dp.Id)).ToList();
            if (unassignedDataPoints.Any())
            {
                tagListGroups["unassigned"] = unassignedDataPoints;
                logger.LogTrace("[TAG_LIST_BASED] 'unassigned' group contains {Count} datapoints", 
                    unassignedDataPoints.Count);
            }

            logger.LogTrace("[TAG_LIST_BASED] Grouped into {GroupCount} tag list groups with custom names", tagListGroups.Count);
            return tagListGroups.Select(kvp => new KeyValuePair<string, IEnumerable<UADataPoint>>(kvp.Key, kvp.Value));
        }

        /// <summary>
        /// Groups data points based on OPC UA tag changes (subscription-based)
        /// Each data point is treated as individual subscription change
        /// </summary>
        private IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>> GroupByTagChange(
            IList<UADataPoint> dataPoints)
        {
            logger.LogTrace("[TAG_CHANGE_BASED] Processing {Count} datapoints as individual tag changes", 
                dataPoints.Count);

            // Group by tag ID to handle multiple values for same tag
            var tagGroups = dataPoints.GroupBy(dp => dp.Id)
                .Select(group => new KeyValuePair<string, IEnumerable<UADataPoint>>(group.Key, group.AsEnumerable()));

            logger.LogTrace("[TAG_CHANGE_BASED] Grouped into {GroupCount} tag change groups", tagGroups.Count());
            return tagGroups;
        }

        /// <summary>
        /// Groups data points using chunk-based strategy (existing behavior)
        /// </summary>
        private IEnumerable<KeyValuePair<string, IEnumerable<UADataPoint>>> GroupByChunk(
            IList<UADataPoint> dataPoints)
        {
            logger.LogTrace("[CHUNK_BASED] Processing {Count} datapoints using existing chunking strategy", 
                dataPoints.Count);

            // Group by tag ID first, then let AdaptiveChunker handle the chunking
            var tagGroups = dataPoints.GroupBy(dp => dp.Id)
                .ToDictionary(group => group.Key, group => group.AsEnumerable());

            return tagGroups.Select(kvp => new KeyValuePair<string, IEnumerable<UADataPoint>>(kvp.Key, kvp.Value));
        }

        /// <summary>
        /// Extracts root node identifier from datapoint ID using prefix-based approach
        /// </summary>
        private string ExtractRootNodeFromId(string datapointId)
        {
            try
            {
                // Extract root node based on ID structure
                // For KEPServer format like "kepkeps=S.D.Tag34", extract "kepkeps=S.D"
                // For format like "gpidKEPServerEX:s=Simulation Examples.Functions.Ramp1", 
                // extract "gpidKEPServerEX:s=Simulation Examples.Functions"
                
                // Handle format: "kepkeps=S.D.Tag34" -> extract "kepkeps=S.D"
                if (datapointId.Contains("=S."))
                {
                    var parts = datapointId.Split('.');
                    if (parts.Length >= 3) // kepkeps=S, D, Tag34
                    {
                        // Take first two parts: "kepkeps=S" + "D" = "kepkeps=S.D"
                        var rootGroup = $"{parts[0]}.{parts[1]}";
                        return rootGroup;
                    }
                }
                
                // Handle OPC UA format: "gpidKEPServerEX:s=Simulation Examples.Functions.Ramp1"
                if (datapointId.Contains(":s="))
                {
                    var parts = datapointId.Split(new[] { ":s=" }, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length >= 2)
                    {
                        var prefix = parts[0]; // "gpidKEPServerEX"
                        var nodePath = parts[1]; // "Simulation Examples.Functions.Ramp1"
                        
                        // Extract the first two levels of the node path for grouping
                        var nodeSegments = nodePath.Split('.');
                        if (nodeSegments.Length >= 2)
                        {
                            var rootGroup = $"{prefix}:s={nodeSegments[0]}.{nodeSegments[1]}";
                            return rootGroup;
                        }
                        else
                        {
                            var rootGroup = $"{prefix}:s={nodePath}";
                            return rootGroup;
                        }
                    }
                }
                
                // Fallback: use dot-separated grouping for other formats
                var segments = datapointId.Split('.');
                if (segments.Length >= 3)
                {
                    var rootGroup = string.Join(".", segments.Take(3));
                    return rootGroup;
                }
                else if (segments.Length >= 2)
                {
                    var rootGroup = string.Join(".", segments.Take(2));
                    return rootGroup;
                }
                else
                {
                    return segments[0];
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "[ROOT_NODE_BASED] Error extracting root node from ID: {Id}", datapointId);
                return "unassigned";
            }
        }
    }
} 