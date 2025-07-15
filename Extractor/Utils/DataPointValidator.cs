using System;
using Cognite.OpcUa.Types;
using Cognite.OpcUa.Config;
using Cognite.OpcUa.History;
using Cognite.Extensions;
using Microsoft.Extensions.Logging;
using Opc.Ua;

namespace Cognite.OpcUa.Utils
{
    /// <summary>
    /// Consolidated data point validation logic
    /// </summary>
    public static class DataPointValidator
    {
        private static readonly DateTime MinDateTime = DateTime.Parse("1900-01-01T00:00:00Z").ToUniversalTime();
        
        /// <summary>
        /// Validates and preprocesses a DataValue before conversion to UADataPoint
        /// </summary>
        /// <param name="datapoint">Original DataValue</param>
        /// <param name="node">Variable extraction state</param>
        /// <param name="config">Full configuration</param>
        /// <param name="log">Logger</param>
        /// <returns>ValidationResult with processed data or null if invalid</returns>
        public static ValidationResult? ValidateAndPreprocess(
            DataValue datapoint, 
            VariableExtractionState node, 
            FullConfig config, 
            ILogger log)
        {
            // Status code validation
            if (StatusCode.IsNotGood(datapoint.StatusCode))
            {
                UAExtractor.BadDataPoints.Inc();

                if (config.Subscriptions.LogBadValues)
                {
                    log.LogDebug("Bad streaming datapoint: {BadDatapointExternalId} {SourceTimestamp}. Value: {Value}, Status: {Status}",
                        node.Id, datapoint.SourceTimestamp, datapoint.Value, ExtractorUtils.GetStatusCodeName((uint)datapoint.StatusCode));
                }

                switch (config.Extraction.StatusCodes.StatusCodesToIngest)
                {
                    case StatusCodeMode.All:
                        break;
                    case StatusCodeMode.Uncertain:
                        if (!StatusCode.IsUncertain(datapoint.StatusCode))
                        {
                            return null;
                        }
                        break;
                    case StatusCodeMode.GoodOnly:
                        return null;
                }
            }

            // Timestamp validation
            if (datapoint.SourceTimestamp < MinDateTime)
            {
                return null;
            }

            return new ValidationResult
            {
                DataValue = datapoint,
                Node = node,
                IsValid = true
            };
        }

        /// <summary>
        /// Validates UADataPoint for MQTT transmission
        /// </summary>
        /// <param name="datapoint">UADataPoint to validate</param>
        /// <param name="config">MQTT configuration</param>
        /// <param name="log">Logger</param>
        /// <returns>Validated and potentially modified UADataPoint, or null if invalid</returns>
        public static UADataPoint? ValidateForMqtt(UADataPoint datapoint, MqttPusherConfig config, ILogger log)
        {
            var dp = datapoint;

            // Non-finite value handling
            if (!dp.IsString && dp.DoubleValue.HasValue && !double.IsFinite(dp.DoubleValue.Value))
            {
                if (config.NonFiniteReplacement != null)
                {
                    dp = new UADataPoint(dp, config.NonFiniteReplacement.Value);
                }
                else
                {
                    return null;
                }
            }

            // Numeric range validation
            if (!dp.IsString && dp.DoubleValue.HasValue && 
                (dp.DoubleValue >= CogniteUtils.NumericValueMax || dp.DoubleValue <= CogniteUtils.NumericValueMin))
            {
                if (config.NonFiniteReplacement != null)
                {
                    dp = new UADataPoint(dp, config.NonFiniteReplacement.Value);
                }
                else
                {
                    return null;
                }
            }

            // String null handling
            if (dp.IsString && dp.StringValue == null)
            {
                dp = new UADataPoint(dp, "");
            }

            return dp;
        }
    }

    /// <summary>
    /// Result of data validation
    /// </summary>
    public class ValidationResult
    {
        public DataValue DataValue { get; set; } = null!;
        public VariableExtractionState Node { get; set; } = null!;
        public bool IsValid { get; set; }
    }
} 