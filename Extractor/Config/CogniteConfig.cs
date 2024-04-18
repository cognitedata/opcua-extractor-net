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

using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils;
using CogniteSdk.Beta.DataModels;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;

namespace Cognite.OpcUa.Config
{
    public class CognitePusherConfig : CogniteConfig, IPusherConfig
    {
        /// <summary>
        /// DEPRECATED. Data set to use for new objects. Existing objects will not be updated.
        /// </summary>
        public long? DataSetId { get; set; }
        /// <summary>
        /// DEPRECATED. Data set to use for new objects, overridden by data-set-id. Requires the capability datasets:read for the given data set.
        /// </summary>
        public string? DataSetExternalId { get; set; }
        /// <summary>
        /// Data set to use for new objects. Requires the capability datasets:read if external-id is used.
        /// </summary>
        public DataSetConfig? DataSet { get; set; }
        /// <summary>
        /// DEPRECATED. Debug mode, if true, Extractor will not push to target
        /// </summary>
        public bool Debug { get; set; }
        /// <summary>
        /// Whether to read start/end-points on startup, where possible. At least one pusher should be able to do this,
        /// otherwise back/frontfill will run for the entire history every restart.
        /// The CDF pusher is not able to read start/end points for events, so if reading historical events is enabled, one other pusher
        /// able to do this should be enabled.
        /// The state-store can do all this, if the state-store is enabled this can still be enabled if timeseries have been deleted from CDF
        /// and need to be re-read from history.
        /// </summary>
        [DefaultValue(true)]
        public bool ReadExtractedRanges { get; set; } = true;
        /// <summary>
        /// Do not push any metadata at all. If this is true, plain timeseries without metadata will be created,
        /// similarly to raw-metadata, and datapoints will be pushed. Nothing will be written to raw, and no assets will be created.
        /// Events will be created, but without asset context.
        /// </summary>
        [Obsolete("Deprecated!")]
        public bool SkipMetadata { get; set; }
        /// <summary>
        /// Store assets and/or timeseries data in raw. Assets will not be created at all,
        /// timeseries will be created with just externalId, isStep and isString.
        /// Both timeseries and assets will be persisted in their entirety to raw.
        /// Datapoints are not affected, events will be created, but without asset context. The externalId
        /// of the source node is added to metadata if applicable.
        /// Use different table names for assets and timeseries.
        /// </summary>
        [Obsolete("Deprecated! Use MetadataTargetsConfig.RawMetadataTargetConfig instead.")]
        public RawMetadataConfig? RawMetadata { get; set; }
        /// <summary>
        /// Map metadata to asset/timeseries attributes. Each of "assets" and "timeseries" is a map from property DisplayName to
        /// CDF attribute. Legal attributes are "name, description, parentId" and "unit" for timeseries. "parentId" must somehow refer to
        /// an existing asset. For timeseries it must be a mapped asset, for assets it can be any asset.
        /// Example usage:
        /// timeseries:
        ///    "EngineeringUnits": "unit"
        ///    "EURange": "description"
        /// assets:
        ///    "Name": "name"
        /// </summary>
        public MetadataMapConfig? MetadataMapping { get; set; }
        /// <summary>
        /// Read from CDF instead of OPC-UA when starting, to speed up start on slow servers.
        /// Requires extraction.data-types.expand-node-ids and append-internal-values to be set to true.
        ///
        /// This should generally be enabled along with skip-metadata or raw-metadata
        /// If browse-on-empty is set to true, and raw-metadata is configured with the same
        /// database and tables, the extractor will read into raw on first run, then use raw later,
        /// and the raw database can be deleted to reset on next read.
        /// </summary>
        public CDFNodeSourceConfig? RawNodeBuffer { get; set; }
        /// <summary>
        /// Replacement for NaN values.
        /// </summary>
        public double? NonFiniteReplacement
        {
            get => NanReplacement;
            set => NanReplacement = value == null || double.IsFinite(value.Value)
                && value.Value > CogniteUtils.NumericValueMin
                && value.Value < CogniteUtils.NumericValueMax ? value : null;
        }
        /// <summary>
        /// Specification for a CDF function that is called after nodes are pushed to CDF,
        /// reporting the number changed.
        /// </summary>
        public BrowseCallbackConfig? BrowseCallback { get; set; }

        /// <summary>
        /// There is no good way to mark relationships as deleted, so they are hard-deleted.
        /// This has to be enabled to delete relationships deleted from OPC-UA. This requires extraction.deletes to be enabled.
        /// Alternatively, use Raw, where relationships can be marked as deleted.
        /// </summary>
        public bool DeleteRelationships { get; set; }

        /// <summary>
        /// This is the implementation of the metadata targets 
        /// </summary>
        public MetadataTargetsConfig? MetadataTargets { get; set; }
    }
    public class RawMetadataConfig
    {
        /// <summary>
        /// Database to store data in, required.
        /// </summary>
        [Required]
        public string? Database { get; set; }
        /// <summary>
        /// Table to store assets in.
        /// </summary>
        public string? AssetsTable { get; set; }
        /// <summary>
        /// Table to store timeseries in.
        /// </summary>
        public string? TimeseriesTable { get; set; }
        /// <summary>
        /// Table to store relationships in
        /// </summary>
        public string? RelationshipsTable { get; set; }
    }
    public class MetadataTargetsConfig
    {
        /// <summary>
        /// Raw metadata targets config
        /// </summary>
        public RawMetadataTargetConfig? Raw { get; set; }
        /// <summary>
        /// Clean metadata targets config
        /// </summary>
        public CleanMetadataTargetConfig? Clean { get; set; }
        /// <summary>
        /// FDM destination config
        /// </summary>
        public FdmDestinationConfig? DataModels { get; set; }
    }
    public class RawMetadataTargetConfig
    {
        public string? Database { get; set; }
        public string? AssetsTable { get; set; }
        public string? TimeseriesTable { get; set; }
        public string? RelationshipsTable { get; set; }
    }
    public class CleanMetadataTargetConfig
    {
        public bool Assets { get; set; }
        public bool Timeseries { get; set; }
        public bool Relationships { get; set; }
    }
    public class MetadataMapConfig
    {
        public Dictionary<string, string>? Assets { get; set; }
        public Dictionary<string, string>? Timeseries { get; set; }
    }
    public class CDFNodeSourceConfig
    {
        /// <summary>
        /// Enable the raw node buffer.
        /// </summary>
        public bool Enable { get; set; }
        /// <summary>
        /// Raw database to read from.
        /// </summary>
        public string? Database { get; set; }
        /// <summary>
        /// Table to read assets from, for events.
        /// </summary>
        public string? AssetsTable { get; set; }
        /// <summary>
        /// Table to read timeseries from.
        /// </summary>
        public string? TimeseriesTable { get; set; }
        /// <summary>
        /// Run normal browse if nothing is found when reading from CDF, either because the tables are empty, or they do not exist.
        /// No valid nodes must be found to run this at all, meaning it may run if there are nodes, but none of them are
        /// potentially valid extraction targets.
        /// </summary>
        public bool BrowseOnEmpty { get; set; }
    }
    public class BrowseCallbackConfig : FunctionCallConfig
    {
        /// <summary>
        /// Call callback even if zero items are created or updated.
        /// </summary>
        public bool ReportOnEmpty { get; set; }
    }

    public enum TypesToMap
    {
        Referenced,
        Custom,
        All
    }

    public class FdmDestinationConfig
    {
        public class ModelInfo
        {
            public ModelInfo(FdmDestinationConfig config)
            {
                ModelSpace = config.ModelSpace ?? throw new ConfigurationException("data-models.model-space is required when writing to data models is enabled");
                InstanceSpace = config.InstanceSpace ?? throw new ConfigurationException("data-models.instance-space is required when writing to data models is enabled");
                ModelVersion = config.ModelVersion ?? throw new ConfigurationException("data-models.model-version is required when writing to data models is enabled");
            }

            public string ModelSpace { get; }
            public string InstanceSpace { get; }
            public string ModelVersion { get; }

            public FDMExternalId FDMExternalId(string externalId)
            {
                return new FDMExternalId(externalId, ModelSpace, ModelVersion);
            }

            public ViewIdentifier ViewIdentifier(string externalId)
            {
                return new ViewIdentifier(ModelSpace, externalId, ModelVersion);
            }

            public ContainerIdentifier ContainerIdentifier(string externalId)
            {
                return new ContainerIdentifier(ModelSpace, externalId);
            }
        }

        /// <summary>
        /// Space to create models in.
        /// </summary>
        public string? ModelSpace { get; set; }
        /// <summary>
        /// Space to create instances in. Can be the same as ModelSpace.
        /// </summary>
        public string? InstanceSpace { get; set; }

        /// <summary>
        /// Version string used for model and view.
        /// </summary>
        public string? ModelVersion { get; set; }
        /// <summary>
        /// True to enable. This will not produce meaningful results unless
        /// extraction.types.as-nodes, extraction.relationships.enabled, extraction.relationships.hierarchical,
        /// are all set to "true", and there is exactly one root node i=84
        /// </summary>
        public bool Enabled { get; set; }

        /// <summary>
        /// Enum for which types to map to FDM.
        /// Note that setting this to "All" tends to not work that well. It is rarely something you want.
        /// The extractor can handle it, but it will produce a lot of types, and no application will ever need all of them.
        /// Still, it is easy to add for completeness.
        /// </summary>
        public TypesToMap TypesToMap { get; set; } = TypesToMap.Custom;

        /// <summary>
        /// Do not create views without an associated container.
        /// Simplifies the model greatly.
        /// </summary>
        public bool SkipSimpleTypes { get; set; }

        /// <summary>
        /// Let mandatory options be nullable.
        /// 
        /// Lots of servers don't do this properly.
        /// </summary>
        public bool IgnoreMandatory { get; set; }

        /// <summary>
        /// Target connections on the form "Type"."Property": "Target"
        /// Useful for certain schemas.
        /// </summary>
        public Dictionary<string, string>? ConnectionTargetMap { get; set; }

        /// <summary>
        /// Enable deleting FDM nodes. These are generally hard deletes.
        /// Will only delete nodes extracted as part of the instance hierarchy.
        /// Types, and type-related nodes will not be deleted.
        /// </summary>
        public bool EnableDeletes { get; set; }

        /// <summary>
        /// Maximum number of parallel instance insertion requests.
        /// </summary>
        public int InstanceParallelism { get; set; } = 4;

        /// <summary>
        /// Number of instances per instance create request.
        /// </summary>
        public int InstanceChunk { get; set; } = 1000;

        /// <summary>
        /// Number of views and containers per create request.
        /// </summary>
        public int ModelChunk { get; set; } = 100;
    }
}
