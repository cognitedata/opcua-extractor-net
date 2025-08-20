﻿/* Cognite Extractor for OPC-UA
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
using Cognite.Extractor.Utils.Unstable.Configuration;
using CogniteSdk.DataModels;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;

namespace Cognite.OpcUa.Config
{
    public class CognitePusherConfig : BaseCogniteConfig, IPusherConfig
    {
        /// <summary>
        /// Data set to use for new objects. Requires the capability datasets:read if external-id is used.
        /// </summary>
        public DataSetConfig? DataSet { get; set; }
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
        /// There is no good way to mark relationships as deleted, so they are hard-deleted.
        /// This has to be enabled to delete relationships deleted from OPC-UA. This requires extraction.deletes to be enabled.
        /// Alternatively, use Raw, where relationships can be marked as deleted.
        /// </summary>
        public bool DeleteRelationships { get; set; }

        /// <summary>
        /// This is the implementation of the metadata targets 
        /// </summary>
        public MetadataTargetsConfig? MetadataTargets { get; set; }

        /// <summary>
        /// Configuration for writing events to records.
        /// </summary>
        public RecordsConfig? Records { get; set; }
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

        public string? Space { get; set; }
        public string? Source { get; set; }
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

    public class RecordsConfig
    {
        /// <summary>
        /// The space any containers will be created in.
        /// </summary>
        public string? ModelSpace { get; set; }
        /// <summary>
        /// The space set on the created logs.
        /// </summary>
        public string? LogSpace { get; set; }
        /// <summary>
        /// The stream logs are written to.
        /// </summary>
        public string? Stream { get; set; }
        /// <summary>
        /// Write node ID properties directly, on the form `ns=2;i=123`, instead of
        /// creating direct relations with external IDs on the form `[prefix][namespace][identifier]`
        /// </summary>
        public bool UseRawNodeId { get; set; }
        /// <summary>
        /// Use reversible JSON encoding.
        /// </summary>
        public bool UseReversibleJson { get; set; }
    }

}
