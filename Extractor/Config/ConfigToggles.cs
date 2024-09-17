namespace Cognite.OpcUa.Config
{
    /// <summary>
    /// Class wrapping a config instance, enabling features that may be enabled from different places.
    /// </summary>
    public class ConfigToggles
    {
        private readonly FullConfig config;
        public ConfigToggles(FullConfig config)
        {
            this.config = config;
        }

        private bool FdmEnabled => config.Cognite?.MetadataTargets?.DataModels?.Enabled ?? false;

        /// <summary>
        /// Should the extractor load references to and from properties?
        /// </summary>
        public bool MapPropertyReferences => FdmEnabled;

        /// <summary>
        /// Should the extractor get references to types when loading non-hierarchical references?
        /// </summary>
        public bool GetNonHierarchicalTypeReferences => FdmEnabled || config.Extraction.NodeTypes.AsNodes;
        /// <summary>
        /// Should the extractor create unmapped nodes it encounters when reading non-hierarchical references, or just ignore them?
        /// </summary>
        public bool CreateUnknownReferencedNodes => FdmEnabled || config.Extraction.Relationships.CreateReferencedNodes;
        /// <summary>
        /// Should the extractor create nodes that are types in OPC-UA?
        /// Any node with NodeClass ObjectType, ReferenceType, VariableType, or DataType is considered
        /// a type.
        /// </summary>
        public bool LoadTypesAsNodes => FdmEnabled || config.Extraction.NodeTypes.AsNodes;

        // Type manager
        /// <summary>
        /// Should the type manager load data types when reading the type hierarchy?
        /// </summary>
        public bool LoadDataTypes => FdmEnabled || config.Extraction.DataTypes.AutoIdentifyTypes;
        /// <summary>
        /// Should the type manager load event types when reading the type hierarchy?
        /// If LoadTypeDefinitions is true, this is enabled implicitly, since event types
        /// are also type definitions.
        /// </summary>
        public bool LoadEventTypes => config.Events.Enabled;
        /// <summary>
        /// Should the type manager load reference types when reading the type hierarchy?
        /// </summary>
        public bool LoadReferenceTypes => FdmEnabled || config.Extraction.Relationships.Enabled;
        /// <summary>
        /// Should the type manager load object and variable types when reading the type hierarchy?
        /// </summary>
        public bool LoadTypeDefinitions => FdmEnabled;
        /// <summary>
        /// Should the type manager store references between types in the type hierarchy?
        /// </summary>
        public bool LoadTypeReferences => FdmEnabled;
    }
}
