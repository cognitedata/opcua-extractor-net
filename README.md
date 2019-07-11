# opcua-extractor-net
OPC-UA extractor using libraries from OPCFoundation see [here](https://github.com/OPCFoundation/UA-.NETStandard)

## How to use
Add artifactory as a nuget source as outlined [here](https://cognitedata.atlassian.net/wiki/spaces/IDE/pages/711884992/Migrating+to+Artifactory)
then simply run `dotnet restore` to install dependencies.

To run, the simplest is just to install .net core 2.2 from [here](https://dotnet.microsoft.com/download) then run 
`donet run [optional config file]`. If no .yml config file is specified, config.yml in the application directory is used.
See the [example configuration](Extractor/config.example.yml) for a template.

It is also possible to compile it to an executable, though it will obviously be system specific (TODO, do this automatically)

### Configuration
The config file should contain description of most config options, a few are notable
 - `GlobalPrefix` and the `nsmaps` category are used to create ExternalIds, and if these are changed after the extractor
 has already been run, then new assets and timeseries will be created. The externalId will be on the form
 `[GlobalPrefix].[Mapped or full namespace];[Identifiertype and identifier]`. If GlobalPrefix is chosen to be unique within CDF
 then the ExternalId should remain unique.
 - `RootAssetId`, `RootNamespaceId` and `RootNodeId` are used to designate the root node and root asset, these are mapped such
 that the RootAsset and RootNode will be in the same place in the final hierarchy. In theory, multiple extractors could
 run in parallel against different top level assets/nodes.
 - Buffering is done by setting `BufferOnFailure` to true and specifying a `BufferFile`. If this is done, the extractor
 will write non-historizing datapoints to a binary file when the connection to CDF is down.

## Development
You will need .net core 2.2, and to set up artifactory as mentioned above. Then simply run `dotnet build` to compile,
or `dotnet run` to compile and run. Make sure that the config file is moved to the build directory, either by specifying
in the solution or by moving it manually.
