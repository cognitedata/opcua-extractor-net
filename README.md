# opcua-extractor-net
[![codecov](https://codecov.io/gh/cognitedata/opcua-extractor-net/branch/master/graph/badge.svg?token=SS8CBL93bW)](https://codecov.io/gh/cognitedata/opcua-extractor-net)

OPC-UA extractor using libraries from OPCFoundation see [here](https://github.com/OPCFoundation/UA-.NETStandard)

## How to use

### Setup

Install using `dotnet tool restore`. Then run `dotnet restore` to install packages.

Now you can build projects using `dotnet build [Project]`. Using just `dotnet build` is likely to fail on "OpcUaServiceManager", since it cannot be built
with the dotnet CLI.

### Building

Install .NET 8.0 from [here](https://dotnet.microsoft.com/download).

To run:
`dotnet run --project ExtractorLauncher/`.

There are a few environment variables that may be used:

- OPCUA_CONFIG_DIR
- OPCUA_CERTIFICATE_DIR

Beyond this, the .yml config file also supports environment variable substitution like `${SOME_ENVIRONMENT_VARIABLE}`.

For `CONFIG_DIR`, default is `[application dir]/config`. `CERTIFICATE_DIR` is used for opc-ua certificates, and defaults to `[application dir]/certificates`.

See the [example configuration](config/config.example.yml) for a config template.

### Using Docker

There are docker images of each release at dockerhub: opcua-extractor-net.

Config, both opcua config `opc.ua.extractor.Config.xml` and `config.yml` are located in a volume /config and certificates are located in subfolders of the volume /certificates. Example:

`docker run -v "$(pwd)/config:/config" -v "$(pwd)/certificates:/certificates opcua-extractor-net:tag`

which would run the build tagged with `tag` using config stored in `current_dir/config`.

### Binaries

There will be binaries for the most recent release here on github. There are two different builds:

- windows x64, which should work for newer versions of windows (the 81 version will probably work for newer versions as well)
- linux x64, which should work on linux and osx.

There should be a system specific executable at the top level, which launches the extractor. These should be able to run with no extra setup.

In addition there are .deb, .rpm and .msi installers.

### Command line arguments

The extractor also takes a few command line arguments for convenience, use `OpcuaExtractor -h` for documentation.

### Configuration

Documentation can be found [here](https://cognitedata.atlassian.net/wiki/spaces/DSC/pages/1049264826/OPC+UA+Extractor)

## Local setup

If you do not have access to an OPC-UA server, and wish to experiment with the extractor, you can run the test server, which is found in releases along with the version of the extractor it was used to test.

You can also build and run it by following the instructions in "Setup" above, then running `dotnet run --project Server`. Use `dotnet run --project Server -- [options]` to add command line options when running it this way.

The server has a command line interface, and is capable of simulating a lot of different server behavior. Run Server -h for documentation.

By default it runs on `opc.tcp://localhost:62546`, so that is where you would connect with UAExpert or the extractor.

The server is capable of generating history for events and simple and complex datapoints, as well as generating periodic updates, events and changes over time.
It can also optionally simulate some types of buggy server behavior.

## Development

You will need .NET 8.0. Then simply run `dotnet build` to compile,
or `dotnet run --project ExtractorLauncher` to compile and run.

The compiler may complain about OpcUaExtractorSetup and OpcUaServiceManager, which aren't generally necessary to compile during development.
You can use `dotnet build Test` or `dotnet build ExtractorLauncher` to only compile some parts.

For testing metrics, a good solution is the prom-stack found [here](https://github.com/evnsio/prom-stack)

### Testing

To run the tests locally, run `dotnet test`, or use the `test.sh` script.

Some tests require an instance of influxdb 1.8 found [here](https://portal.influxdata.com/downloads/) running on port 8086,
and some require a version of mosquitto, found [here](https://mosquitto.org/) running on port 4060. The tests now run their own OPC-UA servers.

During development it is perfectly fine to just run a subset of the tests using `dotnet test --filter MyTestMethod`. All tests run as part of CI.

### Releasing

Update `manifest.yaml` with the new release`. Merging to master will automatically
deploy binaries to github releases and docker images to dockerhub. For
the MSI to build, the version must be simple, i.e. on the form "a.b.c".

## Contributing

Please follow the code style of the rest of the code, meaning:

- Public and protected members are properties and Capitalized.
- Private members are not prefixed, and should be camelCase.
- Curly braces on the next line.

In general just make sure the code remains consistent. The code has nullable analysis enabled, so make sure to fix any warnings. You should add tests for any new behavior or fixes.

This project adheres to [Contributor Covenant v2.0](https://www.contributor-covenant.org/version/2/0/code_of_conduct/) as a code of conduct.

## MQTT Transmission Strategy Metadata

The OPC-UA extractor now supports enhanced metadata in MQTT JSON messages based on the configured transmission strategy. This feature automatically adds strategy-specific information to the metadata section of JSON messages.

### Configuration

Configure the transmission strategy in your `config.yml`:

```yaml
mqtt:
  use-grpc: false  # Required for JSON format with metadata
  include-metadata: true
  json-format-type: Subscription  # Recommended for rich metadata support
  
  mqtt-transmission-strategy:
    data-group-by: ROOT_NODE_BASED  # Choose your strategy
    
    # For TAG_LIST_BASED strategy:
    # tag-lists:
    #   - ["sensor1", "sensor2", "sensor3"]
    #   - ["pump1", "pump2"]
```

### Transmission Strategies and Metadata

#### ROOT_NODE_BASED
Groups data by root nodes and adds `root_node_name` to metadata:
```json
{
  "metadata": {
    "data_ingest_type": "subscription",
    "message_timestamp": 1648234567890,
    "root_node_name": "kepkeps=S.D"
  },
  "tags_data": [...]
}
```

#### CHUNK_BASED (Default)
Uses chunking strategy and adds unique `chunk_id` based on sequence numbers:
```json
{
  "metadata": {
    "data_ingest_type": "subscription",
    "message_timestamp": 1648234567890,
    "chunk_id": "chunk_123_456_1648234567890"
  },
  "tags_data": [...]
}
```

#### TAG_LIST_BASED
Groups data by configured tag lists and adds `tag_list_name`:

**New Format (with custom names):**
```yaml
mqtt:
  mqtt-transmission-strategy:
    data-group-by: TAG_LIST_BASED
    tag-list-groups:
      - tag-list-name: "sensors_group"
        tags:
          - "s=S.A.Tag1"
          - "s=S.A.Tag2"
          - "s=S.A.Tag3"
      - tag-list-name: "pumps_group"
        tags:
          - "s=S.B.Pump1"
          - "s=S.B.Pump2"
```

**Output:**
```json
{
  "metadata": {
    "data_ingest_type": "subscription",
    "message_timestamp": 1648234567890,
    "tag_list_name": "sensors_group"
  },
  "tags_data": [
    {
      "tag": "s=S.A.Tag1",
      "data": [
        {
          "timestamp": 1648234567890,
          "value": 124,
          "sc": 0,
          "dt": "int32"
        }
      ]
    },
    {
      "tag": "s=S.A.Tag2",
      "data": [
        {
          "timestamp": 1648234567890,
          "value": "test",
          "sc": 0,
          "dt": "string" 
        }
      ]
    }
  ]
}
```

**Legacy Format:**
```yaml
mqtt:
  mqtt-transmission-strategy:
    data-group-by: TAG_LIST_BASED
    tag-lists:
      - ["tag1", "tag2", "tag3"]
      - ["tag4", "tag5"]
```

Legacy format output:
```json
{
  "metadata": {
    "data_ingest_type": "subscription",
    "message_timestamp": 1648234567890,
    "tag_list_name": "tag_list_1"
  },
  "tags_data": [...]
}
```

#### TAG_CHANGE_BASED
Sends individual tag changes and adds `changed_tag`:
```json
{
  "metadata": {
    "data_ingest_type": "subscription",
    "message_timestamp": 1648234567890,
    "changed_tag": "sensor1"
  },
  "tags_data": [...]
}
```

### Backward Compatibility

The extractor maintains backward compatibility with legacy flat configuration:
```yaml
mqtt:
  mqtt-transmission-strategy: CHUNK_BASED  # Legacy format
```
