# opcua-extractor-net
[Build](https://cd.jenkins.cognite.ai/job/cognitedata-cd/job/opcua-extractor-net/job/master) 
[![codecov](https://codecov.io/gh/cognitedata/opcua-extractor-net/branch/master/graph/badge.svg?token=SS8CBL93bW)](https://codecov.io/gh/cognitedata/opcua-extractor-net)

OPC-UA extractor using libraries from OPCFoundation see [here](https://github.com/OPCFoundation/UA-.NETStandard)

## How to use

### Setup
Install paket using `dotnet tool restore`. Then run `dotnet paket restore` to install packages.

Now you can build projects using `dotnet build [Project]`. Using just `dotnet build` is likely to fail on "OpcUaServiceManager", since it cannot be built
with the dotnet CLI.

### Building
Install .net 6.0 from [here](https://dotnet.microsoft.com/download).

To run:
`dotnet run --project ExtractorLauncher/`.

There are a few environment variables that may be used:
 - OPCUA_CONFIG_DIR
 - OPCUA_CERTIFICATE_DIR

Beyond this, the .yml config file also supports environment variable substitution like `${SOME_ENVIRONMENT_VARIABLE}`.

For `CONFIG_DIR`, default is `[application dir]/config`. `CERTIFICATE_DIR` is used for opc-ua certificates, and defaults to `[application dir]/certificates`.

See the [example configuration](config/config.example.yml) for a config template.

### Using Docker
Simply download and run the latest build from [here](https://console.cloud.google.com/gcr/images/cognitedata/EU/opcua-extractor-net?gcrImageListsize=30).

There are docker images of each release at: eu.gcr.io/cognitedata/opcua-extractor-net and eu.gcr.io/cognite-registry/opcua-extractor-net.

Config, both opcua config `opc.ua.extractor.Config.xml` and `config.yml` are located in a volume /config and certificates are located in subfolders of the volume /certificates. Example:

`docker run -v "$(pwd)/config:/config" -v "$(pwd)/certificates:/certificates eu.gcr.io/cognitedata/opcua-extractor-net:tag`

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

You can also build and run it `by following the instructions in "Setup" above, then running `dotnet run --project Server`. Use `dotnet run --project Server -- [options]` to add command line options when running it this way.

The server has a command line interface, and is capable of simulating a lot of different server behavior. Run Server -h for documentation.

By default it runs on `opc.tcp://localhost:62546`, so that is where you would connect with UAExpert or the extractor.

The server is capable of generating history for events and simple and complex datapoints, as well as generating periodic updates, events and changes over time.
It can also optionally simulate some types of buggy server behavior.

If the server is run with pubsub enabled, a local instance of mosquitto must be running for the server to start at all. This is due to a bug in the OPC-UA SDK.

## Development
You will need .net 6.0. Then simply run `dotnet build` to compile,
or `dotnet run --project ExtractorLauncher` to compile and run.

The compiler may complain about OpcUaExtractorSetup, which isn't generally necessary to compile during development.
You can use `dotnet build Test` or `dotnet build ExtractorLauncher` to only compile some parts.

For testing metrics, a good solution is the prom-stack found [here](https://github.com/evnsio/prom-stack)

### Testing
To run the tests locally, run `dotnet test`, or use the `test.sh` script. 

Some tests require an instance of influxdb 1.8 found [here](https://portal.influxdata.com/downloads/) running on port 8086,
and some require a version of mosquitto, found [here](https://mosquitto.org/) running on port 4060. The tests now run their own OPC-UA servers.

### Releasing
The release.sh script just creates a new tag on the current commit, then pushes it to `origin`, which should be this repository. If the CI is run on a commit with a tag, it automatically
deploys to github releases and docker images to eu.gcr.io/cognitedata/, and eu.gcr.io/cognite-registry/.
