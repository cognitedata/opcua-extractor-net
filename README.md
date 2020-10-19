# opcua-extractor-net
[Build](https://cd.jenkins.cognite.ai/job/cognitedata-cd/job/opcua-extractor-net/job/master) 
[![codecov](https://codecov.io/gh/cognitedata/opcua-extractor-net/branch/master/graph/badge.svg?token=SS8CBL93bW)](https://codecov.io/gh/cognitedata/opcua-extractor-net)

OPC-UA extractor using libraries from OPCFoundation see [here](https://github.com/OPCFoundation/UA-.NETStandard)

## How to use

### Building
Install .net core 3.0 from [here](https://dotnet.microsoft.com/download).

To run:
`dotnet run -- project ExtractorLauncher/`.

There are a few environment variables that may be used:
 - OPCUA_CONFIG_DIR
 - OPCUA_CERTIFICATE_DIR

Beyond this, the .yml config file also supports environment variable substitution like `${SOME_ENVIRONMENT_VARIABLE}`.

For `CONFIG_DIR`, default is `[application dir]/config`. `CERTIFICATE_DIR` is used for opc-ua certificates, and defaults to `[application dir]/certificates`.

See the [example configuration](config/config.example.yml) for a config template.

### Using Docker
Simply download and run the latest build from [here](https://console.cloud.google.com/gcr/images/cognitedata/EU/opcua-extractor-net?gcrImageListsize=30)

Config, both opcua config `opc.ua.extractor.Config.xml` and `config.yml` are located in a volume /config and certificates are located in subfolders of the volume /certificates. Example:

`docker run -v "$(pwd)/config:/config" -v "$(pwd)/certificates:/certificates eu.gcr.io/cognitedata/opcua-extractor-net:tag`

which would run the build tagged with `tag` using config stored in `current_dir/config`, and output logs to `current_dir/logs`

### Binaries
There will be binaries for the most recent release here on github. There are three different builds:
 - windows81 x64, which should work for windows server 2012.
 - windows x64, which should work for newer versions of windows (the 81 version will probably work for newer versions as well)
 - linux x64, which should work on linux.
There should be a system specific executable at the top level, which launches the extractor. These should be able to run with no extra setup.

### Command line arguments
The extractor also takes a few command line arguments for convenience, though it can run without any:
 - `-t|--tool` - Run the configuration tool
 - `-h|--host [host]` - Override configured OPC-UA endpoint
 - `-u|--user [username]` - Override configured OPC-UA username
 - `-p|--password [password]` - Override configured OPC-UA password
 - `-s|--secure` - Use a secure connection to OPC-UA
 - `-f|--config-file [path]` - Set the config-file path. Overrides config-dir for .yml config files
 - `-a|--auto-accept` - Auto-accept server certificates
 - `-d|--config-dir [path]` - Set the path to the config directory
 - `-ct|--config-target [path]` - Set the path to the output file for the config tool. By default [config-dir]/config.config-tool-output.yml. This file is overwritten.
 - `-nc|--no-config` - Do not attempt to load yml config files. The OPC-UA XML config file will still be needed.
 - `-l|--log-level` - Set the console log-level [fatal/error/warning/information/debug/verbose].
 - `-x|--exit` - Exit the extractor on failure, equivalent to Source.ExitOnFailure.

### Configuration
Documentation can be found [here](https://cognitedata.atlassian.net/wiki/spaces/DSC/pages/1049264826/OPC+UA+Extractor)

## Development
You will need .net core 3.0. Then simply run `dotnet build` to compile,
or `dotnet run --project ExtractorLauncher` to compile and run.

For testing metrics, a good solution is the prom-stack found [here](https://github.com/evnsio/prom-stack)

### Testing
There is a test script `test.sh`. To run the tests locally, either use that or simply run `dotnet test`.

### Releasing
The release.sh script just creates a new tag on the current commit, then pushes it to `origin`, which should be this repository. If the CI is run on a commit with a tag, it automatically
deploys to github releases. It always deploys docker images to eu.gcr.io if run on master. The version is generated from `git describe`, which uses the number of commits since last tag.
