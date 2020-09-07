# opcua-extractor-net
[![build](https://webhooks.dev.cognite.ai/build/buildStatus/icon?job=github-builds/opcua-extractor-net/master)](https://jenkins.cognite.ai/job/github-builds/job/opcua-extractor-net/job/master/)
[![codecov](https://codecov.io/gh/cognitedata/opcua-extractor-net/branch/master/graph/badge.svg?token=SS8CBL93bW)](https://codecov.io/gh/cognitedata/opcua-extractor-net)

OPC-UA extractor using libraries from OPCFoundation see [here](https://github.com/OPCFoundation/UA-.NETStandard)

## How to use

### Building
Install .net core 3.0 from [here](https://dotnet.microsoft.com/download).

To run:
`dotnet run -- project ExtractorLauncher/`.

There are a few environment variables that may be used:
 - OPCUA_LOGGER_DIR
 - OPCUA_CONFIG_DIR
 - OPCUA_CERTIFICATE_DIR

For CONFIG_DIR, default is `[application dir]/config`, CERTIFICATE_DIR is used for opcua certificates, and defaults to `[application dir]/certificates`,
LOGGER_DIR defaults to `[application dir]/logs`

See the [example configuration](config/config.example.yml) for a config template.

### Using Docker
Simply download and run the latest build from [here](https://console.cloud.google.com/gcr/images/cognitedata/EU/opcua-extractor-net?gcrImageListsize=30)

Config, both opcua config `opc.ua.extractor.Config.xml` and `config.yml` are located in a volume /config, and logfiles if enabled will be located in volume /logs, certificates are located in subfolders of the volume /certificates. Example:

`docker run -v "$(pwd)/config:/config" -v "$(pwd)/logs:/logs" -v "$(pwd)/certificates:/certificates eu.gcr.io/cognitedata/opcua-extractor-net:tag`

which would run the build tagged with `tag` using config stored in `current_dir/config`, and output logs to `current_dir/logs`

### Binaries
There will be binaries for the most recent release here on github. There are three different builds:
 - windows81 x64, which should work for windows server 2012.
 - windows x64, which should work for newer versions of windows (the 81 will probably work for never versions as well)
 - linux x64, which should work on linux.
There should be a system specific executable at the top level, which launches the extractor. These should be able to run with no extra setup.

### Configuration
Documentation can be found [here](https://cognitedata.atlassian.net/wiki/spaces/DSC/pages/1049264826/OPC+UA+Extractor)

## Development
You will need .net core 3.0. Then simply run `dotnet build` to compile,
or `dotnet run --project ExtractorLauncher` to compile and run.

For testing metrics, a good solution is the prom-stack found [here](https://github.com/evnsio/prom-stack)

### Testing
There is a test script under Testing/test.sh. To run the tests locally, you should first start the python test servers, using `startservers.sh`. There are four test servers currently.

 - Basicserver, server-test1.py, is a small server with two historizing nodes and a few objects. 4840
 - Fullserver, server-test2.py has around 2000 variables and a 30 layer deep tree of 150 objects. 4841
 - Arrayserver, server-test3.py is similar to Basicserver, but contains a couple of string and array variables to test more complex data. 4842
 - Eventserver, server-test4.py contains a large number of events, with multiple emitters and event types. 4843
 - Auditingserver, server-test5.py is a server that periodically adds new references and nodes, then triggers auditing events on the server node. 4844

To add more servers just add them to the servers folder, then edit the startservers.sh script to start the new server in the background. Make sure to use a different port.

### Releasing
The release.sh script just creates a new tag on the current commit, then pushes it to `origin`, which should be this repository. If the CI is run on a commit with a tag, it automatically
deploys to github releases. It always deploys docker images to eu.gcr.io if run on master. The version is generated from `git describe`, which uses the number of commits since last tag.
