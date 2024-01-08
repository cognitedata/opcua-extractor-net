#!/bin/bash

set -x -e

configuration="$1"
linux="$2"
version="$3"

mkdir -p $configuration/
mv MQTTCDFBridge/bin/Release/net8.0/$configuration/publish/* ./$configuration/
rm -f ./$configuration/*.config ./$configuration/*.pdb ./$configuration/*.xml

dotnet sbom-tool generate -b ./$configuration/ -bc MQTTCDFBridge/ -pn MQTTCDFBridge -pv $version -ps Cognite -nsb https://sbom.cognite.com
cp ./$configuration/_manifest/spdx_2.2/* ./$configuration/
rm -r ./$configuration/_manifest/

mkdir -p ./$configuration/config
cp ./LICENSE.md ./$configuration/
cp ./config/config.bridge.example.yml ./$configuration/config/

if [ $linux = "true" ]
then
    chmod +x ./$configuration/MQTTCDFBridge
fi

cd $configuration
zip -r ../mqtt-cdf-bridge.$configuration.$version.zip *
cd ..
rm -r ./$configuration
