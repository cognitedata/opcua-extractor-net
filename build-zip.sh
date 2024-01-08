#!/bin/bash

set -x -e
configuration="$1"
linux="$2"
version="$3"

mkdir -p $configuration/
mv ExtractorLauncher/bin/Release/net8.0/$configuration/publish/* ./$configuration/
rm -f ./$configuration/*.config ./$configuration/*.pdb ./$configuration/*.xml
mkdir -p ./$configuration/config

dotnet sbom-tool generate -b ./$configuration/ -bc ExtractorLauncher/ -pn OpcuaExtractor -pv $version -ps Cognite -nsb https://sbom.cognite.com
cp ./$configuration/_manifest/spdx_2.2/* ./$configuration/
rm -r ./$configuration/_manifest/

cp ./LICENSE.md ./$configuration/
cp ./config/config.example.yml ./$configuration/config/
cp ./config/config.minimal.yml ./$configuration/config/
cp ./config/opc.ua.net.extractor.Config.xml ./$configuration/config/
cp ./CHANGELOG.md ./$configuration/

if [ $linux = "true" ]
then
    chmod +x ./$configuration/OpcuaExtractor
fi

cd $configuration
zip -r ../opcua-extractor.$configuration.$version.zip *
cd ..
rm -r ./$configuration
