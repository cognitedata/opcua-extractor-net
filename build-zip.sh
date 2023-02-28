#!/bin/bash

set -x
configuration="$1"
linux="$2"
version="$3"

mkdir -p $configuration/
mv ExtractorLauncher/bin/Release/net7.0/$configuration/publish/* ./$configuration/
rm -f ./$configuration/*.config ./$configuration/*.pdb ./$configuration/*.xml
mkdir -p ./$configuration/config
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
