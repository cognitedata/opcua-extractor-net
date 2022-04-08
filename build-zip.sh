#!/bin/bash

configuration="$1"
buildargs="$2"
linux="$3"

dotnet publish -c Release -r $configuration $buildargs ExtractorLauncher/
mkdir -p $configuration/
mv ExtractorLauncher/bin/Release/net6.0/$configuration/publish/* ./$configuration/
rm -f ./$configuration/*.config ./$configuration/*.pdb ./$configuration/*.xml
mkdir -p ./$configuration/config
cp ./LICENSE.md ./$configuration/
cp ./config/config.example.yml ./$configuration/config/
cp ./config/config.minimal.yml ./$configuration/config/
cp ./config/opc.ua.net.extractor.Config.xml ./$configuration/config/
cp ./CHANGELOG.md ./$configuration/

if ["$linux" -eq "true"]
then
    chmod +x ./$configuration/OpcuaExtractor
fi

cd $configuration
zip -r ../opcua-extractor.$configuration.$version.zip *
cd ..
rm -r ./$configuration
