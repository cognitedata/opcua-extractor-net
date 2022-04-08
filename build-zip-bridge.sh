#!/bin/bash

configuration="$1"
buildargs="$2"
linux="$3"

dotnet publish -c Release -r $configuration $buildargs MQTTCDFBridge/
mkdir -p $configuration/
mv MQTTCDFBridge/bin/Release/net6.0/$configuration/publish/* ./$configuration/
rm -f ./$configuration/*.config ./$configuration/*.pdb ./$configuration/*.xml
mkdir -p ./$configuration/config
cp ./LICENSE.md ./$configuration/
cp ./config/config.bridge.example.yml ./$configuration/config/

if ["$linux" -eq "true"]
then
    chmod +x ./$configuration/MQTTCDFBridge
fi

cd $configuration
zip -r ../mqtt-cdf-bridge.$configuration.$version.zip *
cd ..
rm -r ./$configuration
