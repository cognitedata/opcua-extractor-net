#!/bin/bash

configuration="$1"
linux="$2"
version="$3"

mkdir -p $configuration/
mv MQTTCDFBridge/bin/Release/net7.0/$configuration/publish/* ./$configuration/
rm -f ./$configuration/*.config ./$configuration/*.pdb ./$configuration/*.xml
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
