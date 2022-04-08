#!/bin/bash

configuration="$1"
buildargs="$2"
linux="$3"

dotnet publish -c Release -r $configuration $buildargs Server/
mkdir -p $configuration/
mv Server/bin/Release/net6.0/$configuration/publish/* ./$configuration/
rm -f ./$configuration/*.config ./$configuration/*.pdb ./$configuration/*.xml
mkdir -p ./$configuration/config
cp ./LICENSE.md ./$configuration/
cp ./config/Server.Test.Config.xml ./$configuration/config/

if ["$linux" -eq "true"]
then
    chmod +x ./$configuration/Server
fi

cd $configuration
zip -r ../test-server.$configuration.$version.zip *
cd ..
rm -r ./$configuration