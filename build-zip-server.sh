#!/bin/bash

set -x -e
configuration="$1"
linux="$2"
version="$3"

mkdir -p $configuration/
mv Server/bin/Release/net8.0/$configuration/publish/* ./$configuration/
rm -f ./$configuration/*.config ./$configuration/*.pdb ./$configuration/*.xml

dotnet sbom-tool generate -b ./$configuration/ -bc Server/ -pn Server -pv $version -ps Cognite -nsb https://sbom.cognite.com
mv ./$configuration/_manifest/spdx_2.2/* ./$configuration/
rm -r ./$configuration/_manifest/

mkdir -p ./$configuration/config

cp ./LICENSE.md ./$configuration/
cp ./config/Server.Test.Config.xml ./$configuration/config/

if [ $linux = "true" ]
then
    chmod +x ./$configuration/Server
fi

cd $configuration
zip -r ../test-server.$configuration.$version.zip *
cd ..
rm -r ./$configuration