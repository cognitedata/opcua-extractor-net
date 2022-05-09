#!/bin/bash

set -x
configuration="$1"
linux="$2"
version="$3"

mkdir -p $configuration/
mv Server/bin/Release/net6.0/$configuration/publish/* ./$configuration/
rm -f ./$configuration/*.config ./$configuration/*.pdb ./$configuration/*.xml
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