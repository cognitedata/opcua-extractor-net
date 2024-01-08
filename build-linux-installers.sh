#!/bin/bash

set -x -e
version="$1"
rpmver="$(echo "$version" | tr - _)"

cp LICENSE.md linux/
cp -r config/ linux/

dotnet sbom-tool generate -b linux/publish -bc ExtractorLauncher/ -pn OpcuaExtractor -pv $version -ps Cognite -nsb https://sbom.cognite.com
mv linux/publish/_manifest/spdx_2.2/* linux/
rm -r linux/publish/_manifest/

cd linux/
./build-deb.sh $version amd64
./build-rpm.sh $rpmver x86_64
cd ..

mv linux/*.deb .
mv linux/rpmbuild/RPMS/x86_64/*.rpm .