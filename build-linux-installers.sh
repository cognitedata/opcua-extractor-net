#!/bin/bash

set -x
version="$1"
rpmver="$(echo "$version" | tr - _)"

cp LICENSE.md linux/
cp -r config/ linux/

cd linux/
./build-deb.sh $version amd64
./build-rpm.sh $rpmver x86_64
cd ..

mv linux/*.deb .
mv linux/rpmbuild/RPMS/x86_64/*.rpm .