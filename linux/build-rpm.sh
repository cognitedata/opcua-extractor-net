#!/bin/bash
version=$1
architecture=$2

if [ -z "$1" ]
then
	echo "Version required"
	exit 1
fi

if [ -z "$2" ]
then
	echo "Architecture required"
	exit 1
fi

mkdir -p rpmbuild/BUILD
mkdir -p rpmbuild/RPMS
mkdir -p rpmbuild/SPECS
mkdir -p rpmbuild/SRPMS

cp installer/rpm/opcua-extractor.spec rpmbuild/SPECS/
cp LICENSE.md rpmbuild/BUILD/LICENSE
cp publish/OpcuaExtractor rpmbuild/BUILD/opcua-extractor

cp -r config rpmbuild/BUILD/config
cp installer/opcua-extractor.service rpmbuild/BUILD/
cp installer/opcua-extractor@.service rpmbuild/BUILD/

cd rpmbuild/

rpmbuild -bb --define "extversion $version" --define "extarch $architecture" --define "_topdir `pwd`" SPECS/opcua-extractor.spec


