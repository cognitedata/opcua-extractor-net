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
mkdir -p rpmbuild/SOURCES
mkdir -p rpmbuild/SPECS
mkdir -p rpmbuild/SRPMS

cp installer/rpm/opcua-extractor.spec rpmbuild/SPECS/
cp installer/LICENSE.md rpmbuild/SOURCES/LICENSE
cp publish/OpcuaExtractor rpmbuild/SOURCES/opcua-extractor

cp -r config rpmbuild/SOURCES/config
cp installer/opcua-extractor.service rpmbuild/SOURCES/
cp installer/opcua-extractor@.service rpmbuild/SOURCES/

cd rpmbuild/

rpmbuild --define "ext-version $version" --define "ext-arch $architecture" --define "_topdir `pwd`" -bb


