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

name="opcua-extractor_${version}_${architecture}"

mkdir -p "$name"
cd "$name"
mkdir -p DEBIAN
cp -r ../installer/debian/* DEBIAN/
chmod 0755 DEBIAN/postinst
chmod 0755 DEBIAN/prerm

sed -i "s/{version}/$version/" DEBIAN/control
sed -i "s/{architecture}/$architecture/" DEBIAN/control

mkdir -p usr/bin
cp ../publish/OpcuaExtractor usr/bin/opcua-extractor
chmod 0755 usr/bin/opcua-extractor

mkdir -p var/lib/cognite/opcua
mkdir -p var/log/cognite/opcua
mkdir -p etc/cognite/opcua
cp -r ../config/* etc/cognite/opcua/
mkdir -p etc/systemd/system/

cp ../installer/opcua-extractor.service etc/systemd/system/
cp ../installer/opcua-extractor@.service etc/systemd/system/

cd ..

dpkg-deb --build --root-owner-group $name

