#!/bin/sh

version=$1
echo "Release version $version"
git tag -m "Version $version" $version
git push origin $version
echo "Wait for release to appear on github"
