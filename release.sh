#!/bin/sh

version=$1
echo "Release version $version"
git tag -m "Version $version" $version
git push --tags
echo "Wait for release to apper on github"
