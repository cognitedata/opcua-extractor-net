#!/bin/sh

version="$1"
desc="$(git describe --tags --dirty) $(git log -1  --format=%ai)"
echo "/p:InformationalVersion=\"$version\" /p:Description=\"$desc\" --self-contained true /p:PublishSingleFile=\"true\""