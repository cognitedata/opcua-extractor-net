#!/bin/sh
screen -dmS uaserver python3 servers/server-test1.py
sleep 2
dotnet test --filter "Category=basicserver|Category=both" /p:Exclude="[xunit*]*" /p:CollectCoverage=true /p:CoverletOutputFormat=json /p:CoverletOutput='coverage.json'
screen -X -S uaserver stuff $'\003'
sleep 1
screen -dmS uaserver python3 servers/server-test2.py
sleep 2
dotnet test --filter "Category=fullserver|Category=both" /p:Exclude="[xunit*]*" /p:CollectCoverage=true /p:MergeWith='coverage.json' /p:CoverletOutputFormat=lcov /p:CoverletOutput='../coverage.lcov'
screen -X -S uaserver stuff $'\003'
rm coverage.json
