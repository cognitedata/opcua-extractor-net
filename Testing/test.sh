#!/bin/sh
screen -dmS uaserver python3 servers/server-test1.py
sleep 3
dotnet test --filter "Category=basicserver|Category=both" /p:Exclude="[xunit*]*" /p:CollectCoverage=true /p:CoverletOutputFormat=json /p:CoverletOutput='coverage.json'
while screen -list | grep -q "uaserver"
do
    screen -X -S uaserver stuff $'\003'
    sleep 1
done
screen -dmS uaserver python3 servers/server-test2.py
sleep 5
dotnet test --filter "Category=fullserver|Category=both" /p:Exclude="[xunit*]*" /p:CollectCoverage=true /p:MergeWith='coverage.json' /p:CoverletOutputFormat=lcov /p:CoverletOutput='../coverage.lcov'
screen -X -S uaserver stuff $'\003'
rm coverage.json
