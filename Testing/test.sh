#!/bin/sh

killservers()
{
    screen -X -S uaserver stuff $'\003'
    sleep 0.5
    if screen -list | grep -q "uaserver"
    then
        kill $(ps aux | grep '[P]ython servers' | awk '{print $2}')
    fi
}
killservers
screen -dmS uaserver python3 servers/server-test1.py
sleep 3
dotnet test --filter "Category=basicserver|Category=both" /p:Exclude="[xunit*]*" /p:CollectCoverage=true /p:CoverletOutputFormat=json /p:CoverletOutput='coverage.json'
killservers
screen -dmS uaserver python3 servers/server-test2.py
sleep 5
dotnet test --filter "Category=fullserver|Category=both" /p:Exclude="[xunit*]*" /p:CollectCoverage=true /p:MergeWith='coverage.json' /p:CoverletOutputFormat=lcov /p:CoverletOutput='../coverage.lcov'
killservers
rm coverage.json
