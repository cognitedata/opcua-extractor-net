#!/bin/sh

killservers()
{
    kill $(ps aux | grep '[P]ython Testing/servers' | awk '{print $2}') > /dev/null 2>&1
}
killservers
python3 Testing/servers/server-test1.py > /dev/null 2>&1 &
sleep 3
dotnet test --filter "Category=basicserver|Category=both" /p:Exclude="[xunit*]*" /p:CollectCoverage=true /p:CoverletOutputFormat=json /p:CoverletOutput='coverage.json'
killservers
python3 Testing/servers/server-test2.py > /dev/null 2>&1 &
sleep 5
dotnet test --filter "Category=fullserver|Category=both" /p:Exclude="[xunit*]*" /p:CollectCoverage=true /p:MergeWith='coverage.json' /p:CoverletOutputFormat=lcov /p:CoverletOutput='../coverage.lcov'
killservers
rm coverage.json
