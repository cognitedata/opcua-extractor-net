#!/bin/sh

sh Test/startservers.sh
sleep 4
dotnet test /p:Exclude="[xunit*]*" /p:CollectCoverage=true /p:CoverletOutputFormat=json /p:CoverletOutput='../coverage.lcov'
