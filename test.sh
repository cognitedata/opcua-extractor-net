#!/bin/sh

dotnet test /p:Exclude="[xunit*]*%2c[Server]*%2c[System*]*" /p:CollectCoverage=true /p:CoverletOutputFormat=lcov /p:CoverletOutput='../coverage.lcov'
