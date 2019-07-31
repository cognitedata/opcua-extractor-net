#!/bin/sh

dotnet test /p:Exclude="[xunit*]*" /p:CollectCoverage=true /p:CoverletOutputFormat=lcov /p:CoverletOutput='../coverage.lcov'
