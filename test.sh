#!/bin/sh

dotnet test /p:Exclude="[xunit*]*" /p:ExcludeByFile=\"../Extractor/Program.cs,../Extractor/Logger.cs\" /p:CollectCoverage=true /p:CoverletOutputFormat=lcov /p:CoverletOutput='../coverage.lcov'
