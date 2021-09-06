#!/bin/sh

dotnet test -v n /p:Exclude="[xunit*]*%2c[Server]*%2c[System*]*" /p:ExcludeByFile=\"**/Program.cs,**/Logger.cs\" /p:CollectCoverage=true /p:CoverletOutputFormat=lcov /p:CoverletOutput='../coverage.lcov'
