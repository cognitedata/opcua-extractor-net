#!/bin/sh

dotnet test -v n /p:Exclude="[xunit*]*%2c[Server]*" /p:ExcludeByFile=\"**/Program.cs,**/Logger.cs\" /p:CollectCoverage=true /p:CoverletOutputFormat=lcov /p:CoverletOutput='../coverage.lcov'
