#!/bin/sh

dotnet test -v n --filter UAClientTest. /p:Exclude="[xunit*]*" /p:ExcludeByFile=\"**/Program.cs,**/Logger.cs\" /p:CollectCoverage=true /p:CoverletOutputFormat=lcov /p:CoverletOutput='../coverage.lcov'
