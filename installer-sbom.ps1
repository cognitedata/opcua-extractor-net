Param(
	[Parameter(Mandatory=$true)]
	[Alias("v")]
	[string]$version
)

dotnet sbom-tool generate -b extractorbuild\ -bc .\ExtractorLauncher\ -pn OpcuaExtractor -pv $version -ps Cognite -nsb https://sbom.cognite.com
dotnet sbom-tool generate -b managerbuild\ -bc .\OpcUaServiceManager\ -pn OpcUaServiceManager -pv $version -ps Cognite -nsb https://sbom.cognite.com

mkdir manifests

mv extractorbuild\_manifest\spdx_2.2\manifest.spdx.json manifests\manifest.extractor.spdx.json
mv extractorbuild\_manifest\spdx_2.2\manifest.spdx.json.sha256 manifests\manifest.extractor.spdx.json.sha256
mv managerbuild\_manifest\spdx_2.2\manifest.spdx.json manifests\manifest.manager.spdx.json
mv managerbuild\_manifest\spdx_2.2\manifest.spdx.json.sha256 manifests\manifest.manager.spdx.json.sha256

rm -r extractorbuild\_manifest\
rm -r managerbuild\_manifest\

dotnet sbom-tool generate -b manifests\ -pn OpcuaExtractor -pv $version -ps Cognite -nsb https://sbom.cognite.com
mv manifests\_manifest\spdx_2.2\manifest.spdx.json manifests\manifest.spdx.json
mv manifests\_manifest\spdx_2.2\manifest.spdx.json.sha256 manifests\manifest.spdx.json.sha256

rm -r manifests\_manifest\