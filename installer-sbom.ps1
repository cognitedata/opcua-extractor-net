Param(
	[Parameter(Mandatory=$true)]
	[Alias("v")]
	[string]$version
)

dotnet sbom-tool generate -b installerbuild\extractorbuild\ -bc .\ExtractorLauncher\ -pn OpcuaExtractor -pv $version -ps Cognite -nsb https://sbom.cognite.com
dotnet sbom-tool generate -b installerbuild\managerbuild\ -bc .\OpcUaServiceManager\ -pn OpcUaServiceManager -pv $version -ps Cognite -nsb https://sbom.cognite.com

mkdir installerbuild\manifests

mv installerbuild\extractorbuild\_manifest\spdx_2.2\manifest.spdx.json installerbuild\manifests\manifest.extractor.spdx.json
mv installerbuild\extractorbuild\_manifest\spdx_2.2\manifest.spdx.json.sha256 installerbuild\manifests\manifest.extractor.spdx.json.sha256
mv installerbuild\managerbuild\_manifest\spdx_2.2\manifest.spdx.json installerbuild\manifests\manifest.manager.spdx.json
mv installerbuild\managerbuild\_manifest\spdx_2.2\manifest.spdx.json.sha256 installerbuild\manifests\manifest.manager.spdx.json.sha256

rm -r installerbuild\extractorbuild\_manifest\
rm -r installerbuild\managerbuild\_manifest\

dotnet sbom-tool generate -b installerbuild\manifests\ -pn OpcuaExtractor -pv $version -ps Cognite -nsb https://sbom.cognite.com
mv installerbuild\manifests\_manifest\spdx_2.2\manifest.spdx.json installerbuild\manifests\manifest.spdx.json
mv installerbuild\manifests\_manifest\spdx_2.2\manifest.spdx.json.sha256 installerbuild\manifests\manifest.spdx.json.sha256

rm -r installerbuild\manifests\_manifest\