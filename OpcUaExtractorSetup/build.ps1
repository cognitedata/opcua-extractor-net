Param(
	[Parameter(Mandatory=$true)]
	[Alias("b")]
	[string]$msbuild,
	
	[Parameter(Mandatory=$true)]
	[Alias("v")]
	[string]$version,
	
	[Parameter(Mandatory=$true)]
	[Alias("d")]
	[string]$description,
	
	[Parameter(Mandatory=$true)]
	[Alias("c")]
	[string]$config
)

$Args = ""
$Settings = Get-Content -Path $config | ConvertFrom-Json
foreach ($prop in $Settings.PsObject.Properties) {
	if ($prop.Name -eq "setup_project") {
		continue
	}
	$Args = "$($Args) /p:$($prop.Name)=`"$($prop.Value)`""
}
$version = $version -replace '([0-9]+\.[0-9]+\.[0-9]+)-?.*','$1'
echo "Publishing $version"

$Args = "$($Args) /p:target_version=`"$($version)`" /p:target_description=`"$($description)`" /p:Configuration=Release"

$build_command = ".`"$msbuild`" $Args $($Settings.setup_project)"
echo $build_command
Invoke-Expression $build_command