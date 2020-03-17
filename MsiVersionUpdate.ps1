Param(
  [Parameter(Mandatory=$true)]
  [string]$file,
  [Parameter(Mandatory=$true)]
  [string]$version
)

(gc $file) -replace '/*ProductVersion=.*', ('ProductVersion="' + "$($version)" + '"?>') | Set-Content -encoding ASCII $file
