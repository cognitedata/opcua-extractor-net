$cert = (Get-ChildItem -Path "Cert:\LocalMachine\My" | Where-Object {$_.Subject -Match "Cognite AS"})[0]
write-host $cert
Set-AuthenticodeSignature -FilePath "$args[0]" -Certificate $cert