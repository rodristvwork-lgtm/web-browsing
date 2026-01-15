echo "Copia modular_software.zip tramite TeamViewer"
pause
cd .\modular_software\loading_performance
$mydate=Get-Date -UFormat "%Y-%m-%d"
tar -caf results-$mydate.zip results
mv results-$mydate.zip ..\..
cd ..\..
Remove-Item '.\modular_software' -Recurse
tar -xf modular_software.zip
del modular_software.zip
cd .\modular_software\loading_performance
(Get-Content config.py) -replace 'os.uname\(\).nodename', '"Huges"' | Out-File -encoding ASCII config.py
Get-Content settings.json | Where-Object {$_ -notmatch 'token_dropbox'} | Set-Content sett.json
del settings.json
mv sett.json settings.json
(Get-Content loading.py) -replace 'dropbox_export.export_data\(\)', '' | Out-File -encoding ASCII loading.py
(Get-Content loading.py) -replace 'to_ret = _get_modem_info\(to_ret\)', '' | Out-File -encoding ASCII loading.py
(Get-Content loading.py) -replace 'import modem', '' | Out-File -encoding ASCII loading.py
(Get-Content loading.py) -replace 'import dropbox_export', '' | Out-File -encoding ASCII loading.py
exit