#!/bin/bash

# il nome va cambiato in base alla macchina
NOME='"GW6_A12"'
PASSWORD='Admin4Jup!'
echo "Su nocscript.lan eseguire deploy_noc.sh"
read -p "Premere invio per proseguire... " AVANTI
echo "Stoppo processo python"
pgrep python | xargs kill -9 2>/dev/null
echo "Prendo nuovo IP su interfaccia ens192"
echo $PASSWORD | sudo -S dhclient -r ens192 1>/dev/null
echo $PASSWORD | sudo -S dhclient -v ens192 1>/dev/null
echo "Creo zip della cartella results e lo sposto in Software"
cd modular_software/loading_performance
zip -9pr results-"`date +"%F"`".zip results 1>/dev/null
mv results-"`date +"%F"`".zip ../..
cd ../..
echo "Rimuovo cartella modular_software"
rm -rf modular_software 1>/dev/null
ADDR=`hostname -I | awk {'print $1'}`
echo "Copia modular_software.zip da nocscript.lan a $ADDR"
read -p "Premere invio per proseguire... " AVANTI
echo "Estraggo ed elimino lo zip"
unzip modular_software.zip 1>/dev/null
rm -rf modular_software.zip 1>/dev/null
echo "Entro in loading performance"
cd modular_software/loading_performance
echo "Assegno a tutti i file i permessi di esecuzione"
chmod 777 * 
echo "Imposto il nome nel file config.py"
sed -i "s/os.uname().nodename/$NOME/g" config.py
echo "Rimuovo .exe da path geckodriver in settings.json"
sed -i 's/geckodriver.exe/geckodriver/g' settings.json
echo "Prendo nuovo IP su interfaccia ens224"
echo $PASSWORD | sudo -S dhclient -r ens224 1>/dev/null
echo $PASSWORD | sudo -S dhclient -v ens224 1>/dev/null
curl -I https://www.google.com | head -n 1
# cd ../..
# sudo chown administrator -R modular_software/
exit 0