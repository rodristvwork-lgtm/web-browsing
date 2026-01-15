#!/bin/bash

read -p "Vuoi eseguire il pull da git? (y/n) " SCELTA
if [ "$SCELTA" = "y" ]; then
    read -p "Inserire username GIT: " USER
    read -p "Inserire password GIT per utente $USER: " PASSWORD
    git pull https://$USER:$PASSWORD@git.skylogic.com/mrotondo/modular_software 1>/dev/null 2>/dev/null
fi
cd ..
echo "Creo zip di modular software/loading_performance"
zip -r modular_software.zip modular_software/loading_performance 1>/dev/null
for i in {1..4}
do
    echo "Installazione numero $i"
    printf "Inserire numero utenza per scp:\n1) administrator\n2) stt\n"
    read -p "" USER_SCP
    if [ $USER_SCP = 1 ]; then
        USER_SCP="administrator"
    else
        USER_SCP="stt"
    fi
    read -p "Inserire indirizzo IP della macchina su cui portare lo zip: " ADDR
    scp modular_software.zip $USER_SCP@$ADDR:/home/$USER_SCP/Software
done
rm -rf modular_software.zip
exit 0