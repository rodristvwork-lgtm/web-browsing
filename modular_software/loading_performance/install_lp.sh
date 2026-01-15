#!/bin/bash 
sudo yum install python3 -y
sudo yum update firefox -y
python3 -m pip install selenium --user
python3 -m pip install requests --user

mkdir -p /home/administrator/Software/modular_software/
mv /home/administrator/loading_performance.zip /home/administrator/Software/modular_software/
cd /home/administrator/Software/modular_software
unzip loading_performance.zip
sleep 5
cd /home/administrator/Software/modular_software/loading_performance
chmod 777 *
printf "Inserire numero che si vuole dare al file csv"
read -p "" NOME
sed -i "s/os.uname().nodename/$NOME/g" config.py
cp launcher.sh /home/administrator/Software
