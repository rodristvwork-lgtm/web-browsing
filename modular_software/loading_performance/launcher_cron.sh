#!/bin/bash
cd /home/administrator/Software
python3 test_detector.py

if [ $? -eq 1 ]  
then
    pgrep python | xargs kill -9 2>/dev/null
    sleep 1
    pgrep python | xargs kill -9 2>/dev/null

    cd /home/administrator/Software/modular_software/loading_performance/Sitespeed
    python3 browsertime.py -b 1 > "browsertime.log"
    cd /home/administrator/Software/modular_software
    python3 loading_performance/loading.py

elif [[ $? -eq 2 ]]
then
    exit 0

else
    cd /home/administrator/Software/modular_software/loading_performance/Sitespeed
    python3 browsertime.py -b 1 > "browsertime.log"
    cd /home/administrator/Software/modular_software
    python3 loading_performance/loading.py

fi
