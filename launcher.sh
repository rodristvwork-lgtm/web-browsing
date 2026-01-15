#!/bin/bash

pgrep python | xargs kill -9 2>/dev/null
sleep 1
pgrep python | xargs kill -9 2>/dev/null

cd /home/admin/Software/modular_software
python3 loading_performance/loading.py
