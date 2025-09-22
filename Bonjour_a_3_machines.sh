#!/bin/bash

python3 client.py tp-1a207-34 &
python3 client.py tp-1a207-35 &
python3 client.py tp-1a207-36 &
wait

echo "Tous les clients ont terminé."