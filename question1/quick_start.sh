#!/bin/bash
conda create -n tower_code_test python=3.11 -y
conda activate tower_code_test
pip install -r requirements.txt

cd src/

echo "Starting matrix.py..."
python matrix.py &

sleep 1

echo "Starting correlation.py..."
python correlation.py &
correlation_pid=$!

wait $correlation_pid
echo "correlation.py has finished. Exiting script."
