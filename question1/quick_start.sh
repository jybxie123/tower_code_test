#!/bin/bash
cd src/

echo "Starting matrix.py..."
python matrix.py &

sleep 1

echo "Starting correlation.py..."
python correlation.py &
correlation_pid=$!

wait $correlation_pid
echo "correlation.py has finished. Exiting script."
