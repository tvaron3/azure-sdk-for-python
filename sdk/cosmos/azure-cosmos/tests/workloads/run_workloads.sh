#!/bin/bash
if [ $# -eq 0 ]; then
    echo "Usage: $0 num_runs [operations] [proxy]"
    echo "  num_runs:   number of processes per config"
    echo "  operations: comma-separated (default: read,write,query)"
    echo "  proxy:      true/false (default: false)"
    exit 1
fi

num_runs=$1
operations=${2:-read,write,query}
use_proxy=${3:-false}

echo "[Info] Installing azure-cosmos package..."
pip install ../../.
if [ $? -ne 0 ]; then
    echo "[Error] Failed to install azure-cosmos. Exiting."
    exit 2
fi
echo "[Info] azure-cosmos installed successfully."

echo "[Info] Starting $num_runs processes: operations=$operations proxy=$use_proxy"

for (( i=0; i<num_runs; i++ )); do
    WORKLOAD_OPERATIONS=$operations WORKLOAD_USE_PROXY=$use_proxy nohup python3 workload.py > /dev/null 2>&1 &
    sleep 1
done

echo "[Info] All workloads started."
