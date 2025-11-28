#!/bin/bash
# Startup script for Flask Server on port 30214
# This script will start the server and keep it running

cd /home/superadmin/hr/mcp_server_code/gaurdiaandemoapp

# Activate the correct Python environment
export PYENV_VERSION=venv_gar

# Start the server using nohup to keep it running in background
nohup python app.py > server_30214.log 2>&1 &

# Save the process ID
echo $! > server_30214.pid

echo "Server started on port 30214"
echo "PID: $(cat server_30214.pid)"
echo "Log file: server_30214.log"
echo "To stop: kill \$(cat server_30214.pid)"
