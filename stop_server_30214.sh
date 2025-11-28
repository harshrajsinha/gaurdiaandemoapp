#!/bin/bash
# Stop script for Flask Server on port 30214

cd /home/superadmin/hr/mcp_server_code/gaurdiaandemoapp

if [ -f server_30214.pid ]; then
    PID=$(cat server_30214.pid)
    echo "Stopping server with PID: $PID"
    kill $PID 2>/dev/null
    rm server_30214.pid
    echo "Server stopped"
else
    echo "No PID file found. Checking for running processes on port 30214..."
    PID=$(lsof -ti:30214)
    if [ -n "$PID" ]; then
        echo "Killing process $PID on port 30214"
        kill $PID
    else
        echo "No process found on port 30214"
    fi
fi
