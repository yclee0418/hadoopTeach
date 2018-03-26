#!/bin/bash
NCSRV=$1
PORT=$2
if [ -z "$1" ]; then
        NCSRV="localhost"
        PORT="9999"
fi
#send content to nc -lk 9999, line by line
#received by nc localhost 9999
cat data/apache.access.log | while read line; do
        echo "$line"
        sleep 0.5
done | nc -l $PORT


