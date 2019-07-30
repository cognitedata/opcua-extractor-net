#!/bin/sh

kill $(ps aux | grep '[P]ython servers' | awk '{print $2}') > /dev/null 2>&1
sleep 1
dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
cd $dir
python3 servers/server-test1.py > /dev/null 2>&1 &
python3 servers/server-test2.py > /dev/null 2>&1 &