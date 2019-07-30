#!/bin/sh

kill $(ps aux | grep '[P]ython servers' | awk '{print $2}')
sleep 1
dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
cd $dir
python3 servers/server-test1.py  &
python3 servers/server-test2.py  &
