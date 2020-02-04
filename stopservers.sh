#!/bin/sh

kill $(ps aux | grep '[P]ython servers' | awk '{print $2}') > /dev/null 2>&1
kill $(ps aux | grep '[p]ython3 servers' | awk '{print $2}') > /dev/null 2>&1
