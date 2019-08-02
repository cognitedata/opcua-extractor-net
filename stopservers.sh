#!/bin/sh

kill $(ps aux | grep '[P]ython servers' | awk '{print $2}') > /dev/null 2>&1
