#!/bin/sh

dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
cd $dir/Test
python3 servers/server-test1.py  &
python3 servers/server-test2.py  &
