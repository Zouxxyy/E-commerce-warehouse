#!/bin/bash

for i in dell-r720 dell-r730-4; do
  echo "========== $i =========="
  ssh $i "cd /home/zxy/software/applog; java -jar gmall2020-mock-log-2021-10-10.jar >/dev/null 2>&1 &"
done
