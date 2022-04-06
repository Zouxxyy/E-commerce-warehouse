#!/bin/bash

for i in dell-r720 dell-r730-4 dell-r730-5; do
  echo --------- $i ----------
  ssh $i 'jps $@ | grep -v Jps'
done
