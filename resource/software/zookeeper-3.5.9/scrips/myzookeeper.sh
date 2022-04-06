#!/bin/bash

case $1 in
"start") {
  for i in dell-r720 dell-r730-4 dell-r730-5; do
    echo ---------- zookeeper $i 启动 ------------
    ssh $i "/home/zxy/software/apache-zookeeper-3.5.9/bin/zkServer.sh start"
  done
} ;;
"stop") {
  for i in dell-r720 dell-r730-4 dell-r730-5; do
    echo ---------- zookeeper $i 停止 ------------
    ssh $i "/home/zxy/software/apache-zookeeper-3.5.9/bin/zkServer.sh stop"
  done
} ;;
"status") {
  for i in dell-r720 dell-r730-4 dell-r730-5; do
    echo ---------- zookeeper $i 状态 ------------
    ssh $i "/home/zxy/software/apache-zookeeper-3.5.9/bin/zkServer.sh status"
  done
} ;;
esac
