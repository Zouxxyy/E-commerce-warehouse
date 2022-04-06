#!/bin/bash

if [ $# -lt 1 ]; then
  echo "No Args Input..."
  exit
fi
case $1 in
"start")
  echo " =================== 启动 hadoop集群 ==================="

  echo " --------------- 启动 hdfs ---------------"
  ssh dell-r720 "/home/zxy/software/hadoop-3.3.2/sbin/start-dfs.sh"
  echo " --------------- 启动 yarn ---------------"
  ssh dell-r730-4 "/home/zxy/software/hadoop-3.3.2/sbin/start-yarn.sh"
  echo " --------------- 启动 historyserver ---------------"
  ssh dell-r720 "/home/zxy/software/hadoop-3.3.2/bin/mapred --daemon start historyserver"
  ;;
"stop")
  echo " =================== 关闭 hadoop集群 ==================="

  echo " --------------- 关闭 historyserver ---------------"
  ssh dell-r720 "/home/zxy/software/hadoop-3.3.2/bin/mapred --daemon stop historyserver"
  echo " --------------- 关闭 yarn ---------------"
  ssh dell-r730-4 "/home/zxy/software/hadoop-3.3.2/sbin/stop-yarn.sh"
  echo " --------------- 关闭 hdfs ---------------"
  ssh dell-r720 "/home/zxy/software/hadoop-3.3.2/sbin/stop-dfs.sh"
  ;;
*)
  echo "Input Args Error..."
  ;;
esac
