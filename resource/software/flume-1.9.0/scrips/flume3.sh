#!/bin/bash

case $1 in
"start")
  echo " --------启动 dell-r730-5 消费日志数据flume-------"
  ssh dell-r730-5 "nohup /home/zxy/software/apache-flume-1.9.0/bin/flume-ng agent -n a1 -c /home/zxy/software/apache-flume-1.9.0/conf -f /home/zxy/software/apache-flume-1.9.0/job/kafka_to_hdfs_db.conf >/dev/null 2>&1 &"
  ;;
"stop")
  echo " --------停止 dell-r730-5 消费日志数据flume-------"
  ssh dell-r730-5 "ps -ef | grep kafka_to_hdfs_db.conf | grep -v grep |awk '{print \$2}' | xargs -n1 kill"
  ;;
esac
