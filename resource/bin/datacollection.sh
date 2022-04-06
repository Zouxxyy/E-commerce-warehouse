#!/bin/bash

case $1 in
"start") {
  echo "================== 启动 集群 =================="

  #启动 Zookeeper集群
  myzookeeper.sh start
  sleep 10
  #启动 Hadoop集群
  myhadoop.sh start
  sleep 10
  #启动 Kafka采集集群
  mykafka.sh start
  sleep 10
  #启动 Flume1
  flume1.sh start
  sleep 10
  #启动 Flume2
  flume2.sh start
  sleep 10
  #启动 Flume3
  flume3.sh start
  sleep 10
  #启动 Maxwell
  mymaxwell.sh start

} ;;
"stop") {
  echo "================== 停止 集群 =================="

  #停止 Maxwell
  mymaxwell.sh stop
  sleep 10
  #停止 Flume3
  flume3.sh stop
  sleep 10
  #停止 Flume2
  flume2.sh stop
  sleep 10
  #停止 Flume1
  flume1.sh stop
  sleep 10
  #停止 Kafka采集集群
  mykafka.sh stop
  sleep 10
  #停止 Hadoop集群
  myhadoop.sh stop
  sleep 10
  #停止 Zookeeper集群
  myzookeeper.sh stop

} ;;
esac
