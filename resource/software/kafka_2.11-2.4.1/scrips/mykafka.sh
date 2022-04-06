#!/bin/bash

case $1 in
"start") {
  for i in dell-r720 dell-r730-4 dell-r730-5; do
    echo " --------启动 $i Kafka-------"
    ssh $i "/home/zxy/software/kafka_2.11-2.4.1/bin/kafka-server-start.sh -daemon /home/zxy/software/kafka_2.11-2.4.1/config/server.properties"
  done
} ;;
"stop") {
  for i in dell-r720 dell-r730-4 dell-r730-5; do
    echo " --------停止 $i Kafka-------"
    ssh $i "/home/zxy/software/kafka_2.11-2.4.1/bin/kafka-server-stop.sh stop"
  done
} ;;
esac
