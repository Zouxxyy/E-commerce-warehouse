#!/bin/bash

MAXWELL_HOME=/home/zxy/software/maxwell-1.29.2

start_maxwell() {
  if [[ $(ps -ef | grep maxwell-1.29.2 | grep -v grep -c) -lt 1 ]]; then
    echo "启动Maxwell"
    $MAXWELL_HOME/bin/maxwell --config $MAXWELL_HOME/config.properties --daemon
  else
    echo "Maxwell正在运行"
  fi
}

stop_maxwell() {
  if [[ $(ps -ef | grep maxwell-1.29.2 | grep -v grep -c) -gt 0 ]]; then
    echo "停止Maxwell"
    ps -ef | grep maxwell | grep -v grep | awk '{print $2}' | xargs kill -9
  else
    echo "Maxwell未在运行"
  fi
}

case $1 in
start)
  start_maxwell
  ;;
stop)
  stop_maxwell
  ;;
restart)
  stop_maxwell
  start_maxwell
  ;;
esac
