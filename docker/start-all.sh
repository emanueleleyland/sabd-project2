#!/usr/bin/env bash

#comment only one
KAFKA_MODE='SINGLE'
#KAFKA_MODE='CLUSTER'

BROKERS_NUM=2
TASK_MANAGER_NUM=2


docker network create --driver bridge --subnet 10.0.0.0/16 dspnet

docker run --network=dspnet --ip 10.0.0.11 --name=redis -p 6379:6379 -d sickp/alpine-redis

if [[ ${KAFKA_MODE} = 'SINGLE' ]]; then
    echo "starting single kafka broker"
    docker-compose -f kafka/docker-compose-single-broker.yml up -d
else
    echo "starting multi kafka broker"
    docker-compose -f ./kafka/docker-compose.yml up -d --scale kafka=${BROKERS_NUM}
fi

docker-compose -f flink/docker-compose.yml up -d --scale taskmanager=${TASK_MANAGER_NUM}
