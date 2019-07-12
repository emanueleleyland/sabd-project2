#!/usr/bin/env bash


docker-compose -f kafka/docker-compose-single-broker.yml down
docker-compose -f kafka/docker-compose.yml down
docker-compose -f flink/docker-compose.yml down
docker kill redis
docker rm redis

docker network rm dspnet
