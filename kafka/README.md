# Commands and Code for Hours With Experts - Kafka Class.

## Running Zookeeper and Kafka

### Starting Zookeeper
```
zookeeper-server-start config/zookeeper.properties
```

### Starting a Kafka Broker
Following three commands will start 3 kafka brokers running off 3 different config files.
```
kafka-server-start config/server.properties
kafka-server-start config/server1.properties
kafka-server-start config/server2.properties
```
Source: https://www.michael-noll.com/blog/2013/03/13/running-a-multi-broker-apache-kafka-cluster-on-a-single-node/

## CLI
Found in CLI/COMMANDS.md

## Python Consumer and Producer
Found in python directory

## Java Consumer and Producer
Found in src