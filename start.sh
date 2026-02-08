#!bin/bash

mkdir -p /ikgw/logs
mvn clean install
java -jar target/0.0.1-SNAPSHOT.jar