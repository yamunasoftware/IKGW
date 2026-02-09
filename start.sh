#!bin/bash

apt-get update && apt-get install -y openjdk-17-jre-headless
mkdir -p /ikgw/logs
mvn clean package
java -jar target/IKGW-0.0.1-SNAPSHOT.jar