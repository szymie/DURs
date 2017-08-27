#!/bin/bash

rm -rf jpaxosLogs

java -Dserver.port=$((8090 + $1)) -jar target/DURs-1.0-SNAPSHOT.jar -id $1 -port $2 -paxosProcesses $3 -bossThreads $4 -workerThreads $5 --spring.profiles.active=$6 > $7 2>&1