#!/bin/bash

PROFILE=$1
shift
PAXOS_PROCESSES=$1
shift
FIRST_IP=$1
shift

ID_COUNTER=0

while (( "$#" )); do

ssh -n -f inf109733@$1 "sh -c 'cd /home/inf109733/mgr/DURs; nohup java -Dserver.port=8090 -jar target/DURs-1.0-SNAPSHOT.jar -id $ID_COUNTER -port 8080 -paxosProcesses $PAXOS_PROCESSES -bossThreads 0 -workerThreads 0 --spring.profiles.active=$PROFILE > /dev/null 2>&1 &'"
ssh -n -f inf109733@$1 "sh -c 'cd /home/inf109733/mgr/DURs; nohup ../apache-jmeter-3.2/bin/jmeter-server -Djava.rmi.server.hostname=10.10.0.$FIRST_IP > /dev/null 2>&1 &'"	

FIRST_IP=$((FIRST_IP+1))
ID_COUNTER=$((ID_COUNTER+1))

shift

done


