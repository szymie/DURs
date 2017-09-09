#!/bin/bash

FIRST_IP=$1
shift

while (( "$#" )); do

ssh -n -f inf109733@$1 "sh -c 'pkill -f java &'"

FIRST_IP=$((FIRST_IP+1))

shift

done


