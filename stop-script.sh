#!/bin/bash

while (( "$#" )); do

ssh -n -f inf109733@$1 "sh -c 'pkill -f java &'"

shift

done
