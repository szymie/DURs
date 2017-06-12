#!/bin/bash

./cleaner.sh
java -jar target/DURs-1.0-SNAPSHOT.jar -id $1 -port $2 -address $3
