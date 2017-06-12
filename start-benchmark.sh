#!/bin/bash
java -jar target/DURs-1.0-SNAPSHOT.jar -port $1 -address $2 -B -keys 300 -threads 4 -readsInQuery 100 -readsInUpdate 8 -writesInUpdate 2 -delay 0 -saturation 1
