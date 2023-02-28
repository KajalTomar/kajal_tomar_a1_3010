#!/bin/bash

if [ -z "$1" ]
  then
    echo "Requires an argument that is the port number of your Message queue server"
fi

# load up the queue!

echo sending data to port $1 on localhost

# send the file!
# The & makes the process run "in the background"
# so these will all be running at the same time

(awk '{print "JOB " $0}' < gadsby.txt | nc localhost $1) &
(awk '{print "JOB " $0}' < pride.txt  | nc localhost $1) &
(awk '{print "JOB " $0}' < wap.txt  | nc localhost $1) &

wait