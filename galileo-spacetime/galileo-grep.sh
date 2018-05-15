#!/bin/bash
top -b -n 10 -d 1 | grep java > /s/$HOSTNAME/a/tmp/galileo-sapmitra/perf.log
date +"%s" >> /s/$HOSTNAME/a/tmp/galileo-sapmitra/perf.log

cat /s/$HOSTNAME/a/tmp/galileo-sapmitra/storage-node.log | grep "ENTIRE THING FINISHED IN" > /s/chopin/b/grad/sapmitra/Documents/Conflux/logs/$HOSTNAME
