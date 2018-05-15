#!/bin/bash
top -b -n 2000 -d 0.1 | grep java > /s/$HOSTNAME/a/tmp/galileo-sapmitra/perf.log
date +"%s" >> /s/$HOSTNAME/a/tmp/galileo-sapmitra/perf.log
