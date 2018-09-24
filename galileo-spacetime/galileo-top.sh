#!/bin/bash
date +"%s" > /s/chopin/b/grad/sapmitra/Documents/Conflux/systemPerf/$HOSTNAME.log
date >> /s/chopin/b/grad/sapmitra/Documents/Conflux/systemPerf/$HOSTNAME.log
top -b -n 2000 -d 0.01 | grep java >> /s/chopin/b/grad/sapmitra/Documents/Conflux/systemPerf/$HOSTNAME.log
date +"%s" >> /s/chopin/b/grad/sapmitra/Documents/Conflux/systemPerf/$HOSTNAME.log
