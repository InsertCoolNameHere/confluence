#!/bin/bash
date
while IFS='' read -r line || [[ -n "$line" ]]; do
    
    ssh $line "sh /s/chopin/b/grad/sapmitra/git/lassofinal/galileo-spacetime/galileo-readPerf.sh;"&
	echo "Running perf on $line"
done < "$1"

