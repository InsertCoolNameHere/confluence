#!/bin/bash
date
while IFS='' read -r line || [[ -n "$line" ]]; do
    
    ssh $line "sh /s/chopin/b/grad/sapmitra/git/lassofinal/galileo-spacetime/galileo-grep.sh;"&
	echo "Running grep on $line"
done < "$1"

