#!/bin/bash
while IFS='' read -r line || [[ -n "$line" ]]; do
    
    ssh $line "cp /s/$line/a/tmp/galileo-sapmitra/storage-node.log /s/chopin/b/grad/sapmitra/Documents/Conflux/logs/$line.log;"&
	echo "Logging into galileo on $line"
done < "$1"

