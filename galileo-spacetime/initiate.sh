#!/bin/bash
while IFS='' read -r line || [[ -n "$line" ]]; do
    
    ssh $line "mkdir -p /s/$line/a/tmp/galileo-sapmitra"&
	echo "Initializing galileo on $line"
done < "$1"

