#!/bin/bash
while IFS='' read -r line || [[ -n "$line" ]]; do
    
    ssh $line "mkdir /s/$line/a/nobackup/galileo/sapmitra"&
	echo "MKdir galileo on $line"
done < "$1"

