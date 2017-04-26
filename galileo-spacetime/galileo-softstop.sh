#!/bin/bash
while IFS='' read -r line || [[ -n "$line" ]]; do
    
    ssh $line "rm -rf /s/$line/a/tmp/sapmitra/galileo-sapmitra;rm -rf /tmp/sapmitra-galileo;rm -rf /s/$line/a/tmp/galileo-sapmitra;"&
	echo "Stopping galileo on $line"
done < "$1"

