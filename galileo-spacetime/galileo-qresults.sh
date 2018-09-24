#!/bin/bash
while IFS='' read -r line || [[ -n "$line" ]]; do
    ssh $line "rm -r /s/$line/a/nobackup/galileo/sapmitra/galileo-sapmitra/.qresults/*.blk;"&
	echo "Removing results from galileo on $line"
done < "$1"
