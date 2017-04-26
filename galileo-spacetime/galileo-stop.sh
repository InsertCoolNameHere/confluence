#!/bin/bash
while IFS='' read -r line || [[ -n "$line" ]]; do
    
    ssh $line "killall -9 -u $USER java 2>/dev/null;killall -9 -u $USER jstatd 2>/dev/null;rm -rf /s/$line/a/tmp/sapmitra/galileo-sapmitra;rm -rf /tmp/sapmitra-galileo;rm -rf /s/$line/a/tmp/galileo-sapmitra;mkdir -p /s/$line/a/tmp/galileo-sapmitra;"&
	echo "Stopping galileo on $line"
done < "$1"

