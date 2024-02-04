#!/bin/bash

if [ $# -lt 5 ];
then
        echo "insert-bulk.sh [insert-bulk.jar path] [jdbc-url] [username] [password] [parameters-file.csv]"
        exit -1
fi

export JAR=$1
export URL=$2
export USERNAME=$3
export PASSWORD=$4
export PARAMETERS=$5
export ROWS=50000

echo "Starting bulk insert benchmark"

while IFS="," read -r threads commit batch column temp bulk
do
	echo -n "Run with parameters: threads=$threads commit=$commit batch=$batch columnStore=$column tempTable=$temp bulkMode=$bulk"
	time java -jar $JAR --url=$URL --username=$USERNAME --password=$PASSWORD --rows=$ROWS 	--threads=$threads 	--commit-size=$commit 	--batch-size=$batch 	--column-store=$column 	--temp-table=$temp 	--bulk-mode=$bulk > run_$threads_$commit_$batch_$column_$temp_$bulk

done < <(tail -n +2 $PARAMETERS)   

