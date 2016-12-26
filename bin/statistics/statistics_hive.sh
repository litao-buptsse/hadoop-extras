#!/bin/bash

if [ $# -lt 3 ]; then
  echo "usage: $0 <db> <table> <partitionValStrs>"
  exit 1
fi

type="Hive"
db=$1
table=$2
partitionValStrs=$3

hadoop jar hadoop-extras-1.0-SNAPSHOT.jar \
  com.sogou.hadoop.extras.tools.statistics.Statistics \
  $type $db $table $partitionValStrs
