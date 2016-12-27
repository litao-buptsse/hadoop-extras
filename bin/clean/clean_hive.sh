#!/bin/bash

if [ $# -lt 4 ]; then
  echo "usage: $0 <db> <table> <partitionValStrs> <trashDir>"
  exit 1
fi

type="Hive"
db=$1
table=$2
partitionValStrs=$3
trashDir=$4

hadoop jar hadoop-extras-1.0-SNAPSHOT.jar \
  com.sogou.hadoop.extras.tools.clean.Clean \
  $type $db $table $partitionValStrs $trashDir