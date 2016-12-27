#!/bin/bash

if [ $# -lt 3 ]; then
  echo "usage: $0 <db> <table> <partitionValStrs> [trashRootDir]"
  exit 1
fi

type="Hive"
db=$1
table=$2
partitionValStrs=$3
trashRootDir='/user/hive/tmp/hive_clean_trash'

if [ $# -ge 4 ]; then trashRootDir=$4; fi

hadoop jar hadoop-extras-1.0-SNAPSHOT.jar \
  com.sogou.hadoop.extras.tools.clean.Clean \
  $type $db $table $partitionValStrs $trashRootDir