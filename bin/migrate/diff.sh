#!/bin/bash

if [ $# -le 3 ]; then
  echo "usage: $0 <copylist> <copylist_new> <copylist_diff> [ignoreTime]"
  exit 1
fi

copylist=$1
copylist_new=$2
copylist_diff=$3
ignore_time=false

if [ $# -eq 4 ]; then
  ignore_time=$4
fi

export HADOOP_CLIENT_OPTS="-Xmx50g -Xms50g"

hadoop jar hadoop-extras-1.0-SNAPSHOT.jar \
  com.sogou.hadoop.extras.tools.hdfs.migrate.DiffFileList \
  $copylist $copylist_new $ignore_time > $copylist_diff