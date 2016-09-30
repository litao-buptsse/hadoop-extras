#!/bin/bash

if [ $# -ne 3 ]; then
  echo "usage: $0 <copylist> <copylist_new> <copylist_diff>"
  exit 1
fi

copylist=$1
copylist_new=$2
copylist_diff=$3

export HADOOP_CLIENT_OPTS="-Xmx40g -Xms40g"

bin/hadoop jar hadoop-extras-1.0-SNAPSHOT.jar \
  com.sogou.hadoop.extras.tools.hdfs.fastcp.DiffFileList \
  $copylist $copylist_new > $copylist_diff