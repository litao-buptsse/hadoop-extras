#!/bin/bash

if [ $# -lt 2 ]; then
  echo "usage: $0 <dirPattern> <filePattern>"
  exit 1
fi

type="HDFS"
dirPattern=$1
filePattern=$2

hadoop jar hadoop-extras-1.0-SNAPSHOT.jar \
  com.sogou.hadoop.extras.tools.statistics.Statistics \
  $type $dirPattern $filePattern
