#!/bin/bash

if [ $# -lt 2 ]; then
  echo "usage: $0 <dirPattern> <filePattern> [trashDir]"
  exit 1
fi

type="HDFS"
dirPattern=$1
filePattern=$2
trashDir='/logdata/tmp/hdfs_clean_trash'

if [ $# -ge 3 ]; then trashDir=$3; fi


hadoop jar hadoop-extras-1.0-SNAPSHOT.jar \
  com.sogou.hadoop.extras.tools.clean.Clean \
  $type $dirPattern $filePattern $trashDir