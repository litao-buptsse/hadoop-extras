#!/bin/bash

if [ $# -ne 5 ]; then
  echo "usage: $0 <copyListDir> <srcNamenode> <dstNamenode> <dstDir> <resultDir>"
  exit 1
fi

copyListDir=$1
srcNamenode=$2
dstNamenode=$3
dstDir=$4
fastCopyResultDir=$5

hadoop jar \
  hadoop-extras-1.0-SNAPSHOT.jar \
  com.sogou.hadoop.extras.tools.fastcp.DistributedFastCopy \
  -Dmapreduce.task.timeout=0 \
  -Dmapreduce.map.java.opts="-Xms5g -Xmx5g" \
  -Dmapreduce.map.memory.mb=6144 \
  -Dmapreduce.reduce.java.opts="-Xms8g -Xmx8g" \
  -Dmapreduce.reduce.memory.mb=10240 \
  -Dmapreduce.job.queuename="root.leftover" \
  $copyListDir $srcNamenode $dstNamenode $dstDir $fastCopyResultDir FASTCOPY