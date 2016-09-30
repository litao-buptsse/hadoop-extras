#!/bin/bash

if [ $# -ne 6 ]; then
  echo "usage: $0 <copyListDir> <srcNamenode> <dstNamenode> <dstDir> <resultDir> <jobType>"
  exit 1
fi

copyListDir=$1
srcNamenode=$2
dstNamenode=$3
dstDir=$4
resultDir=$5
jobType=$6

hadoop jar \
  hadoop-extras-1.0-SNAPSHOT.jar \
  com.sogou.hadoop.extras.tools.hdfs.fastcp.DistributedFastCopy \
  -Dmapreduce.task.timeout=0 \
  -Dmapreduce.map.java.opts="-Xms5g -Xmx5g" \
  -Dmapreduce.map.memory.mb=6144 \
  -Dmapreduce.reduce.java.opts="-Xms5g -Xmx5g" \
  -Dmapreduce.reduce.memory.mb=6144 \
  -Dmapreduce.job.reduces=10 \
  -Dmapreduce.job.queuename="root.leftover" \
  $copyListDir $srcNamenode $dstNamenode $dstDir $resultDir $jobType