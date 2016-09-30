#!/bin/bash

if [ $# -ne 3 ]; then
  echo "usage: $0 <srcNamenode> <srcDir> <listFile>"
  exit 1
fi

srcNamenode=$1
srcDir=$2
listFile=$3

export HADOOP_CLIENT_OPTS="-Xmx30g -Xms30g"

bin/hadoop fs -Dfs.defaultFS=$srcNamenode -ls -R $srcDir > $listFile