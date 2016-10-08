#!/bin/bash

if [ $# -ne 4 ]; then
  echo "usage: $0 <srcNamenode> <dstNamenode> <srcDir> <oldCopyListFile>"
  exit 1
fi

srcNamenode=$1
dstNamenode=$2
srcDir=$3
dstDir=/
oldCopyListFile=$4
mapTaskNum=100

time=`date +%Y%m%d%H%M%S`
dirName=`echo $srcDir | sed 's/\///g'`

mkdir -p diff.$time/raw
./list.sh $srcNamenode $srcDir diff.$time/raw/$dirName
./diff.sh $oldCopyListFile diff.$time/raw/$dirName diff.$time/raw/${dirName}.diff.tmp
shuf diff.$time/raw/${dirName}.diff.tmp > diff.$time/raw/${dirName}.diff

mkdir -p diff.$time/split
./split.sh diff.$time/raw/${dirName}.diff diff.$time/split/$dirName $dirName $mapTaskNum

hdfsRoot=/tmp/fastcp/diff.$time
hadoop fs -mkdir -p $hdfsRoot/copylist
hadoop fs -copyFromLocal diff.$time/split/$dirName $hdfsRoot/copylist

hdfsCopyListDir=$hdfsRoot/copylist/$dirName
hdfsResultDir=$hdfsRoot/fastcp.result/$dirName
./fastcp.sh $hdfsCopyListDir $srcNamenode $dstNamenode $dstDir $hdfsResultDir FASTCOPY