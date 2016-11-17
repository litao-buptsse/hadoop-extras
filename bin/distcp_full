#!/bin/bash

if [ $# -ne 3 ]; then
  echo "usage: $0 <srcNamenode> <dstNamenode> <srcDir>"
  exit 1
fi

timestamp=`date +%s`
srcNamenode=$1
dstNamenode=$2
srcDir=$3
dstDir=/
mapTaskNum=25

hdfsLogDir=/tmp/full_distcp_log_$time

hadoop distcp \
  -i -prbugp -m $mapTaskNum \
  -log $hdfsLogDir \
  $srcNamenode/$srcdir $dstNamenode/$dstDir
