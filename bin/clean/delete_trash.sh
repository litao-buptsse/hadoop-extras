#!/bin/bash

if [ $# -lt 3 ]; then
  echo "usage: $0 <trashRootDir> <date>"
  exit 1
fi

trashRootDir=$1
date=$2

hadoop jar hadoop-extras-1.0-SNAPSHOT.jar \
  com/sogou/hadoop/extras/tools/clean/Delete.java \
  $trashRootDir $date
