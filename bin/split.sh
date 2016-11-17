#!/bin/bash

if [ $# -le 3 ]; then
  echo "usage: $0 <input> <output_dir> <output_prefix> [map_task_num]"
  exit 1
fi

input=$1
output_dir=$2
output_prefix=$3
map_task_num=100
if [ $# -ge 4 ]; then
  map_task_num=$4
fi
file_count=`wc -l $input | awk '{print $1}'`
split_size=`expr ${file_count} / ${map_task_num}`
if [ $split_size -eq 0 ]; then
  split_size=1
fi

mkdir -p $output_dir
split -l $split_size -a 5 $input $output_dir/$output_prefix.