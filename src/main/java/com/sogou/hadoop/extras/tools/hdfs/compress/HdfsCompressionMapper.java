package com.sogou.hadoop.extras.tools.hdfs.compress;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Tao Li on 2016/11/28.
 */
public class HdfsCompressionMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
  @Override
  protected void map(LongWritable key, Text value,
                     Context context) throws IOException, InterruptedException {
    context.write(NullWritable.get(), value);
  }
}
