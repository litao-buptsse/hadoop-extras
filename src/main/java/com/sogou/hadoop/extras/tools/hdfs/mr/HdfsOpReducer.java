package com.sogou.hadoop.extras.tools.hdfs.mr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Tao Li on 25/09/2016.
 */
public class HdfsOpReducer extends Reducer<Text, Text, Text, Text> {
  private final Log log = LogFactory.getLog(HdfsOpReducer.class);

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    for (Text value : values) {
      log.info("reduce output: " + key + ", " + value);
      context.write(key, value);
      context.getCounter(HdfsOpCounter.REDUCE).increment(1);
    }
  }
}
