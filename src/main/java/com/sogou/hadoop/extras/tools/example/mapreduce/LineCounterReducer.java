package com.sogou.hadoop.extras.tools.example.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Tao Li on 2016/11/28.
 */
public class LineCounterReducer extends Reducer<NullWritable, LongWritable, NullWritable, LongWritable> {
  @Override
  protected void reduce(NullWritable key, Iterable<LongWritable> values,
                        Context context) throws IOException, InterruptedException {
    long sum = 0;
    for (LongWritable value : values) {
      sum += value.get();
    }
    context.write(NullWritable.get(), new LongWritable(sum));
  }
}
