package com.sogou.hadoop.extras.tools.hdfs.mr;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Created by Tao Li on 29/09/2016.
 */
public class NullRecordReader extends RecordReader {
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return false;
  }

  @Override
  public Object getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public Object getCurrentValue() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {

  }
}
