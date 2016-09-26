package com.sogou.hadoop.extras.tools.fastcp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tao Li on 25/09/2016.
 */
public class FastCpInputFormat extends InputFormat {
  private final Log log = LogFactory.getLog(FastCpInputFormat.class);

  private final static String COPY_LIST_DIR = "copyListDir";
  private final static String SRC_NAMENODE = "srcNamenode";
  private final static String DST_NAMENODE = "dstNamenode";
  private final static String DST_DIR = "dstDir";

  public static void setCopyListDir(Job job, String copyListDir) {
    job.getConfiguration().set(COPY_LIST_DIR, copyListDir);
  }

  public static void setSrcNamenode(Job job, String srcNamenode) {
    job.getConfiguration().set(SRC_NAMENODE, srcNamenode);
  }

  public static void setDstNamenode(Job job, String dstNamenode) {
    job.getConfiguration().set(DST_NAMENODE, dstNamenode);
  }

  public static void setDstDir(Job job, String dstDir) {
    job.getConfiguration().set(DST_DIR, dstDir);
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    Configuration conf = jobContext.getConfiguration();

    PathData copyListDir = new PathData(conf.get(COPY_LIST_DIR), conf);
    String srcNamenode = conf.get(SRC_NAMENODE);
    String dstNamenode = conf.get(DST_NAMENODE);
    String dstDir = conf.get(DST_DIR);

    List<InputSplit> splits = new ArrayList<>();
    for (PathData copyListFile : copyListDir.getDirectoryContents()) {
      splits.add(new FastCpInputSplit(copyListFile.path.toString(),
          srcNamenode, dstNamenode, dstDir));
      log.info("add fastcp split: " + copyListFile.path.toString() + ", "
          + srcNamenode + ", " + dstNamenode + ", " + dstDir);
    }

    return splits;
  }

  @Override
  public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new RecordReader() {
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
    };
  }
}
