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
public class UpdateFileInfoInputFormat extends InputFormat {
  private final Log log = LogFactory.getLog(UpdateFileInfoInputFormat.class);

  private final static String UPDATE_LIST_DIR = "updateListDir";
  private final static String NAMENODE = "namenode";

  public static void setUpdateListDir(Job job, String updadeListDir) {
    job.getConfiguration().set(UPDATE_LIST_DIR, updadeListDir);
  }

  public static void setNamenode(Job job, String namenode) {
    job.getConfiguration().set(NAMENODE, namenode);
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    Configuration conf = jobContext.getConfiguration();

    PathData updateListDir = new PathData(conf.get(UPDATE_LIST_DIR), conf);
    String namenode = conf.get(NAMENODE);
    List<InputSplit> splits = new ArrayList<>();
    for (PathData updateListFile : updateListDir.getDirectoryContents()) {
      splits.add(new UpdateFileInfoInputSplit(updateListFile.path.toString(), namenode));
      log.info("add fastcp split: " + updateListFile.path.toString() + ", " + namenode);
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
