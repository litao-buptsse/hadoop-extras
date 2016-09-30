package com.sogou.hadoop.extras.tools.hdfs.fastcp;

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
public class FastCopyInputFormat extends InputFormat {
  private final Log log = LogFactory.getLog(FastCopyInputFormat.class);

  private final static String CONFIG_COPY_LIST_DIR = "copyListDir";
  private final static String CONFIG_SRC_NAMENODE = "srcNamenode";
  private final static String CONFIG_DST_NAMENODE = "dstNamenode";
  private final static String CONFIG_DST_DIR = "dstDir";
  private final static String CONFIG_JOB_TYPE = "jobType";

  public static void setCopyListDir(Job job, String copyListDir) {
    job.getConfiguration().set(CONFIG_COPY_LIST_DIR, copyListDir);
  }

  public static void setSrcNamenode(Job job, String srcNamenode) {
    job.getConfiguration().set(CONFIG_SRC_NAMENODE, srcNamenode);
  }

  public static void setDstNamenode(Job job, String dstNamenode) {
    job.getConfiguration().set(CONFIG_DST_NAMENODE, dstNamenode);
  }

  public static void setDstDir(Job job, String dstDir) {
    job.getConfiguration().set(CONFIG_DST_DIR, dstDir);
  }

  public static void setJobType(Job job, String jobType) {
    job.getConfiguration().set(CONFIG_JOB_TYPE, jobType);
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();

    PathData copyListDir = new PathData(conf.get(CONFIG_COPY_LIST_DIR), conf);
    String srcNamenode = conf.get(CONFIG_SRC_NAMENODE);
    String dstNamenode = conf.get(CONFIG_DST_NAMENODE);
    String dstDir = conf.get(CONFIG_DST_DIR);
    String jobType = conf.get(CONFIG_JOB_TYPE);

    List<InputSplit> splits = new ArrayList<>();
    for (PathData copyListFile : copyListDir.getDirectoryContents()) {
      InputSplit split = new FastCopyInputSplit(copyListFile.path.toString(),
          srcNamenode, dstNamenode, dstDir, jobType);
      splits.add(split);
      log.info("add fastcp split: " + split.toString());
    }

    return splits;
  }

  @Override
  public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new NullRecordReader();
  }
}
