package com.sogou.hadoop.extras.tools.fastcp;

import com.sogou.hadoop.extras.tools.hdfs.HdfsUtils;
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

  public static void setSrcNamenode(Job job, String srcNamenode) {
    job.getConfiguration().set("srcNamenode", srcNamenode);
  }

  public static void setSrcDir(Job job, String srcDir) {
    job.getConfiguration().set("srcDir", srcDir);
  }

  public static void setDstNamenode(Job job, String dstNamenode) {
    job.getConfiguration().set("dstNamenode", dstNamenode);
  }

  public static void setDstDir(Job job, String dstDir) {
    job.getConfiguration().set("dstDir", dstDir);
  }

  public static void setLevel(Job job, int level) {
    job.getConfiguration().set("level", String.valueOf(level));
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    Configuration conf = jobContext.getConfiguration();

    String srcNamenode = conf.get("srcNamenode");
    String srcDir = conf.get("srcDir");
    String dstNamenode = conf.get("dstNamenode");
    String dstDir = conf.get("dstDir");
    int level = Integer.parseInt(conf.get("level"));

    String fullSrcDir = srcNamenode + srcDir;

    List<PathData> children = new ArrayList<>();
    HdfsUtils.listDirByLevel(new PathData(fullSrcDir, conf), children, level);

    List<InputSplit> inputSplits = new ArrayList<>();
    for (PathData child : children) {
      String childDstDir = dstNamenode + dstDir + child.path.toUri().getPath();
      inputSplits.add(new FastCpInputSplit(child.path.toString(), childDstDir));
      log.info("add fastcp split: " + child + ", " + childDstDir);
    }

    return inputSplits;
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
