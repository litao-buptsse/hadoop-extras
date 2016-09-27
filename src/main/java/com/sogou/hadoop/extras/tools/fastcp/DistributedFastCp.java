package com.sogou.hadoop.extras.tools.fastcp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Tao Li on 25/09/2016.
 */
public class DistributedFastCp implements Tool {
  private final static Log log = LogFactory.getLog(DistributedFastCp.class);

  public final static String TYPE_NORMAL = "normal";
  public final static String TYPE_UPDATE = "update";

  private Configuration conf;
  private String copyListDir;
  private String srcNamenode;
  private String dstNamenode;
  private String dstDir;
  private String resultDir;
  private String type;

  public DistributedFastCp(String copyListDir, String srcNamenode,
                           String dstNamenode, String dstDir, String resultDir, String type) {
    this.copyListDir = copyListDir;
    this.srcNamenode = srcNamenode;
    this.dstNamenode = dstNamenode;
    this.dstDir = dstDir;
    this.resultDir = resultDir;
    this.type = type;
  }

  @Override
  public int run(String[] strings) throws Exception {
    Job job = new Job(getConf());

    job.setJarByClass(DistributedFastCp.class);

    job.setMapperClass(DistributedFastCpMapper.class);
    job.setReducerClass(DistributedFastCpReducer.class);

    job.setInputFormatClass(FastCpInputFormat.class);
    FastCpInputFormat.setCopyListDir(job, copyListDir);
    FastCpInputFormat.setSrcNamenode(job, srcNamenode);
    FastCpInputFormat.setDstNamenode(job, dstNamenode);
    FastCpInputFormat.setDstDir(job, dstDir);
    FastCpInputFormat.setType(job, type);

    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path(resultDir));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  @Override
  public void setConf(Configuration configuration) {
    conf = configuration;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 5) {
      log.error("usage: hadoop jar hadoop-extras.jar com.sogou.hadoop.extras.tools.fastcp "
          + "<copy list dir> <src namenode> <dst namenode> <dst dir> <result dir> [normal|update]");
      System.exit(1);
    }

    String copyListDir = args[0];
    String srcNamenode = args[1];
    String dstNamenode = args[2];
    String dstDir = args[3];
    String resultDir = args[4];
    String type = TYPE_NORMAL;
    if (args.length >= 6 && args[5].equals(TYPE_UPDATE)) {
      type = TYPE_UPDATE;
    }

    ToolRunner.run(
        new DistributedFastCp(copyListDir, srcNamenode, dstNamenode, dstDir, resultDir, type), args);
  }
}
