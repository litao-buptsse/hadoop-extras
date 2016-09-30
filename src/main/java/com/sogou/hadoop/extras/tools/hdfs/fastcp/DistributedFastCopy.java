package com.sogou.hadoop.extras.tools.hdfs.fastcp;

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
public class DistributedFastCopy implements Tool {
  private final static Log log = LogFactory.getLog(DistributedFastCopy.class);

  public final static String JOB_TYPE_FASTCOPY = "FASTCOPY";
  public final static String JOB_TYPE_CHECKSUM = "CHECKSUM";

  private Configuration conf;

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 5) {
      log.error("usage: hadoop jar hadoop-extras.jar " +
          "com.sogou.hadoop.extras.tools.hdfs.fastcp.DistributedFastCopy " +
          "<copyListDir> <srcNamenode> <dstNamenode> <dstDir> <resultDir> [FASTCOPY|CHECKSUM]");
      return 1;
    }

    String copyListDir = args[0];
    String srcNamenode = args[1];
    String dstNamenode = args[2];
    String dstDir = args[3];
    String resultDir = args[4];

    String jobType = args.length >= 6 ? args[5].toUpperCase() : JOB_TYPE_FASTCOPY;
    if (!jobType.equals(JOB_TYPE_FASTCOPY) && !jobType.equals(JOB_TYPE_CHECKSUM)) {
      log.error("invalid fastcp type: " + jobType);
      return 1;
    }

    Job job = new Job(getConf());

    job.setJobName("DistributedFastCopy-" + jobType + ":" + copyListDir);
    job.setJarByClass(DistributedFastCopy.class);

    job.setMapperClass(FastCopyMapper.class);
    job.setReducerClass(FastCopyReducer.class);

    job.setInputFormatClass(FastCopyInputFormat.class);
    FastCopyInputFormat.setCopyListDir(job, copyListDir);
    FastCopyInputFormat.setSrcNamenode(job, srcNamenode);
    FastCopyInputFormat.setDstNamenode(job, dstNamenode);
    FastCopyInputFormat.setDstDir(job, dstDir);
    FastCopyInputFormat.setJobType(job, jobType);

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
    ToolRunner.run(new DistributedFastCopy(), args);
  }
}
