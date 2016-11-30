package com.sogou.hadoop.extras.tools.hdfs.migrate;

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
public class DistributedHdfsMigrate implements Tool {
  private final static Log log = LogFactory.getLog(DistributedHdfsMigrate.class);

  public final static String JOB_TYPE_FASTCOPY = "FASTCOPY";
  public final static String JOB_TYPE_CHECKSUM = "CHECKSUM";
  public final static String JOB_TYPE_DELETE = "DELETE";
  public final static String JOB_TYPE_DISTCP = "DISTCP";

  private Configuration conf;

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 5) {
      log.error("need args: " +
          "<copyListDir> <srcNamenode> <dstNamenode> <dstDir> <resultDir> " +
          "[FASTCOPY|CHECKSUM|DELETE|DISTCP]");
      return 1;
    }

    String copyListDir = args[0];
    String srcNamenode = args[1];
    String dstNamenode = args[2];
    String dstDir = args[3];
    String resultDir = args[4];

    String jobType = args.length >= 6 ? args[5].toUpperCase() : JOB_TYPE_FASTCOPY;
    if (!jobType.equals(JOB_TYPE_FASTCOPY) &&
        !jobType.equals(JOB_TYPE_CHECKSUM) &&
        !jobType.equals(JOB_TYPE_DELETE) &&
        !jobType.equals(JOB_TYPE_DISTCP)) {
      log.error("invalid fastcp type: " + jobType);
      return 1;
    }

    Job job = new Job(getConf());

    job.setJobName("DistributedHdfsMigrate-" + jobType + ":" + copyListDir);
    job.setJarByClass(DistributedHdfsMigrate.class);

    job.setMapperClass(HdfsMigrateMapper.class);
    job.setReducerClass(HdfsMigrateReducer.class);

    job.setInputFormatClass(HdfsMigrateInputFormat.class);
    HdfsMigrateInputFormat.setCopyListDir(job, copyListDir);
    HdfsMigrateInputFormat.setSrcNamenode(job, srcNamenode);
    HdfsMigrateInputFormat.setDstNamenode(job, dstNamenode);
    HdfsMigrateInputFormat.setDstDir(job, dstDir);
    HdfsMigrateInputFormat.setJobType(job, jobType);

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
    ToolRunner.run(new DistributedHdfsMigrate(), args);
  }
}
