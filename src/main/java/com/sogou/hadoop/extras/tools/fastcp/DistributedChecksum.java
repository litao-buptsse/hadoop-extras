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
public class DistributedChecksum implements Tool {
  private final static Log log = LogFactory.getLog(DistributedChecksum.class);

  private Configuration conf;
  private String checksumListDir;
  private String srcNamenode;
  private String dstNamenode;
  private String resultDir;

  public DistributedChecksum(String checksumListDir,
                             String srcNamenode, String dstNamenode, String resultDir) {
    this.checksumListDir = checksumListDir;
    this.srcNamenode = srcNamenode;
    this.dstNamenode = dstNamenode;
    this.resultDir = resultDir;
  }

  @Override
  public int run(String[] strings) throws Exception {
    Job job = new Job(getConf());

    job.setJarByClass(DistributedChecksum.class);

    job.setMapperClass(DistributedChecksumMapper.class);
    job.setReducerClass(DistributedChecksumReducer.class);

    job.setInputFormatClass(ChecksumInputFormat.class);
    ChecksumInputFormat.setChecksumListDir(job, checksumListDir);
    ChecksumInputFormat.setSrcNamenode(job, srcNamenode);
    ChecksumInputFormat.setDstNamenode(job, dstNamenode);

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
    if (args.length < 4) {
      log.error("usage: hadoop jar hadoop-extras.jar " +
          "com.sogou.hadoop.extras.tools.fastcp.DistributedChecksum " +
          "<checksumListDir> <srcNamenode> <dstNamenode> <resultDiir>");
      System.exit(1);
    }

    String checksumListDir = args[0];
    String srcNamenode = args[1];
    String dstNamenode = args[2];
    String resultDiir = args[3];

    ToolRunner.run(
        new DistributedChecksum(checksumListDir, srcNamenode, dstNamenode, resultDiir), args);
  }
}
