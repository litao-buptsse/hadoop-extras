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

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 4) {
      log.error("usage: hadoop jar hadoop-extras.jar " +
          "com.sogou.hadoop.extras.tools.fastcp.DistributedChecksum " +
          "<checksumListDir> <srcNamenode> <dstNamenode> <resultDiir>");
      return 1;
    }

    String checksumListDir = args[0];
    String srcNamenode = args[1];
    String dstNamenode = args[2];
    String resultDir = args[3];

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
    ToolRunner.run(new DistributedChecksum(), args);
  }
}
