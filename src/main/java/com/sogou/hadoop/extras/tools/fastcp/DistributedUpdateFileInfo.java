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
public class DistributedUpdateFileInfo implements Tool {
  private final static Log log = LogFactory.getLog(DistributedUpdateFileInfo.class);

  private Configuration conf;

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      log.error("usage: hadoop jar hadoop-extras.jar " +
          "com.sogou.hadoop.extras.tools.fastcp.DistributedUpdateFileInfo " +
          "<updateListDir> <namenode> <resultDiir>");
      return 1;
    }

    String updateListDir = args[0];
    String namenode = args[1];
    String resultDir = args[2];

    Job job = new Job(getConf());

    job.setJarByClass(DistributedUpdateFileInfo.class);

    job.setMapperClass(DistributedUpdateFileInfoMapper.class);
    job.setReducerClass(DistributedUpdateFileInfoReducer.class);

    job.setInputFormatClass(UpdateFileInfoInputFormat.class);
    UpdateFileInfoInputFormat.setUpdateListDir(job, updateListDir);
    UpdateFileInfoInputFormat.setNamenode(job, namenode);

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
    ToolRunner.run(new DistributedUpdateFileInfo(), args);
  }
}
