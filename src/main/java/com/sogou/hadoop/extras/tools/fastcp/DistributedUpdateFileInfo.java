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
  private String updateListDir;
  private String namenode;
  private String resultDir;

  public DistributedUpdateFileInfo(String updateListDir, String namenode, String resultDir) {
    this.updateListDir = updateListDir;
    this.namenode = namenode;
    this.resultDir = resultDir;
  }

  @Override
  public int run(String[] strings) throws Exception {
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
    if (args.length < 3) {
      log.error("usage: hadoop jar hadoop-extras.jar " +
          "com.sogou.hadoop.extras.tools.fastcp.DistributedUpdateFileInfo " +
          "<updateListDir> <namenode> <resultDiir>");
      System.exit(1);
    }

    String updateListDir = args[0];
    String namenode = args[1];
    String resultDiir = args[2];

    ToolRunner.run(
        new DistributedUpdateFileInfo(updateListDir, namenode, resultDiir), args);
  }
}
