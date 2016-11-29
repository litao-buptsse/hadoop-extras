package com.sogou.hadoop.extras.tools.example.mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Tao Li on 25/09/2016.
 */
public class LineCounter implements Tool {
  private final static Log log = LogFactory.getLog(LineCounter.class);

  private Configuration conf;

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      log.error("args: <inputPath> <outputPath>");
      return 1;
    }

    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);

    conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

    Job job = Job.getInstance(conf, "LineCounter-" + inputPath);

    job.setJarByClass(LineCounter.class);
    job.setMapperClass(LineCounterMapper.class);
    job.setCombinerClass(LineCounterReducer.class);
    job.setReducerClass(LineCounterReducer.class);
    job.setNumReduceTasks(1);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(LongWritable.class);

    TextInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

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
    ToolRunner.run(new LineCounter(), args);
  }
}
