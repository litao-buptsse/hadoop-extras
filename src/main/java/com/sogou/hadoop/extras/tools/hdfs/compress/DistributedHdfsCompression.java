package com.sogou.hadoop.extras.tools.hdfs.compress;

import com.hadoop.compression.lzo.LzoIndexer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Tao Li on 25/09/2016.
 */
public class DistributedHdfsCompression implements Tool {
  private final static Log log = LogFactory.getLog(DistributedHdfsCompression.class);

  private Configuration conf;

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      log.error("args: <inputPath> <dstPath> [lzoCompressAlgorithm] [lzoIndex]");
      return 1;
    }

    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    String lzoCompressor = args.length >= 3 ? args[2] : "LZO1X_1";
    boolean generateIndex = args.length >= 4 ? Boolean.parseBoolean(args[3]) : false;

    conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
    conf.set("mapreduce.output.fileoutputformat.compress", "true");
    conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
    conf.set("mapreduce.output.fileoutputformat.compress.codec", "com.hadoop.compression.lzo.LzopCodec");
    conf.set("io.compression.codec.lzo.compressor", lzoCompressor);

    Job job = Job.getInstance(conf, "DistributedHdfsCompression-" + inputPath);

    job.setJarByClass(DistributedHdfsCompression.class);
    job.setMapperClass(HdfsCompressionMapper.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(CombineTextInputFormat.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    CombineTextInputFormat.setInputPaths(job, inputPath);
    CombineTextInputFormat.setInputDirRecursive(job, true);
    CombineTextInputFormat.setMaxInputSplitSize(job, Long.MAX_VALUE);
    FileOutputFormat.setOutputPath(job, outputPath);

    int ret = job.waitForCompletion(true) ? 0 : 1;
    Path outputLzoPath = new Path(outputPath, "part-m-00000.lzo");
    if (generateIndex && ret == 0 && outputLzoPath.getFileSystem(conf).exists(outputLzoPath)) {
      LzoIndexer lzoIndexer = new LzoIndexer(conf);
      log.info("Indexing lzo file " + outputLzoPath);
      lzoIndexer.index(outputLzoPath);
    }

    return ret;
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
    ToolRunner.run(new DistributedHdfsCompression(), args);
  }
}
