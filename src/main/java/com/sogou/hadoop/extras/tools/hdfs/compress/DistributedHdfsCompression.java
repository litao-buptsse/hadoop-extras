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
      log.error("args: <inputPath> <outputPath> [HOT|WARM|COLD]\n" +
          "options: -DmaxInputSplitSize=<maxInputSplitSize> -DgenerateLzoIndex=[true|false]");
      return 1;
    }

    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    String compressType = args.length >= 3 ? args[2].toUpperCase() : "HOT";

    conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
    conf.set("mapreduce.output.fileoutputformat.compress", "true");
    conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");

    switch (compressType) {
      case "HOT":
        conf.set("mapreduce.output.fileoutputformat.compress.codec",
            "com.hadoop.compression.lzo.LzopCodec");
        conf.set("io.compression.codec.lzo.compressor", "LZO1X_1");
        break;
      case "WARM":
        conf.set("mapreduce.output.fileoutputformat.compress.codec",
            "com.hadoop.compression.lzo.LzopCodec");
        conf.set("io.compression.codec.lzo.compressor", "LZO1X_999");
        break;
      case "COLD":
        conf.set("mapreduce.output.fileoutputformat.compress.codec",
            "org.apache.hadoop.io.compress.BZip2Codec");
        conf.set("io.compression.codec.bzip2.library", "system-native");
        break;
      default:
        log.error("not support compress type: " + compressType);
        return 1;
    }

    Job job = Job.getInstance(conf,
        String.format("DistributedHdfsCompression-%s-%s", compressType, inputPath));

    job.setJarByClass(DistributedHdfsCompression.class);
    job.setMapperClass(HdfsCompressionMapper.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(CombineTextInputFormat.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    CombineTextInputFormat.setInputPaths(job, inputPath);
    CombineTextInputFormat.setInputDirRecursive(job, true);
    CombineTextInputFormat.setMaxInputSplitSize(job,
        conf.getLong("maxInputSplitSize", Long.MAX_VALUE));
    FileOutputFormat.setOutputPath(job, outputPath);

    int ret = job.waitForCompletion(true) ? 0 : 1;

    // only for lzo index
    if (ret == 0 && (compressType.equals("HOT") || compressType.equals("WARM")) &&
        conf.getBoolean("generateLzoIndex", false)) {
      log.info("Indexing lzo file " + outputPath);
      LzoIndexer lzoIndexer = new LzoIndexer(conf);
      lzoIndexer.index(outputPath);
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
