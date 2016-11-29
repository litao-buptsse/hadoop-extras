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
      log.error("args: <inputPath> <outputPath> " +
          "[-Dcodec=<LZO|BZIP2> -DmaxInputSplitSize=<maxInputSplitSize> " +
          "-DlzoCompressAlgorithm=<LZO1X_1|LZO1X_999> -DgenerateLzoIndex=<true|false>]");
      return 1;
    }

    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    String codec = conf.get("codec", "LZO").toUpperCase();

    String codecClass;
    switch (codec) {
      case "LZO":
        codecClass = "com.hadoop.compression.lzo.LzopCodec";
        break;
      case "BZIP2":
        codecClass = "org.apache.hadoop.io.compress.BZip2Codec";
        break;
      default:
        log.error("not support codec: " + codec);
        return 1;
    }

    long maxInputSplitSize = conf.getLong("maxInputSplitSize", Long.MAX_VALUE);

    // for lzo
    String lzoCompressAlgorithm = conf.get("lzoCompressAlgorithm", "LZO1X_1");
    boolean generateLzoIndex = conf.getBoolean("generateLzoIndex", false);

    conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
    conf.set("mapreduce.output.fileoutputformat.compress", "true");
    conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
    conf.set("mapreduce.output.fileoutputformat.compress.codec", codecClass);

    // for lzo
    conf.set("io.compression.codec.lzo.compressor", lzoCompressAlgorithm);

    // for bzip2
    conf.set("io.compression.codec.bzip2.library", "system-native");

    Job job = Job.getInstance(conf, "DistributedHdfsCompression-" + inputPath);

    job.setJarByClass(DistributedHdfsCompression.class);
    job.setMapperClass(HdfsCompressionMapper.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(CombineTextInputFormat.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    CombineTextInputFormat.setInputPaths(job, inputPath);
    CombineTextInputFormat.setInputDirRecursive(job, true);
    CombineTextInputFormat.setMaxInputSplitSize(job, maxInputSplitSize);
    FileOutputFormat.setOutputPath(job, outputPath);

    int ret = job.waitForCompletion(true) ? 0 : 1;
    // Path outputLzoPath = new Path(outputPath, "part-m-00000.lzo");

    // for lzo
    if (codec.equals("LZO") && generateLzoIndex && ret == 0) {
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
