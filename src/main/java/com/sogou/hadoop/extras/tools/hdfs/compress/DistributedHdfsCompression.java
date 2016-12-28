package com.sogou.hadoop.extras.tools.hdfs.compress;

import com.hadoop.compression.lzo.LzoIndexer;
import com.sogou.hadoop.extras.common.CommonUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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

  private final static String DEFAULT_TMP_PATH = "/tmp/hdfs_compress_tmp";
  private final static String DEFAULT_TRASH_PATH = "/tmp/hdfs_compress_trash";

  private Configuration conf;

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      log.error("args: <inputPath> <filePattern> <HOT|WARM|COLD>\n" +
          "options: -DmaxInputSplitSize=<maxInputSplitSize> -DgenerateLzoIndex=<true|false>" +
          " -DfilePrefix=<prefix> -DoutputPath=<path> -DtmpPath=<path> -DtrashPath=<path>");
      return 1;
    }

    String ymd = CommonUtils.now("yyyyMMdd");
    Path inputPath = new Path(args[0], args[1]);
    Path tmpPath = new Path(String.format("%s/$s/%s",
        conf.get("tmpPath", DEFAULT_TMP_PATH), ymd, System.nanoTime()));
    Path outputPath = new Path(conf.get("outputPath", args[0]));
    Path trashPath = new Path(String.format("%s/%s",
        conf.get("trashPath", DEFAULT_TRASH_PATH), ymd));
    String compressType = args[2].toUpperCase();
    String filePrefix = conf.get("filePrefix", "");

    if (tmpPath.getFileSystem(conf).exists(tmpPath)) {
      log.error("tmpPath already exist");
      return 1;
    }

    if (!outputPath.getFileSystem(conf).exists(outputPath)) {
      log.error("outputPath not exist");
      return 1;
    }

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
    FileOutputFormat.setOutputPath(job, tmpPath);

    int ret = job.waitForCompletion(true) ? 0 : 1;

    if (ret == 0) {
      // only for lzo index
      if ((compressType.equals("HOT") || compressType.equals("WARM")) &&
          conf.getBoolean("generateLzoIndex", false)) {
        log.info("Indexing lzo file " + tmpPath);
        LzoIndexer lzoIndexer = new LzoIndexer(conf);
        lzoIndexer.index(tmpPath);
      }

      // move file from input dir to trash dir
      FileSystem inputFS = inputPath.getFileSystem(conf);
      FileSystem trashFS = trashPath.getFileSystem(conf);
      for (FileStatus file : inputFS.globStatus(inputPath)) {
        Path trashDir = new Path(trashPath + file.getPath().getParent().toUri().getPath());
        if (!trashFS.exists(trashDir)) {
          trashFS.mkdirs(trashDir);
        }
        Path trashFile = new Path(trashDir, file.getPath().getName());
        inputFS.rename(file.getPath(), trashFile);
      }

      // move file from tmp dir to output dir
      FileSystem tmpFS = tmpPath.getFileSystem(conf);
      for (FileStatus file : tmpFS.listStatus(tmpPath)) {
        tmpFS.rename(file.getPath(),
            new Path(outputPath, filePrefix + file.getPath().getName()));
      }
      // TODO delete tmp dir

      // TODO delete input dir when empty
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
    int ret = ToolRunner.run(new DistributedHdfsCompression(), args);
    System.exit(ret);
  }
}
