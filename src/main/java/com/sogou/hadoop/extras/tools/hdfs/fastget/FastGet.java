package com.sogou.hadoop.extras.tools.hdfs.fastget;

import com.sogou.hadoop.extras.tools.hdfs.util.ThrottledInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by Tao Li on 2016/8/16.
 */
public class FastGet implements Tool {
  private static final Log LOG = LogFactory.getLog(FastGet.class);

  private Configuration conf;

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      LOG.error("need args: <src> <localdst>");
      return 1;
    }

    String src = args[0];
    String localdst = args[1];

    int threadNum = conf.getInt("dfs.fastget.threadnum", 3);
    long blockSize = conf.getInt("dfs.fastget.blocksize.mb", 128) * 1024 * 1024;
    long maxBytesPerSec = conf.getInt("dfs.fastget.bandwidth.mb", 0) * 1024 * 1024;

    FSDataInputStream rawInput = null;
    RandomAccessFile dstFile = null;
    ExecutorService service = null;

    try {
      Path srcPath = new Path(src);
      Configuration conf = new Configuration();
      FileSystem srcFS = srcPath.getFileSystem(conf);
      FileStatus srcFile = srcFS.getFileStatus(srcPath);

      if (srcFile.isDirectory()) {
        LOG.error("Not support FastGet for directory");
        return 1;
      }

      rawInput = srcFS.open(srcPath);
      ThrottledInputStream input = maxBytesPerSec == 0 ?
          new ThrottledInputStream(rawInput) : new ThrottledInputStream(rawInput, maxBytesPerSec);
      long length = srcFile.getLen();

      File dst = new File(localdst);
      if (dst.isDirectory()) {
        localdst = srcPath.getName();
        dst = new File(dst, localdst);
      }
      if (dst.isFile()) {
        LOG.error(String.format("%s is already exist", localdst));
        return 1;
      }

      dstFile = new RandomAccessFile(localdst, "rw");

      service = Executors.newFixedThreadPool(threadNum);
      List<Future<Long>> futures = new ArrayList<Future<Long>>();
      long position = 0;

      // submit tasks for each block
      for (; position <= length - blockSize; position += blockSize) {
        futures.add(service.submit(new GetTask(input, dstFile, position, blockSize)));
      }
      if (length > position) {
        futures.add(service.submit(new GetTask(input, dstFile, position, length - position)));
      }

      // wait for each task complete
      for (Future<Long> future : futures) {
        long bytes = future.get();
        LOG.debug("download " + bytes + " byte");
      }

      return 0;
    } catch (Exception e) {
      LOG.error("Exception Occured: ", e);
      return 1;
    } finally {
      if (rawInput != null) {
        try {
          rawInput.close();
        } catch (IOException e) {
          // ignore it
        }
      }
      if (dstFile != null) {
        try {
          dstFile.close();
        } catch (IOException e) {
          // ignore it
        }
      }
      if (service != null) {
        service.shutdown();
      }
    }
  }

  @Override
  public void setConf(Configuration configuration) {
    conf = configuration;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }


  static class GetTask implements Callable<Long> {
    private final static int BUFFER_SIZE = 256 * 1024;

    private ThrottledInputStream input;
    RandomAccessFile dst;
    private long offset;
    private long length;
    private long totalBytesRead = 0;
    private byte[] buffer;

    public GetTask(ThrottledInputStream input, RandomAccessFile dst, long offset, long length) {
      this.input = input;
      this.dst = dst;
      this.offset = offset;
      this.length = length;
      buffer = new byte[BUFFER_SIZE];
    }

    @Override
    public Long call() throws Exception {
      int bytesRead = 0;
      while (bytesRead >= 0 && totalBytesRead < length) {
        int toRead = (int) Math.min(buffer.length, length - bytesRead);
        bytesRead = input.read(offset + totalBytesRead, buffer, 0, toRead);

        if (bytesRead < 0) {
          break;
        }

        synchronized (dst) {
          dst.seek(offset + totalBytesRead);
          dst.write(buffer, 0, bytesRead);
          dst.getFD().sync();
        }

        totalBytesRead += bytesRead;
      }
      return totalBytesRead;
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new FastGet(), args);
  }
}