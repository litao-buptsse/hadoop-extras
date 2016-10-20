package com.sogou.hadoop.extras.tools.hdfs.monitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Tao Li on 2016/10/14.
 */
public class DirStatistics implements Tool {
  private Configuration conf;
  private final static long HOT_DATA_THRESHOLD = 1000L * 3600 * 24 * 30;
  private final static long WARM_DATA_THRESHOLD = 1000L * 3600 * 24 * 30 * 6;
  private final static String MONTH_TIMEFORMAT = "yyyyMM";

  enum DataTemperature {
    HOT,
    WARM,
    COLD
  }

  static class DirInfo {
    private String dir;
    private long size = 0;
    private long fileNum = 0;
    private long dirNum = 0;
    private Map<String, Long> fileNumPerModificationTimes = new HashMap<>();
    private Map<String, Long> fileNumPerAccessTimes = new HashMap<>();
    private Map<String, Long> fileNumPerTemperatures = new HashMap<>();
    private Map<String, Long> sizePerModificationTimes = new HashMap<>();
    private Map<String, Long> sizePerAccessTimes = new HashMap<>();
    private Map<String, Long> sizePerTemperatures = new HashMap<>();

    public DirInfo(String dir) {
      this.dir = dir;
    }

    @Override
    public String toString() {
      return "DirInfo{" +
          "dir='" + dir + '\'' +
          ", size=" + size +
          ", fileNum=" + fileNum +
          ", dirNum=" + dirNum +
          ", fileNumPerModificationTimes=" + fileNumPerModificationTimes +
          ", fileNumPerAccessTimes=" + fileNumPerAccessTimes +
          ", fileNumPerTemperatures=" + fileNumPerTemperatures +
          ", sizePerModificationTimes=" + sizePerModificationTimes +
          ", sizePerAccessTimes=" + sizePerAccessTimes +
          ", sizePerTemperatures=" + sizePerTemperatures +
          '}';
    }
  }

  private static void traverse(PathData root, DirInfo info) throws IOException {
    if (!root.exists) {
      return;
    }

    if (root.stat.isFile()) {
      if (info.fileNum % 1000000 == 0) {
        System.err.println("[COUNTER] " + info.dir + ": " + info.fileNum);
      }

      info.fileNum += 1;
      long size = root.stat.getLen();
      info.size += size;

      long modificationTime = root.stat.getModificationTime();
      long accessTime = root.stat.getAccessTime();

      DataTemperature temperature = calculateDataTemperature(modificationTime, accessTime);
      putOrIncrease(info.fileNumPerTemperatures, temperature.toString(), 1L);
      putOrIncrease(info.sizePerTemperatures, temperature.toString(), size);

      String modificationMonth = convertMillisecondsToString(modificationTime, MONTH_TIMEFORMAT);
      putOrIncrease(info.fileNumPerModificationTimes, modificationMonth, 1L);
      putOrIncrease(info.sizePerModificationTimes, modificationMonth, size);

      String accessMonth = convertMillisecondsToString(accessTime, MONTH_TIMEFORMAT);
      putOrIncrease(info.fileNumPerAccessTimes, accessMonth, 1L);
      putOrIncrease(info.sizePerAccessTimes, accessMonth, size);
    } else {
      info.dirNum += 1;
      try {
        PathData[] children = root.getDirectoryContents();
        for (PathData child : children) {
          try {
            traverse(child, info);
          } catch (IOException e) {
            // ignore
          }
        }
      } catch (IOException e) {
        // ignore
      }
    }
  }

  private static String convertMillisecondsToString(long milliseconds, String format) {
    return new SimpleDateFormat(format).format(new Date(milliseconds)).toString();
  }

  private static DataTemperature calculateDataTemperature(long modificationTime, long accessTime) {
    long diff = accessTime - modificationTime;
    if (diff <= HOT_DATA_THRESHOLD) {
      return DataTemperature.HOT;
    } else {
      if (diff <= WARM_DATA_THRESHOLD) {
        return DataTemperature.WARM;
      } else {
        return DataTemperature.COLD;
      }
    }
  }

  private static void putOrIncrease(Map<String, Long> map, String key, long value) {
    if (!map.containsKey(key)) {
      map.put(key, 0L);
    }
    map.put(key, map.get(key) + value);
  }

  @Override
  public int run(String[] args) throws Exception {
    int nThreads = conf.getInt("com.sogou.hadoop.extras.dir-statistics.threads", 5);
    ExecutorService service = Executors.newFixedThreadPool(nThreads);
    for (final String path : args) {
      service.submit(new Runnable() {
        @Override
        public void run() {
          try {
            PathData root = new PathData(path, conf);
            DirInfo info = new DirInfo(path);
            traverse(root, info);
            System.out.println(info);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
    }
    service.shutdown();
    return 0;
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
    ToolRunner.run(new DirStatistics(), args);
  }
}