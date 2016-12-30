package com.sogou.hadoop.extras.tools.clean;

import com.sogou.hadoop.extras.common.CommonUtils;
import com.sogou.hadoop.extras.common.HiveUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by lauo on 16/12/27.
 * move target files to trashRootDir, saving their absolute path under the trashRootDir.
 */
public class Clean {
  private final static Log log = LogFactory.getLog(Clean.class);

  private static Configuration conf = new Configuration();

  private final static String today = CommonUtils.now("yyyyMMdd");

  public static void safelyClean(String db, String table, List<String> partitionVals,
                                 String trashRootDir) throws IOException, TException {
    String dirPattern = new Path(
        HiveUtils.getLocation(db, table, partitionVals)
    ).toUri().getPath();
    safelyClean(dirPattern, "*", trashRootDir);
    HiveUtils.dropPartition(db, table, partitionVals, false);
  }

  public static void safelyClean(String dirPattern, String filePattern,
                                 String trashRootDir) throws IOException {
    safelyClean(dirPattern + "/" + filePattern, trashRootDir);
  }

  public static void safelyClean(String pathPattern,
                                 String trashRootDir) throws IOException {
    Path path = new Path(pathPattern);
    FileSystem fs = path.getFileSystem(conf);

    FileStatus[] fileStatuses = fs.globStatus(path);

    for (FileStatus fileStatus : fileStatuses) {
      Path trashDirPath = new Path(
          trashRootDir + "/" + today + "/" + fileStatus.getPath().getParent().toUri().getPath());
      if (!fs.exists(trashDirPath)) {
        fs.mkdirs(trashDirPath);
      }
      Path trashFilePath = new Path(trashDirPath, fileStatus.getPath().getName());
      fs.rename(fileStatus.getPath(), trashFilePath);
    }
  }

  public static void main(String[] args) {
    if (args.length != 4 && args.length != 5) {
      log.error(
          "need args: " +
              "<type> <dirPattern> <filePattern> <trashRootDir>" +
              " or <type> <db> <table> <partitionValStrs> <trashRootDir>");
      System.exit(1);
    }

    String type = args[0];
    String trashRootDir = args[args.length - 1];
    try {
      if ("HDFS".equals(type) && args.length == 4) {
        String dirPattern = args[1];
        String filePattern = args[2];
        Clean.safelyClean(dirPattern, filePattern, trashRootDir);
      } else if ("HIVE".equals(type) && args.length == 5) {
        String db = args[1];
        String table = args[2];
        String partitionValStrs = args[3];
        List<String> partitionVals = Arrays.asList(
            partitionValStrs.split("\\s*/\\s*"));
        Clean.safelyClean(db, table, partitionVals, trashRootDir);
      } else {
        log.error(String.format("type with given args not exists: %s", type));
        System.exit(1);
      }
      System.exit(0);
    } catch (IOException | TException e) {
      log.error(e);
      System.exit(1);
    }
  }

}
