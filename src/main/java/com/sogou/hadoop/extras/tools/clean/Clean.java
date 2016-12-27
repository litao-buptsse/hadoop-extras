package com.sogou.hadoop.extras.tools.clean;

import com.sogou.hadoop.extras.common.HiveUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.joda.time.DateTime;

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

  private final static String today = DateTime.now().toString("yyyyMMdd");

  public static boolean clean(
      String dirPattern, String filePattern
      , String trashRootDir
  ) throws IOException {
    if (!dirPattern.endsWith("/")) {
      dirPattern += "/";
    }
    log.debug("dirPattern=" + dirPattern);
    return clean(dirPattern + filePattern, trashRootDir);
  }

  public static boolean clean(
      String db, String table, List<String> partitionVals
      , String trashRootDir
  ) throws IOException, TException {
    String dirPattern = new Path(
        HiveUtils.getLocation(db, table, partitionVals)
    ).toUri().getPath();
    log.debug("dirPattern=" + dirPattern);
    boolean cleanFileSuccess = clean(dirPattern, "*", trashRootDir);
    if (cleanFileSuccess) {
      HiveUtils.dropPartition(db, table, partitionVals, false);
    }
    return cleanFileSuccess;
  }

  public static boolean clean(
      String pathPattern
      , String trashRootDir
  ) throws IOException {
    Path path = new Path(pathPattern);
    FileSystem fs = path.getFileSystem(conf);

    FileStatus[] fileStatuses = fs.globStatus(path);
    if (fileStatuses.length == 0) {
      throw new IOException(
          String.format("no path match pattern: %s", pathPattern));
    }

    if (!trashRootDir.endsWith("/")) {
      trashRootDir += "/";
    }

    boolean success = false;
    Path trashDirPath = null;
    FileSystem trashFS = new Path(trashRootDir).getFileSystem(conf);
    for (FileStatus fileStatus : fileStatuses) {
      trashDirPath = new Path(
          trashRootDir + today + "/" + fileStatus.getPath().getParent().toUri().getPath());
      if (!trashFS.exists(trashDirPath)) {
        trashFS.mkdirs(trashDirPath);
      }
      Path trashFilePath = new Path(trashDirPath, fileStatus.getPath().getName());
      log.debug("fileStatus.getPath()=" + fileStatus.getPath());
      log.debug("trashDirPath=" + trashDirPath);
      if (!(success = trashFS.rename(fileStatus.getPath(), trashFilePath))) {
        log.debug("success=" + success);
        break;
      }
    }

    return success;
  }

  public static void main(String[] args) {
    if (args.length != 1 + 2 + 1 && args.length != 1 + 3 + 1) {
      log.error(
          "need args: " +
              "<type> <dirPattern> <filePattern> <trashRootDir>" +
              " or <type> <db> <table> <partitionValStrs> <trashRootDir>");
      System.exit(1);
    }

    String type = args[0];
    if (!"HDFS".equals(type) && !"Hive".equals(type)) {
      log.error(String.format("type not exists: %s", type));
      System.exit(1);
    }

    String trashRootDir = args[args.length - 1];
    log.debug("trashRootDir=" + trashRootDir);

    try {
      boolean rs = false;
      if ("HDFS".equals(type) && args.length == 1 + 2 + 1) {
        String dirPattern = args[1];
        String filePattern = args[2];
        rs = Clean.clean(dirPattern, filePattern, trashRootDir);
      } else {//if ("Hive".equals(type) && args.length == 1 + 3 + 1) {
        String db = args[1];
        String table = args[2];
        String partitionValStrs = args[3];
        List<String> partitionVals = Arrays.asList(
            partitionValStrs.split("\\s*/\\s*"));
        rs = Clean.clean(db, table, partitionVals, trashRootDir);
      }
      System.exit(rs ? 0 : 1);
    } catch (IOException | TException e) {
      log.error(e);
      System.exit(1);
    }
  }

}
