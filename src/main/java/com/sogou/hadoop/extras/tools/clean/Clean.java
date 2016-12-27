package com.sogou.hadoop.extras.tools.clean;

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
 * move target files to trashDir, saving their absolute path under the trashDir.
 */
public class Clean {
  private final static Log log = LogFactory.getLog(Clean.class);

  private static Configuration conf = new Configuration();

  public static boolean clean(
      String dirPattern, String filePattern
      , String trashDir
  ) throws IOException {
    if (!dirPattern.endsWith("/")) {
      dirPattern += "/";
    }
    return clean(dirPattern + filePattern, trashDir);
  }

  public static boolean clean(
      String db, String table, List<String> partitionVals
      , String trashDir
  ) throws IOException, TException {
    String dirPattern = new Path(
        HiveUtils.getLocation(db, table, partitionVals)
    ).toUri().getPath();
    boolean cleanFileSuccess = clean(dirPattern, "*", trashDir);
    if (cleanFileSuccess) {
      HiveUtils.dropPartition(db, table, partitionVals, false);
    }
    return cleanFileSuccess;
  }

  public static boolean clean(
      String pathPattern
      , String trashDir
  ) throws IOException {
    Path path = new Path(pathPattern);
    FileSystem fs = path.getFileSystem(conf);

    FileStatus[] fileStatuses = fs.globStatus(path);
    if (fileStatuses.length == 0) {
      throw new IOException(
          String.format("no path match pattern: %s", pathPattern));
    }

    if (!trashDir.endsWith("/")) {
      trashDir += "/";
    }

    boolean success = false;
    Path trashPath = null;
    for (FileStatus fileStatus : fileStatuses) {
      trashPath = new Path(trashDir + fileStatus.getPath().getParent().toUri().getPath());
      if (!(success = fs.rename(fileStatus.getPath(), trashPath))) {
        break;
      }
    }

    return success;
  }

  public static void main(String[] args) {
    if (args.length != 1 + 2 + 1 && args.length != 1 + 3 + 1) {
      log.error(
          "need args: " +
              "<type> <dirPattern> <filePattern> <trashDir>" +
              " or <type> <db> <table> <partitionValStrs> <trashDir>");
      System.exit(1);
    }

    String type = args[0];
    if (!"HDFS".equals(type) && !"Hive".equals(type)) {
      log.error(String.format("type not exists: %s", type));
      System.exit(1);
    }

    String trashDir = args[args.length - 1];
    try {
      boolean rs = false;
      if ("HDFS".equals(type) && args.length == 1 + 2 + 1) {
        String dirPattern = args[1];
        String filePattern = args[2];
        rs = Clean.clean(dirPattern, filePattern, trashDir);
      } else {//if ("Hive".equals(type) && args.length == 1 + 3 + 1) {
        String db = args[1];
        String table = args[2];
        String partitionValStrs = args[3];
        List<String> partitionVals = Arrays.asList(
            partitionValStrs.split("\\s+/\\s+"));
        rs = Clean.clean(db, table, partitionVals, trashDir);
      }
      System.out.println(rs);
      System.exit(0);
    } catch (IOException | TException e) {
      log.error(e);
      System.exit(1);
    }
  }

}
