package com.sogou.hadoop.extras.tools.clean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by lauo on 16/12/30.
 */
public class Delete {
  private final static Log log = LogFactory.getLog(Delete.class);

  private static Configuration conf = new Configuration();

  public static void safelyDelete(String trashRootDir,
                                  String date) throws IOException {
    Path trashDirPath = new Path(
        trashRootDir + "/" + date);
    checkSafety(trashDirPath);

    FileSystem trashFS = trashDirPath.getFileSystem(conf);
    trashFS.delete(trashDirPath, true);

  }

  private static void checkSafety(Path deletePath) throws IOException {
    //TODO need to implement
  }

  public static void main(String[] args) {
    if (args.length != 2) {
      log.error(
          "need args: " +
              "<trashRootDir> <date>");
      System.exit(1);
    }

    try {
      String trashRootDir = args[0];
      String date = args[1];
      Delete.safelyDelete(trashRootDir, date);
      System.exit(0);
    } catch (IOException e) {
      log.error(e);
      System.exit(1);
    }
  }
}
