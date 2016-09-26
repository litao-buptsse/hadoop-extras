package com.sogou.hadoop.extras.tools.hdfs;

import org.apache.hadoop.fs.shell.PathData;

import java.io.IOException;
import java.util.List;

/**
 * Created by Tao Li on 26/09/2016.
 */
public class HdfsUtils {
  private static void listDirByLevel(PathData dir, List<PathData> children,
                                     int level, int maxLevel) throws IOException {
    children.add(dir);

    if (level >= maxLevel) {
      return;
    }

    if (dir.stat.isDirectory()) {
      for (PathData child : dir.getDirectoryContents()) {
        listDirByLevel(child, children, level + 1, maxLevel);
      }
    }
  }

  public static void listDirByLevel(PathData dir, List<PathData> children,
                                    int maxLevel) throws IOException {
    if (maxLevel < 1) {
      throw new IOException("max level must greater or equal than 1");
    }
    listDirByLevel(dir, children, 1, maxLevel);
  }
}
