package com.sogou.hadoop.extras.tools.hdfs.fastcp;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Tao Li on 27/09/2016.
 */
public class DiffFileList {
  static class FileInfo {
    private String info;
    private boolean exist = false;

    public FileInfo(String info) {
      this.info = info;
    }

    public void setExist(boolean exist) {
      this.exist = exist;
    }

    public boolean isExist() {
      return exist;
    }

    public String getInfo() {
      return info;
    }
  }

  public static void main(String[] args) throws IOException {
    File srcFile = new File(args[0]);
    File dstFile = new File(args[1]);

    Map<String, FileInfo> srcMap = new HashMap<>();
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(srcFile)));
    String line = reader.readLine();
    int counter = 1;
    while (line != null) {
      if (counter % 1000000 == 0) {
        System.err.println("src counter: " + counter);
      }
      String[] arr = line.split("\\s+");
      if (arr == null || arr.length != 8) {
        System.err.println("invalid src file info: " + line);
      } else {
        String path = arr[7];
        // skip .Trash dir
        if (!path.contains("/.Trash/") &&
            !path.contains("/_temporary/") &&
            !path.contains("/_distcp_logs_")) {
          String info = String.format("%s %s %s %s %s %s %s",
              arr[0], arr[1], arr[2], arr[3], arr[4], arr[5], arr[6]);
          FileInfo fileInfo = new FileInfo(info);
          srcMap.put(path, fileInfo);
        }
      }

      line = reader.readLine();
      counter++;
    }
    reader.close();

    reader = new BufferedReader(new InputStreamReader(new FileInputStream(dstFile)));
    line = reader.readLine();
    counter = 1;
    while (line != null) {
      if (counter % 1000000 == 0) {
        System.err.println("dst counter: " + counter);
      }

      String[] arr = line.split("\\s+");
      if (arr == null || arr.length != 8) {
        System.err.println("invalid dst file info: " + line);
      } else {
        String path = arr[7];
        // skip .Trash dir
        if (!path.contains("/.Trash/") &&
            !path.contains("/_temporary/") &&
            !path.contains("/_distcp_logs_")) {
          String info = String.format("%s %s %s %s %s %s %s",
              arr[0], arr[1], arr[2], arr[3], arr[4], arr[5], arr[6]);
          if (srcMap.containsKey(path)) {
            // exists
            srcMap.get(path).setExist(true);
            if (!srcMap.get(path).getInfo().equals(info)) {
              // update
              System.out.println("UPDATE " + line);
            }
          } else {
            // not exists
            System.out.println("ADD " + line);
          }
        }
      }

      line = reader.readLine();
      counter++;
    }
    reader.close();

    for (Map.Entry<String, FileInfo> entry : srcMap.entrySet()) {
      if (!entry.getValue().isExist()) {
        System.out.println("DELETE " + entry.getValue().getInfo() + " " + entry.getKey());
      }
    }
  }
}
