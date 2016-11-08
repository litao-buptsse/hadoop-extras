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
    private String time;
    private boolean exist = false;

    public FileInfo(String info, String time) {
      this.info = info;
      this.time = time;
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

    public String getTime() {
      return time;
    }
  }

  public static void main(String[] args) throws IOException {
    File srcFile = new File(args[0]);
    File dstFile = new File(args[1]);

    boolean ignoreTime = false;
    if (args.length == 3) {
      ignoreTime = Boolean.parseBoolean(args[2]);
    }

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
          String info = String.format("%s %s %s %s %s", arr[0], arr[1], arr[2], arr[3], arr[4]);
          String time = String.format("%s %s", arr[5], arr[6]);
          FileInfo fileInfo = new FileInfo(info, time);
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
          String info = String.format("%s %s %s %s %s", arr[0], arr[1], arr[2], arr[3], arr[4]);
          String time = String.format("%s %s", arr[5], arr[6]);
          if (srcMap.containsKey(path)) {
            // exists
            srcMap.get(path).setExist(true);
            String srcValue = ignoreTime ? srcMap.get(path).getInfo() :
                String.format("%s %s", srcMap.get(path).getInfo(), srcMap.get(path).getTime());
            String dstValue = ignoreTime ? info : String.format("%s %s", info, time);
            if (!srcValue.equals(dstValue)) {
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
        System.out.println(String.format("DELETE %s %s %s",
            entry.getValue().getInfo(), entry.getValue().getTime(), entry.getKey()));
      }
    }
  }
}
