package com.sogou.hadoop.extras.tools.fastcp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FastCopy;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tao Li on 25/09/2016.
 */
public class DistributedFastCpMapper extends Mapper<Text, Text, Text, Text> {
  private final Log log = LogFactory.getLog(DistributedFastCpMapper.class);

  private final static String OP_TYPE_ADD = "ADD";
  private final static String OP_TYPE_DELETE = "DELETE";
  private final static String OP_TYPE_UPDATE = "UPDATE";

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    FastCpInputSplit split = (FastCpInputSplit) context.getInputSplit();
    PathData copyListPath = new PathData(split.getCopyListPath(), context.getConfiguration());
    String srcNamenode = split.getSrcNamenode();
    String dstNamenode = split.getDstNamenode();
    String dstPath = split.getDstPath();
    String type = split.getType();

    if (!type.equals(DistributedFastCp.TYPE_NORMAL) && !type.equals(DistributedFastCp.TYPE_UPDATE)) {
      throw new IOException("no such fastcp type: " + copyListPath + ", "
          + srcNamenode + ", " + dstNamenode + ", " + dstPath + ", " + type);
    }

    FastCopy fastCopy;
    List<FastCopy.FastFileCopyRequest> requests = new ArrayList<>();

    try {
      Class<? extends FastCopy> clazz =
          (Class<? extends FastCopy>) context.getConfiguration().getClassByName("org.apache.hadoop.hdfs.FastCopyImpl");
      Class[] cArgs = new Class[3];
      cArgs[0] = Configuration.class;
      cArgs[1] = int.class;
      cArgs[2] = boolean.class;
      fastCopy = clazz.getDeclaredConstructor(cArgs).newInstance(context.getConfiguration(), 1, false);
    } catch (Exception e) {
      throw new IOException("fail to create fastcopy instance", e);
    }

    BufferedReader reader = new BufferedReader(new InputStreamReader(copyListPath.fs.open(copyListPath.path)));
    String line = reader.readLine();
    while (line != null) {
      String[] arr = line.split("\\s+");
      switch (type) {
        case DistributedFastCp.TYPE_NORMAL:
          if (arr == null || arr.length != 8) {
            log.error("invalid src file info: " + line);
          } else {
            String permission = arr[0];
            String srcPath = arr[7];
            try {
              create(context, srcNamenode, srcPath, dstNamenode, dstPath, permission, fastCopy, requests);
            } catch (Exception e) {
              context.write(new Text(srcNamenode + FastCpInputSplit.FIELD_SEPERATOR
                  + srcPath + FastCpInputSplit.FIELD_SEPERATOR
                  + dstNamenode + FastCpInputSplit.FIELD_SEPERATOR
                  + dstPath + FastCpInputSplit.FIELD_SEPERATOR + type), new Text("FAIL"));
            }
          }
          break;
        case DistributedFastCp.TYPE_UPDATE:
          if (arr == null || arr.length != 9) {
            log.error("invalid src file info: " + line);
          } else {
            String opType = arr[0];
            String permission = arr[1];
            String srcPath = arr[8];

            try {
              if (opType.equals(OP_TYPE_ADD)) {
                create(context, srcNamenode, srcPath, dstNamenode, dstPath, permission, fastCopy, requests);
              } else if (opType.equals(OP_TYPE_DELETE)) {
                delete(context, srcPath, dstNamenode, dstPath);
              } else if (opType.equals(OP_TYPE_UPDATE)) {
                update(context, srcNamenode, srcPath, dstNamenode, dstPath, permission, fastCopy, requests);
              }
            } catch (Exception e) {
              context.write(new Text(srcNamenode + FastCpInputSplit.FIELD_SEPERATOR
                  + srcPath + FastCpInputSplit.FIELD_SEPERATOR
                  + dstNamenode + FastCpInputSplit.FIELD_SEPERATOR
                  + dstPath + FastCpInputSplit.FIELD_SEPERATOR + type + "#" + opType), new Text("FAIL"));
            }
            break;
          }
      }

      line = reader.readLine();
    }

    if (fastCopy != null) {
      fastCopy.shutdown();
    }
  }

  private void create(Context context, String srcNamenode, String srcPath,
                      String dstNamenode, String dstPath, String permission,
                      FastCopy fastCopy, List<FastCopy.FastFileCopyRequest> requests) throws IOException {
    boolean isFile = permission.startsWith("-");
    PathData realSrcPath = new PathData(srcNamenode + srcPath, context.getConfiguration());
    PathData realDstPath = new PathData(dstNamenode + dstPath + srcPath, context.getConfiguration());

    if (isFile) {
      // fastcp
      requests.clear();
      requests.add(new FastCopy.FastFileCopyRequest(realSrcPath.path, realDstPath.path,
          realSrcPath.fs, realDstPath.fs));
      try {
        fastCopy.copy(requests);
        log.info("succeed fastcp: " + realSrcPath.path.toString() + ", " + realDstPath.path.toString());
      } catch (Exception e) {
        log.error("failed fastcp: " + realSrcPath.path.toString() + ", " + realDstPath.path.toString());
        throw new IOException(e);
      }
    } else {
      // mkdir
      try {
        realDstPath.fs.mkdirs(realDstPath.path);
        log.info("succeed mkdir: " + realSrcPath.path.toString() + ", " + realDstPath.path.toString());
      } catch (IOException e) {
        log.error("failed mkdir: " + realSrcPath.path.toString() + ", " + realDstPath.path.toString());
        throw new IOException(e);
      }
    }
  }

  private void delete(Context context, String srcPath, String dstNamenode, String dstPath) throws IOException {
    PathData realDstPath = new PathData(dstNamenode + dstPath + srcPath, context.getConfiguration());
    if (realDstPath.stat.isFile()) {
      try {
        realDstPath.fs.delete(realDstPath.path, false);
        log.info("succeed delete: " + realDstPath.path.toString());
      } catch (Exception e) {
        log.info("failed delete: " + realDstPath.path.toString());
        throw new IOException(e);
      }
    }
  }

  private void update(Context context, String srcNamenode, String srcPath,
                      String dstNamenode, String dstPath, String permission,
                      FastCopy fastCopy, List<FastCopy.FastFileCopyRequest> requests) throws IOException {
    delete(context, srcPath, dstNamenode, dstPath);
    create(context, srcNamenode, srcPath, dstNamenode, dstPath, permission, fastCopy, requests);
  }
}
