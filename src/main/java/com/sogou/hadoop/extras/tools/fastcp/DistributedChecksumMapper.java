package com.sogou.hadoop.extras.tools.fastcp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by Tao Li on 25/09/2016.
 */
public class DistributedChecksumMapper extends Mapper<Text, Text, Text, Text> {
  private final Log log = LogFactory.getLog(DistributedChecksumMapper.class);

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    ChecksumInputSplit split = (ChecksumInputSplit) context.getInputSplit();
    PathData checksumListPath = new PathData(split.getChecksumListPath(), context.getConfiguration());
    String srcNamenode = split.getSrcNamenode();
    String dstNamenode = split.getDstNamenode();

    BufferedReader reader = new BufferedReader(
        new InputStreamReader(checksumListPath.fs.open(checksumListPath.path)));
    String line = reader.readLine();
    while (line != null) {
      String[] arr = line.split("\\s+");
      if (arr == null || (arr.length != 8 && arr.length != 9)) {
        log.error("invalid src file info: " + line);
      } else {
        try {
          String path = null;
          if (arr.length == 8) {
            path = arr[7];
          } else if (arr.length == 9 && (arr[0].equals(DistributedFastCp.OP_TYPE_ADD) ||
              arr[0].equals(DistributedFastCp.OP_TYPE_UPDATE))) {
            path = arr[8];
          }

          if (path != null && !compareChecksum(context, srcNamenode, dstNamenode, path)) {
            context.write(new Text(srcNamenode + ChecksumInputSplit.FIELD_SEPERATOR + dstNamenode +
                ChecksumInputSplit.FIELD_SEPERATOR + line), new Text("FAIL"));
          }
        } catch (IOException e) {
          log.error("failed compare checksum: " + srcNamenode + ", " + dstNamenode + ", " + line);
          context.write(new Text(srcNamenode + ChecksumInputSplit.FIELD_SEPERATOR + dstNamenode +
              ChecksumInputSplit.FIELD_SEPERATOR + line), new Text("FAIL"));
        }
      }

      line = reader.readLine();
    }
    reader.close();
  }

  private boolean compareChecksum(Context context, String srcNamenode, String dstNamenode,
                                  String path) throws IOException {
    PathData srcPath = new PathData(srcNamenode + path, context.getConfiguration());
    PathData dstPath = new PathData(dstNamenode + path, context.getConfiguration());
    if(srcPath.exists && dstPath.exists) {
      if (srcPath.stat.isDirectory() && dstPath.stat.isDirectory()) {
        log.info("src and dst is both dir: " + srcNamenode + ", " + dstNamenode + ", " + path);
        return true;
      } else if (srcPath.stat.isFile() && dstPath.stat.isFile()) {
        FileChecksum srcChecksum = srcPath.fs.getFileChecksum(srcPath.path);
        FileChecksum dstChecksum = dstPath.fs.getFileChecksum(dstPath.path);
        if (srcChecksum.equals(dstChecksum)) {
          log.info("src and dst checksum is same: " + srcNamenode + ", " + dstNamenode + ", " + path);
          return true;
        } else {
          log.error("src and dst checksum is different: " + srcNamenode + ", " + dstNamenode + ", " + path);
          return false;
        }
      } else {
        log.error("one of src and dst checksum is dir: " + srcNamenode + ", " + dstNamenode + ", " + path);
        return false;
      }
    } else {
      log.error("at least one of src and dst checksum is not exist: " + srcNamenode + ", " + dstNamenode + ", " + path);
      return false;
    }
  }
}
