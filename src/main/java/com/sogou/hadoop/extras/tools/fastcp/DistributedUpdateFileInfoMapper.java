package com.sogou.hadoop.extras.tools.fastcp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by Tao Li on 25/09/2016.
 */
public class DistributedUpdateFileInfoMapper extends Mapper<Text, Text, Text, Text> {
  private final Log log = LogFactory.getLog(DistributedUpdateFileInfoMapper.class);

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    UpdateFileInfoInputSplit split = (UpdateFileInfoInputSplit) context.getInputSplit();
    PathData updateListPath = new PathData(split.getUpdateListPath(), context.getConfiguration());
    String namenode = split.getNamenode();

    BufferedReader reader = new BufferedReader(new InputStreamReader(updateListPath.fs.open(updateListPath.path)));
    String line = reader.readLine();
    while (line != null) {
      String[] arr = line.split("\\s+");
      if (arr == null || (arr.length != 8 && arr.length != 9)) {
        log.error("invalid src file info: " + line);
      } else {
        try {
          if (arr.length == 8) {
            String permission = arr[0];
            String owner = arr[2];
            String group = arr[3];
            String path = arr[7];
            updateFileInfo(context, namenode, path, owner, group, permission);
          } else if (arr.length == 9) {
            String opType = arr[0];
            String permission = arr[1];
            String owner = arr[3];
            String group = arr[4];
            String path = arr[8];
            if (opType.equals(DistributedFastCp.OP_TYPE_ADD) ||
                opType.equals(DistributedFastCp.OP_TYPE_UPDATE)) {
              updateFileInfo(context, namenode, path, owner, group, permission);
            }
          }
        } catch (IOException e) {
          log.error("failed update file info: " + namenode + ", " + line);
          context.write(new Text(namenode + UpdateFileInfoInputSplit.FIELD_SEPERATOR + line), new Text("FAIL"));
        }
      }

      line = reader.readLine();
    }
    reader.close();
  }

  private void updateFileInfo(Context context, String namenode, String path,
                              String owner, String group, String permission) throws IOException {
    PathData pathData = new PathData(namenode + path, context.getConfiguration());
    pathData.fs.setOwner(pathData.path, owner, group);
    log.info("succeed chown: " + owner + ", " + group + ", " + pathData.path.toString());
    pathData.fs.setPermission(pathData.path, FsPermission.valueOf(permission));
    log.info("succeed chmod: " + permission + ", " + pathData.path.toString());
  }
}
