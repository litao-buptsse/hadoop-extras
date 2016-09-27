package com.sogou.hadoop.extras.tools.fastcp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Tao Li on 25/09/2016.
 */
public class UpdateFileInfoInputSplit extends InputSplit implements Writable {
  private final Log log = LogFactory.getLog(UpdateFileInfoInputSplit.class);

  private String updateListPath;
  private String namenode;

  public static String FIELD_SEPERATOR = "\001";

  public UpdateFileInfoInputSplit() {
  }

  public UpdateFileInfoInputSplit(String updateListPath, String namenode) {
    this.updateListPath = updateListPath;
    this.namenode = namenode;
  }

  public String getUpdateListPath() {
    return updateListPath;
  }

  public String getNamenode() {
    return namenode;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[0];
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, updateListPath + FIELD_SEPERATOR + namenode);
    log.info("split write: " + updateListPath + ", " + namenode);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    String data = Text.readString(in);
    String[] arr = data.split(FIELD_SEPERATOR);
    if (arr == null || arr.length != 2) {
      throw new IOException("invalid split data: " + data);
    }
    updateListPath = arr[0];
    namenode = arr[1];
    log.info("split read: " + updateListPath + ", " + namenode);
  }
}