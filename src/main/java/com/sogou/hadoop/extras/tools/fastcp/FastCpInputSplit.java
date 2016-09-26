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
public class FastCpInputSplit extends InputSplit implements Writable {
  private final Log log = LogFactory.getLog(FastCpInputSplit.class);

  private String copyListPath;
  private String srcNamenode;
  private String dstNamenode;
  private String dstPath;

  public static String FIELD_SEPERATOR = "\001";

  public FastCpInputSplit() {
  }

  public FastCpInputSplit(String copyListPath,
                          String srcNamenode, String dstNamenode, String dstPath) {
    this.copyListPath = copyListPath;
    this.srcNamenode = srcNamenode;
    this.dstNamenode = dstNamenode;
    this.dstPath = dstPath;
  }

  public String getCopyListPath() {
    return copyListPath;
  }

  public String getSrcNamenode() {
    return srcNamenode;
  }

  public String getDstNamenode() {
    return dstNamenode;
  }

  public String getDstPath() {
    return dstPath;
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
    Text.writeString(out, copyListPath + FIELD_SEPERATOR
        + srcNamenode + FIELD_SEPERATOR + dstNamenode + FIELD_SEPERATOR + dstPath);
    log.info("split write: " + copyListPath + ", "
        + srcNamenode + ", " + dstNamenode + ", " + dstPath);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    String data = Text.readString(in);
    String[] arr = data.split(FIELD_SEPERATOR);
    if (arr == null || arr.length != 4) {
      throw new IOException("invalid split data: " + data);
    }
    copyListPath = arr[0];
    srcNamenode = arr[1];
    dstNamenode = arr[2];
    dstPath = arr[3];
    log.info("split read: " + copyListPath + ", "
        + srcNamenode + ", " + dstNamenode + ", " + dstPath);
  }
}