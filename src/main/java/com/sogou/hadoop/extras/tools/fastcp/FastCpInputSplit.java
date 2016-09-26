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

  private String srcPath;
  private String dstPath;
  private static String FIELD_SEPERATOR = "\001";

  public FastCpInputSplit() {
  }

  public FastCpInputSplit(String srcPath, String dstPath) {
    this.srcPath = srcPath;
    this.dstPath = dstPath;
  }

  public String getSrcPath() {
    return srcPath;
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
    Text.writeString(out, srcPath + FIELD_SEPERATOR + dstPath);
    log.info("split write: " + srcPath + ", " + dstPath);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    String data = Text.readString(in);
    String[] arr = data.split(FIELD_SEPERATOR);
    if (arr == null || arr.length != 2) {
      throw new IOException("invalid split data: " + data);
    }
    srcPath = arr[0];
    dstPath = arr[1];
    log.info("split read: " + srcPath + ", " + dstPath);
  }
}