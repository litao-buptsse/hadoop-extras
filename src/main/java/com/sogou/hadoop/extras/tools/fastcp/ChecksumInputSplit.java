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
public class ChecksumInputSplit extends InputSplit implements Writable {
  private final Log log = LogFactory.getLog(ChecksumInputSplit.class);

  private String checksumListPath;
  private String srcNamenode;
  private String dstNamenode;

  public static String FIELD_SEPERATOR = "\001";

  public ChecksumInputSplit() {
  }

  public ChecksumInputSplit(String checksumListPath, String srcNamenode, String dstNamenode) {
    this.checksumListPath = checksumListPath;
    this.srcNamenode = srcNamenode;
    this.dstNamenode = dstNamenode;
  }

  public String getChecksumListPath() {
    return checksumListPath;
  }

  public String getSrcNamenode() {
    return srcNamenode;
  }

  public String getDstNamenode() {
    return dstNamenode;
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
    Text.writeString(out, checksumListPath + FIELD_SEPERATOR +
        srcNamenode + FIELD_SEPERATOR + dstNamenode);
    log.info("split write: " + checksumListPath + ", " + srcNamenode + ", " + dstNamenode);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    String data = Text.readString(in);
    String[] arr = data.split(FIELD_SEPERATOR);
    if (arr == null || arr.length != 3) {
      throw new IOException("invalid split data: " + data);
    }
    checksumListPath = arr[0];
    srcNamenode = arr[1];
    dstNamenode = arr[2];
    log.info("split read: " + checksumListPath + ", " + srcNamenode + ", " + dstNamenode);
  }
}