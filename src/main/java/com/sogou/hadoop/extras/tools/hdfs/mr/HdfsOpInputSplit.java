package com.sogou.hadoop.extras.tools.hdfs.mr;

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
public class HdfsOpInputSplit extends InputSplit implements Writable {
  private final Log log = LogFactory.getLog(HdfsOpInputSplit.class);

  private String copyListPath;
  private String srcNamenode;
  private String dstNamenode;
  private String dstPath;
  private String jobType;

  public static String FIELD_SEPERATOR = "\001";

  public HdfsOpInputSplit() {
  }

  public HdfsOpInputSplit(String copyListPath, String srcNamenode, String dstNamenode,
                          String dstPath, String jobType) {
    this.copyListPath = copyListPath;
    this.srcNamenode = srcNamenode;
    this.dstNamenode = dstNamenode;
    this.dstPath = dstPath;
    this.jobType = jobType;
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

  public String getJobType() {
    return jobType;
  }

  @Override
  public String toString() {
    return "HdfsOpInputSplit{" +
        "dstPath='" + dstPath + '\'' +
        ", jobType='" + jobType + '\'' +
        ", dstNamenode='" + dstNamenode + '\'' +
        ", srcNamenode='" + srcNamenode + '\'' +
        ", copyListPath='" + copyListPath + '\'' +
        '}';
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
    String meta = copyListPath + FIELD_SEPERATOR + srcNamenode + FIELD_SEPERATOR +
        dstNamenode + FIELD_SEPERATOR + dstPath + FIELD_SEPERATOR + jobType;
    Text.writeString(out, meta);
    log.info("split write: " + this.toString());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    String meta = Text.readString(in);
    String[] arr = meta.split(FIELD_SEPERATOR);
    if (arr == null || arr.length != 5) {
      throw new IOException("invalid split data: " + meta);
    }
    copyListPath = arr[0];
    srcNamenode = arr[1];
    dstNamenode = arr[2];
    dstPath = arr[3];
    jobType = arr[4];
    log.info("split read: " + this.toString());
  }
}