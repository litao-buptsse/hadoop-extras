package com.sogou.hadoop.extras.mapreduce.lib.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * Created by Tao Li on 2016/11/28.
 */
public class CombineMultiTextInputFormat extends
    CombineMultiFileInputFormat<LongWritable, Text> {

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
                                                             TaskAttemptContext context) throws IOException {
    return new CombineFileRecordReader<LongWritable, Text>(
        (CombineFileSplit) split, context, CombineFileLineRecordReader.class);
  }

  /**
   * RecordReader is responsible from extracting records from a chunk
   * of the CombineFileSplit.
   */
  public static class CombineFileLineRecordReader
      extends RecordReader<LongWritable, Text> {
    private CompressionCodecFactory compressionCodecs = null;

    private long startOffset; //offset of the chunk;
    private long end; //end of the chunk;
    private long pos; // current pos
    private FileSystem fs;
    private Path path;
    private LongWritable key;
    private Text value;

    private FSDataInputStream fileIn;
    private LineReader reader;

    private boolean isCompressed = false;

    public CombineFileLineRecordReader(CombineFileSplit split,
                                       TaskAttemptContext context, Integer index) throws IOException {

      Configuration conf = context.getConfiguration();
      fs = FileSystem.get(conf);
      this.path = split.getPath(index);
      this.startOffset = split.getOffset(index);
      this.end = startOffset + split.getLength(index);

      if (compressionCodecs == null)
        compressionCodecs = new CompressionCodecFactory(conf);
      final CompressionCodec codec = compressionCodecs.getCodec(this.path);

      //open the file
      fileIn = fs.open(path);
      if (codec != null) {
        isCompressed = true;
        reader = new LineReader(codec.createInputStream(fileIn), conf);
        if (startOffset != 0) {
          fileIn.seek(startOffset);

          // read and ignore the first line
          reader.readLine(new Text());
          startOffset = fileIn.getPos();
        }
      } else {
        reader = new LineReader(fileIn);
        if (startOffset != 0) {// skip first line and re-establish "startOffset".
          --startOffset;
          fileIn.seek(startOffset);
          startOffset += reader.readLine(new Text(), 0,
              (int) Math.min((long) Integer.MAX_VALUE, end - startOffset));
        }
      }
      this.pos = startOffset;
    }

    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
    }

    public void close() throws IOException {
      if (reader != null) {
        reader.close();
        reader = null;
      }
    }

    public float getProgress() throws IOException {
      if (startOffset == end) {
        return 0.0f;
      } else {
        return Math.min(1.0f, (pos - startOffset) / (float) (end - startOffset));
      }
    }

    public boolean nextKeyValue() throws IOException {
      if (key == null) {
        key = new LongWritable();
      }
      key.set(pos);
      if (value == null) {
        value = new Text();
      }
      int newSize = 0;
      if (!isCompressed) {
        if (pos < end) {
          newSize = reader.readLine(value);
          pos += newSize;
        }
      } else {
        if (pos <= end) {
          newSize = reader.readLine(value);
          pos = fileIn.getPos();
        }
      }
      if (newSize == 0) {
        key = null;
        value = null;
        return false;
      } else {
        return true;
      }
    }

    public LongWritable getCurrentKey()
        throws IOException, InterruptedException {
      return key;
    }

    public Text getCurrentValue() throws IOException, InterruptedException {
      return value;
    }
  }

}
