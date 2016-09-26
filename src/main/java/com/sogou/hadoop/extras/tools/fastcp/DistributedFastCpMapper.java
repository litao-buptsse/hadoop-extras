package com.sogou.hadoop.extras.tools.fastcp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FastCopyUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * Created by Tao Li on 25/09/2016.
 */
public class DistributedFastCpMapper extends Mapper<Text, Text, Text, Text> {
  private final Log log = LogFactory.getLog(DistributedFastCpMapper.class);

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    FastCpInputSplit inputSplit = (FastCpInputSplit) context.getInputSplit();
    Path path = new Path(inputSplit.getSrcPath());
    Path dstDir = new Path(inputSplit.getDstPath());
    log.info("I am going to fastcp: " + path + " to " + dstDir);
    try {
      List<String> failedSrcs = FastCopyUtil.fastCopy(context.getConfiguration(),
          new String[]{path.toString()}, dstDir, path.getFileSystem(context.getConfiguration()), dstDir.getFileSystem(context.getConfiguration()), 10);
      context.write(new Text(path.toString() + " to " + dstDir.toString()), new Text("OK"));
    } catch (Exception e) {
      log.error("occur exception", e);
      context.write(new Text(path.toString() + " to " + dstDir.toString()), new Text("FAIL"));
    }
  }
}
