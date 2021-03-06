package com.sogou.hadoop.extras.tools.hdfs.migrate;

import com.sogou.hadoop.extras.common.ThrottledInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FastCopy;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tao Li on 25/09/2016.
 */
public class HdfsMigrateMapper extends Mapper<Text, Text, Text, Text> {
  private final Log log = LogFactory.getLog(HdfsMigrateMapper.class);

  private final static String OP_TYPE_ADD = "ADD";
  private final static String OP_TYPE_DELETE = "DELETE";
  private final static String OP_TYPE_UPDATE = "UPDATE";

  private MapperTask createMapperTask(Context context, String jobType) throws IOException {
    switch (jobType) {
      case DistributedHdfsMigrate.JOB_TYPE_FASTCOPY:
        return new FastCopyTask(context);
      case DistributedHdfsMigrate.JOB_TYPE_CHECKSUM:
        return new ChecksumTask(context);
      case DistributedHdfsMigrate.JOB_TYPE_DELETE:
        return new DeleteTask(context);
      case DistributedHdfsMigrate.JOB_TYPE_DISTCP:
        return new DistcpTask(context);
      default:
        throw new IOException("no such jobType: " + jobType);
    }
  }

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    HdfsMigrateInputSplit split = (HdfsMigrateInputSplit) context.getInputSplit();
    PathData copyListPath = new PathData(split.getCopyListPath(), context.getConfiguration());
    String srcNamenode = split.getSrcNamenode();
    String dstNamenode = split.getDstNamenode();
    String dstPath = split.getDstPath();
    String jobType = split.getJobType();

    MapperTask task = null;
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        copyListPath.fs.open(copyListPath.path)))) {
      task = createMapperTask(context, jobType);

      String line = reader.readLine();
      while (line != null) {
        context.getCounter(HdfsMigrateCounter.TOTAL).increment(1);

        String[] arr = line.split("\\s+");
        String permission = null;
        String owner = null;
        String group = null;
        String srcPath = null;
        String opType = null;
        if (arr != null && arr.length == 8) {
          opType = OP_TYPE_ADD;
          permission = arr[0];
          owner = arr[2];
          group = arr[3];
          srcPath = arr[7];
        } else if (arr != null && arr.length == 9) {
          opType = arr[0].toUpperCase();
          permission = arr[1];
          owner = arr[3];
          group = arr[4];
          srcPath = arr[8];
        } else {
          log.error("invalid src file info: " + split.toString() + ", " + line);
        }

        if (opType != null) {
          try {
            task.run(srcNamenode, dstNamenode, dstPath,
                opType, permission, owner, group, srcPath);
            context.getCounter(HdfsMigrateCounter.SUCC).increment(1);
          } catch (Exception e) {
            log.error("fail to run task: " + split.toString() + ", " + line, e);
            context.write(new Text(split.toString()), new Text(line));
            context.getCounter(HdfsMigrateCounter.FAIL).increment(1);
          }
        }

        line = reader.readLine();
      }
    } finally {
      if (task != null) {
        task.kill();
      }
    }
  }

  interface MapperTask {
    void run(String srcNamenode, String dstNamenode, String dstPath,
             String opType, String permission, String owner, String group,
             String srcPath) throws IOException;

    void kill() throws IOException;
  }

  class FastCopyTask implements MapperTask {
    private Context context;
    private FastCopy fastCopy;
    List<FastCopy.FastFileCopyRequest> requests = new ArrayList<>();

    public FastCopyTask(Context context) throws IOException {
      this.context = context;
      try {
        Class<? extends FastCopy> clazz = (Class<? extends FastCopy>) context.getConfiguration().
            getClassByName("org.apache.hadoop.hdfs.FastCopyImpl");
        Class[] cArgs = new Class[3];
        cArgs[0] = Configuration.class;
        cArgs[1] = int.class;
        cArgs[2] = boolean.class;
        fastCopy = (FastCopy) clazz.getDeclaredConstructor(cArgs).
            newInstance(context.getConfiguration(), 1, false);
      } catch (Exception e) {
        throw new IOException("fail to create fastcopy instance", e);
      }
    }

    @Override
    public void run(String srcNamenode, String dstNamenode, String dstPath,
                    String opType, String permission, String owner, String group,
                    String srcPath) throws IOException {
      if (srcPath.contains("/.Trash/") ||
          srcPath.contains("/_temporary/") ||
          srcPath.contains("/_distcp_logs_")) {
        log.info("skip dir: " + opType + ", " +
            srcNamenode + ", " + dstNamenode + ", " + srcPath + ", " + dstPath);
        return;
      }

      switch (opType) {
        case OP_TYPE_ADD:
          create(srcNamenode, srcPath, dstNamenode, dstPath, permission, owner, group,
              fastCopy, requests);
          context.getCounter(HdfsMigrateCounter.ADD).increment(1);
          break;
        case OP_TYPE_DELETE:
          delete(srcPath, dstNamenode, dstPath);
          context.getCounter(HdfsMigrateCounter.DELETE).increment(1);
          break;
        case OP_TYPE_UPDATE:
          update(srcNamenode, srcPath, dstNamenode, dstPath, permission, owner, group,
              fastCopy, requests);
          context.getCounter(HdfsMigrateCounter.UPDATE).increment(1);
          break;
        default:
          throw new IOException("no such opType: " + opType);
      }
    }

    @Override
    public void kill() throws IOException {
      if (fastCopy != null) {
        fastCopy.shutdown();
      }
    }

    private void create(String srcNamenode, String srcPath,
                        String dstNamenode, String dstPath,
                        String permission, String owner, String group,
                        FastCopy fastCopy,
                        List<FastCopy.FastFileCopyRequest> requests) throws IOException {
      boolean isFile = permission.startsWith("-");
      PathData realSrcPath = new PathData(srcNamenode + srcPath,
          context.getConfiguration());
      PathData realDstPath = new PathData(dstNamenode + dstPath + srcPath,
          context.getConfiguration());

      if (isFile) {
        // fastcp
        requests.clear();
        requests.add(new FastCopy.FastFileCopyRequest(realSrcPath.path, realDstPath.path,
            realSrcPath.fs, realDstPath.fs));
        try {
          fastCopy.copy(requests);
          context.getCounter(HdfsMigrateCounter.FASTCOPY).increment(1);
          log.info("succeed fastcp: " + realSrcPath.path.toString() + ", " +
              realDstPath.path.toString());
        } catch (Exception e) {
          throw new IOException(e);
        }
      } else {
        // mkdir
        if (!realDstPath.exists) {
          realDstPath.fs.mkdirs(realDstPath.path);
          log.info("succeed mkdir: " + realSrcPath.path.toString() + ", " +
              realDstPath.path.toString());
        }
        context.getCounter(HdfsMigrateCounter.MKDIR).increment(1);
      }

      // chown
      realDstPath.fs.setOwner(realDstPath.path, owner, group);
      log.info("succeed chown: " + owner + ", " + group + ", " + realDstPath.path.toString());

      // chmod
      realDstPath.fs.setPermission(realDstPath.path, FsPermission.valueOf(permission));
      log.info("succeed chmod: " + permission + ", " + realDstPath.path.toString());

      // set times
      realDstPath.fs.setTimes(realDstPath.path,
          realSrcPath.stat.getModificationTime(), realSrcPath.stat.getAccessTime());
      log.info("succeed set times: " + realDstPath.path.toString() + ", " +
          realSrcPath.stat.getModificationTime() + ", " + realSrcPath.stat.getAccessTime());
    }

    private void delete(String srcPath,
                        String dstNamenode, String dstPath) throws IOException {
      PathData realDstPath = new PathData(dstNamenode + dstPath + srcPath,
          context.getConfiguration());
      if (realDstPath.exists) {
        realDstPath.fs.delete(realDstPath.path, true);
        log.info("succeed delete: " + realDstPath.path.toString());
      }
    }

    private void update(String srcNamenode, String srcPath,
                        String dstNamenode, String dstPath,
                        String permission, String owner, String group,
                        FastCopy fastCopy,
                        List<FastCopy.FastFileCopyRequest> requests) throws IOException {
      PathData realDstPath = new PathData(dstNamenode + dstPath + srcPath,
          context.getConfiguration());
      if (realDstPath.exists && realDstPath.stat.isFile()) {
        delete(srcPath, dstNamenode, dstPath);
      }
      create(srcNamenode, srcPath, dstNamenode, dstPath,
          permission, owner, group, fastCopy, requests);
    }
  }

  class ChecksumTask implements MapperTask {
    private Context context;

    public ChecksumTask(Context context) {
      this.context = context;
    }


    @Override
    public void run(String srcNamenode, String dstNamenode, String dstPath, String opType,
                    String permission, String owner, String group,
                    String srcPath) throws IOException {
      if (srcPath.contains("/.Trash/")) {
        log.info("skip trash dir: " + opType + ", " +
            srcNamenode + ", " + dstNamenode + ", " + srcPath + ", " + dstPath);
        return;
      }

      if (opType.equals(OP_TYPE_ADD) || opType.equals(OP_TYPE_UPDATE)) {
        if (!compareChecksum(srcNamenode, dstNamenode, srcPath, dstPath)) {
          throw new IOException("fail to compare checksum");
        }
      }
    }

    @Override
    public void kill() throws IOException {

    }

    private boolean compareChecksum(String srcNamenode, String srcPath,
                                    String dstNamenode, String dstPath) throws IOException {
      PathData realSrcPath = new PathData(srcNamenode + srcPath,
          context.getConfiguration());
      PathData realDstPath = new PathData(dstNamenode + dstPath + srcPath,
          context.getConfiguration());

      if (realSrcPath.exists && realDstPath.exists) {
        if (realSrcPath.stat.isDirectory() && realDstPath.stat.isDirectory()) {
          log.info("src and dst is both dir: " +
              realSrcPath.path.toString() + ", " + realDstPath.path.toString());
          return true;
        } else if (realSrcPath.stat.isFile() && realDstPath.stat.isFile()) {
          FileChecksum srcChecksum = realSrcPath.fs.getFileChecksum(realSrcPath.path);
          FileChecksum dstChecksum = realDstPath.fs.getFileChecksum(realDstPath.path);
          if (srcChecksum.equals(dstChecksum)) {
            log.info("src and dst checksum is same: " +
                realSrcPath.path.toString() + ", " + realDstPath.path.toString());
            return true;
          } else {
            log.error("src and dst checksum is different: " +
                realSrcPath.path.toString() + ", " + realDstPath.path.toString());
            return false;
          }
        } else {
          log.error("one of src and dst checksum is dir: " +
              realSrcPath.path.toString() + ", " + realDstPath.path.toString());
          return false;
        }
      } else {
        log.error("at least one of src and dst checksum is not exist: " +
            realSrcPath.path.toString() + ", " + realDstPath.path.toString());
        return false;
      }
    }
  }

  class DeleteTask implements MapperTask {
    private Context context;
    private boolean isRecursive;

    public DeleteTask(Context context) {
      this.context = context;
      this.isRecursive = context.getConfiguration().
          getBoolean("com.sogou.hadoop.extras.hdfs.fastcp.is-delete-recursive", false);
    }

    @Override
    public void run(String srcNamenode, String dstNamenode, String dstPath, String opType,
                    String permission, String owner, String group,
                    String srcPath) throws IOException {
      // skip delete dir /user/xxx and /user/xxx/.Trash
      if (srcPath.startsWith("/user") &&
          (srcPath.split("/").length == 3 ||
              srcPath.split("/").length == 4 && srcPath.endsWith(".Trash"))) {
        return;
      }

      PathData realSrcPath = new PathData(srcNamenode + srcPath,
          context.getConfiguration());

      if (realSrcPath.exists) {
        if (realSrcPath.stat.isFile() || realSrcPath.stat.isDirectory() && isRecursive) {
          realSrcPath.fs.delete(realSrcPath.path, true);
          context.getCounter(HdfsMigrateCounter.DELETE).increment(1);
          log.info("succeed delete: " + realSrcPath.path.toString());
        }
      }
    }

    @Override
    public void kill() throws IOException {

    }
  }

  class DistcpTask implements MapperTask {
    private final Context context;
    private final static int BUFFER_SIZE = 256 * 1024;
    private final byte[] buffer = new byte[BUFFER_SIZE];
    private final long maxBytesPerSec;

    public DistcpTask(Context context) {
      this.context = context;
      maxBytesPerSec =
          context.getConfiguration().getInt("distcp.map.bandwidth.mb", 0) * 1024 * 1024;
    }

    @Override
    public void run(String srcNamenode, String dstNamenode, String dstPath,
                    String opType, String permission, String owner, String group,
                    String srcPath) throws IOException {
      if (srcPath.contains("/.Trash/") ||
          srcPath.contains("/_temporary/") ||
          srcPath.contains("/_distcp_logs_")) {
        log.info("skip dir: " + opType + ", " +
            srcNamenode + ", " + dstNamenode + ", " + srcPath + ", " + dstPath);
        return;
      }

      switch (opType) {
        case OP_TYPE_ADD:
          create(srcNamenode, srcPath, dstNamenode, dstPath, permission, owner, group);
          context.getCounter(HdfsMigrateCounter.ADD).increment(1);
          break;
        case OP_TYPE_DELETE:
          delete(srcPath, dstNamenode, dstPath);
          context.getCounter(HdfsMigrateCounter.DELETE).increment(1);
          break;
        case OP_TYPE_UPDATE:
          update(srcNamenode, srcPath, dstNamenode, dstPath, permission, owner, group);
          context.getCounter(HdfsMigrateCounter.UPDATE).increment(1);
          break;
        default:
          throw new IOException("no such opType: " + opType);
      }
    }

    @Override
    public void kill() throws IOException {

    }

    private void create(String srcNamenode, String srcPath,
                        String dstNamenode, String dstPath,
                        String permission, String owner, String group) throws IOException {
      boolean isFile = permission.startsWith("-");
      PathData realSrcPath = new PathData(srcNamenode + srcPath,
          context.getConfiguration());
      PathData realDstPath = new PathData(dstNamenode + dstPath + srcPath,
          context.getConfiguration());

      if (isFile) {
        distcp(realSrcPath, realDstPath);
        context.getCounter(HdfsMigrateCounter.DISTCP).increment(1);
        log.info("succeed distcp: " + realSrcPath.path.toString() + ", " +
            realDstPath.path.toString());
      } else {
        // mkdir
        if (!realDstPath.exists) {
          realDstPath.fs.mkdirs(realDstPath.path);
          log.info("succeed mkdir: " + realSrcPath.path.toString() + ", " +
              realDstPath.path.toString());
        }
        context.getCounter(HdfsMigrateCounter.MKDIR).increment(1);
      }

      // chown
      realDstPath.fs.setOwner(realDstPath.path, owner, group);
      log.info("succeed chown: " + owner + ", " + group + ", " + realDstPath.path.toString());

      // chmod
      realDstPath.fs.setPermission(realDstPath.path, FsPermission.valueOf(permission));
      log.info("succeed chmod: " + permission + ", " + realDstPath.path.toString());

      // set times
      realDstPath.fs.setTimes(realDstPath.path,
          realSrcPath.stat.getModificationTime(), realSrcPath.stat.getAccessTime());
      log.info("succeed set times: " + realDstPath.path.toString() + ", " +
          realSrcPath.stat.getModificationTime() + ", " + realSrcPath.stat.getAccessTime());
    }

    private void distcp(PathData srcPath, PathData dstPath) throws IOException {
      try (
          InputStream rawInput = srcPath.fs.open(srcPath.path, BUFFER_SIZE);
          FSDataOutputStream output = dstPath.fs.create(
              dstPath.path, true, BUFFER_SIZE,
              srcPath.stat.getReplication(), srcPath.stat.getBlockSize())) {
        ThrottledInputStream input = maxBytesPerSec == 0 ?
            new ThrottledInputStream(rawInput) :
            new ThrottledInputStream(rawInput, maxBytesPerSec);
        int bytesRead = 0;
        long totalBytesRead = 0;
        long length = srcPath.stat.getLen();
        while (bytesRead >= 0 && totalBytesRead < length) {
          int toRead = (int) Math.min(buffer.length, length - bytesRead);
          bytesRead = input.read(totalBytesRead, buffer, 0, toRead);
          if (bytesRead < 0) {
            break;
          }
          output.write(buffer, 0, bytesRead);
          totalBytesRead += bytesRead;
        }
      }
    }

    private void delete(String srcPath,
                        String dstNamenode, String dstPath) throws IOException {
      PathData realDstPath = new PathData(dstNamenode + dstPath + srcPath,
          context.getConfiguration());
      if (realDstPath.exists) {
        realDstPath.fs.delete(realDstPath.path, true);
        log.info("succeed delete: " + realDstPath.path.toString());
      }
    }

    private void update(String srcNamenode, String srcPath,
                        String dstNamenode, String dstPath,
                        String permission, String owner, String group) throws IOException {
      PathData realDstPath = new PathData(dstNamenode + dstPath + srcPath,
          context.getConfiguration());
      if (realDstPath.exists && realDstPath.stat.isFile()) {
        delete(srcPath, dstNamenode, dstPath);
      }
      create(srcNamenode, srcPath, dstNamenode, dstPath, permission, owner, group);
    }
  }
}
