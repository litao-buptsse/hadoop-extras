package com.sogou.hadoop.extras.mapreduce.lib.input;

import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzopCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NodeBase;

import java.io.IOException;
import java.util.*;

/**
 * Created by Tao Li on 2016/11/28.
 */
public abstract class CombineMultiFileInputFormat<K, V> extends FileInputFormat<K, V> {

  // default MultiPathFilter list
  private static final PathFilter lzoPathFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      if (path.getName().endsWith(".lzo"))
        return true;
      return false;
    }
  };
  private static final PathFilter lzoIndexPathFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      if (path.getName().endsWith(".lzo.index"))
        return true;
      return false;
    }
  };
  private static final PathFilter gzPathFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      if (path.getName().endsWith(".gz"))
        return true;
      return false;
    }
  };


  // ability to limit the size of a single split
  private long maxSplitSize = 0;
  private long minSplitSizeNode = 0;
  private long minSplitSizeRack = 0;

  // A pool of input paths filters. A split cannot have blocks from files
  // across multiple pools.
  private List<MultiPathFilter> pools = new ArrayList<MultiPathFilter>();

  // mapping from a rack name to the set of Nodes in the rack
  private HashMap<String, Set<String>> rackToNodes = new HashMap<String, Set<String>>();

  /**
   * Specify the maximum size (in bytes) of each split. Each split is
   * approximately equal to the specified size.
   */
  protected void setMaxSplitSize(long maxSplitSize) {
    this.maxSplitSize = maxSplitSize;
  }

  /**
   * Specify the minimum size (in bytes) of each split per node.
   * This applies to data that is left over after combining data on a single
   * node into splits that are of maximum size specified by maxSplitSize.
   * This leftover data will be combined into its own split if its size
   * exceeds minSplitSizeNode.
   */
  protected void setMinSplitSizeNode(long minSplitSizeNode) {
    this.minSplitSizeNode = minSplitSizeNode;
  }

  /**
   * Specify the minimum size (in bytes) of each split per rack.
   * This applies to data that is left over after combining data on a single
   * rack into splits that are of maximum size specified by maxSplitSize.
   * This leftover data will be combined into its own split if its size
   * exceeds minSplitSizeRack.
   */
  protected void setMinSplitSizeRack(long minSplitSizeRack) {
    this.minSplitSizeRack = minSplitSizeRack;
  }

  /**
   * Create a new pool and add the filters to it.
   * A split cannot have files from different pools.
   */
  protected void createPool(List<PathFilter> filters) {
    createPool(FileType.SplitablePlainText, filters);
  }

  protected void createPool(FileType fileType, List<PathFilter> filters) {
    pools.add(new MultiPathFilter(filters, fileType));
  }

  /**
   * Create a new pool and add the filters to it.
   * A pathname can satisfy any one of the specified filters.
   * A split cannot have files from different pools.
   */
  protected void createPool(PathFilter... filters) {
    createPool(FileType.SplitablePlainText, filters);
  }

  protected void createPool(FileType fileType, PathFilter... filters) {
    MultiPathFilter multi = new MultiPathFilter();
    for (PathFilter f : filters) {
      multi.add(f);
    }
    multi.setFileType(fileType);
    pools.add(multi);
  }

  /**
   * default constructor
   */
  public CombineMultiFileInputFormat() {
    createPool(FileType.SplitableCompressed, lzoPathFilter, lzoIndexPathFilter);
    createPool(FileType.UnsplitableCompressed, gzPathFilter);
  }

  @Override
  public List<InputSplit> getSplits(JobContext job)
      throws IOException {

    long minSizeNode = 0;
    long minSizeRack = 0;
    long maxSize = 0;
    Configuration conf = job.getConfiguration();

    // the values specified by setxxxSplitSize() takes precedence over the
    // values that might have been specified in the config
    if (minSplitSizeNode != 0) {
      minSizeNode = minSplitSizeNode;
    } else {
      minSizeNode = conf.getLong("mapreduce.input.fileinputformat.split.minsize.per.node", 0);
    }
    if (minSplitSizeRack != 0) {
      minSizeRack = minSplitSizeRack;
    } else {
      minSizeRack = conf.getLong("mapreduce.input.fileinputformat.split.minsize.per.rack", 0);
    }
    if (maxSplitSize != 0) {
      maxSize = maxSplitSize;
    } else {
      maxSize = conf.getLong("mapreduce.input.fileinputformat.split.maxsize", 512 * 1024 * 1024l);
    }
    if (minSizeNode != 0 && maxSize != 0 && minSizeNode > maxSize) {
      throw new IOException("Minimum split size pernode " + minSizeNode +
          " cannot be larger than maximum split size " +
          maxSize);
    }
    if (minSizeRack != 0 && maxSize != 0 && minSizeRack > maxSize) {
      throw new IOException("Minimum split size per rack" + minSizeRack +
          " cannot be larger than maximum split size " +
          maxSize);
    }
    if (minSizeRack != 0 && minSizeNode > minSizeRack) {
      throw new IOException("Minimum split size per node" + minSizeNode +
          " cannot be smaller than minimum split " +
          "size per rack " + minSizeRack);
    }

    // all the files in input set
    Path[] paths = FileUtil.stat2Paths(
        listStatus(job).toArray(new FileStatus[0]));
    List<InputSplit> splits = new ArrayList<InputSplit>();
    if (paths.length == 0) {
      return splits;
    }

    // Convert them to Paths first. This is a costly operation and
    // we should do it first, otherwise we will incur doing it multiple
    // times, one time each for each pool in the next loop.
    List<Path> newpaths = new LinkedList<Path>();
    for (int i = 0; i < paths.length; i++) {
      Path p = new Path(paths[i].toUri().getPath());
      newpaths.add(p);
    }
    paths = null;

    // In one single iteration, process all the paths in a single pool.
    // Processing one pool at a time ensures that a split contains paths
    // from a single pool only.
    for (MultiPathFilter onepool : pools) {
      ArrayList<Path> myPaths = new ArrayList<Path>();

      // pick one input path. If it matches all the filters in a pool,
      // add it to the output set
      for (Iterator<Path> iter = newpaths.iterator(); iter.hasNext(); ) {
        Path p = iter.next();
        if (onepool.accept(p)) {
          myPaths.add(p); // add it to my output set
          iter.remove();
        }
      }
      // create splits for all files in this pool.
      getMoreSplits(conf, myPaths.toArray(new Path[myPaths.size()]),
          maxSize, minSizeNode, minSizeRack, splits, onepool.getFileType());
    }

    // create splits for all files that are not in any pool.
    getMoreSplits(conf, newpaths.toArray(new Path[newpaths.size()]),
        maxSize, minSizeNode, minSizeRack, splits, FileType.SplitablePlainText);

    // free up rackToNodes map
    rackToNodes.clear();
    return splits;
  }

  /**
   * Return all the splits in the specified set of paths
   */
  private void getMoreSplits(Configuration conf, Path[] paths,
                             long maxSize, long minSizeNode, long minSizeRack,
                             List<InputSplit> splits, FileType filetype)
      throws IOException {
    switch (filetype) {
      case SplitableCompressed:
        getMoreSplitsForSplitableCompressed(conf, paths, maxSize, minSizeNode,
            minSizeRack, splits);
        break;
      case UnsplitableCompressed:
        getMoreSplitsForUnsplitableCompressed(conf, paths, maxSize, minSizeNode,
            minSizeRack, splits);
        break;
      case SplitablePlainText:
      default:
        getMoreSplitsForPlainText(conf, paths, maxSize, minSizeNode, minSizeRack,
            splits);
    }
  }

  /**
   * Return all the splits in the specified set of paths
   */
  private void getMoreSplitsForUnsplitableCompressed(Configuration conf, Path[] paths,
                                                     long maxSize, long minSizeNode, long minSizeRack,
                                                     List<InputSplit> splits)
      throws IOException {
    getMoreSplits(conf, paths, maxSize, minSizeNode, minSizeRack, splits, true, null);
  }

  /**
   * Return all the splits in the specified set of paths
   */
  private void getMoreSplitsForPlainText(Configuration conf, Path[] paths,
                                         long maxSize, long minSizeNode, long minSizeRack,
                                         List<InputSplit> splits)
      throws IOException {
    getMoreSplits(conf, paths, maxSize, minSizeNode, minSizeRack, splits, false, null);
  }

  /**
   * Return all the splits in the specified set of paths
   */
  private void getMoreSplits(Configuration conf, Path[] paths,
                             long maxSize, long minSizeNode, long minSizeRack,
                             List<InputSplit> splits, boolean treatOneFileAsOneBlock, Map<Path, LzoIndex> indexes)
      throws IOException {

    // all blocks for all the files in input set
    OneFileInfo[] files;

    // mapping from a rack name to the list of blocks it has
    HashMap<String, List<OneBlockInfo>> rackToBlocks =
        new HashMap<String, List<OneBlockInfo>>();

    // mapping from a block to the nodes on which it has replicas
    HashMap<OneBlockInfo, String[]> blockToNodes =
        new HashMap<OneBlockInfo, String[]>();

    // mapping from a node to the list of blocks that it contains
    HashMap<String, List<OneBlockInfo>> nodeToBlocks =
        new HashMap<String, List<OneBlockInfo>>();

    files = new OneFileInfo[paths.length];
    if (paths.length == 0) {
      return;
    }

    // populate all the blocks for all files
    long totLength = 0;
    for (int i = 0; i < paths.length; i++) {
      files[i] = new OneFileInfo(paths[i], conf,
          rackToBlocks, blockToNodes, nodeToBlocks, rackToNodes, treatOneFileAsOneBlock, indexes);
      totLength += files[i].getLength();
    }

    ArrayList<OneBlockInfo> validBlocks = new ArrayList<OneBlockInfo>();
    ArrayList<String> nodes = new ArrayList<String>();
    long curSplitSize = 0;

    // process all nodes and create splits that are local
    // to a node.
    for (Iterator<Map.Entry<String,
        List<OneBlockInfo>>> iter = nodeToBlocks.entrySet().iterator();
         iter.hasNext(); ) {

      Map.Entry<String, List<OneBlockInfo>> one = iter.next();
      nodes.add(one.getKey());
      List<OneBlockInfo> blocksInNode = one.getValue();

      // for each block, copy it into validBlocks. Delete it from
      // blockToNodes so that the same block does not appear in
      // two different splits.
      for (OneBlockInfo oneblock : blocksInNode) {
        if (blockToNodes.containsKey(oneblock)) {
          validBlocks.add(oneblock);
          blockToNodes.remove(oneblock);
          curSplitSize += oneblock.length;

          // if the accumulated split size exceeds the maximum, then
          // create this split.
          if (maxSize != 0 && curSplitSize >= maxSize) {
            // create an input split and add it to the splits array
            addCreatedSplit(splits, nodes, validBlocks);
            curSplitSize = 0;
            validBlocks.clear();
          }
        }
      }
      // if there were any blocks left over and their combined size is
      // larger than minSplitNode, then combine them into one split.
      // Otherwise add them back to the unprocessed pool. It is likely
      // that they will be combined with other blocks from the
      // same rack later on.
      if (minSizeNode != 0 && curSplitSize >= minSizeNode) {
        // create an input split and add it to the splits array
        addCreatedSplit(splits, nodes, validBlocks);
      } else {
        for (OneBlockInfo oneblock : validBlocks) {
          blockToNodes.put(oneblock, oneblock.hosts);
        }
      }
      validBlocks.clear();
      nodes.clear();
      curSplitSize = 0;
    }

    // if blocks in a rack are below the specified minimum size, then keep them
    // in 'overflow'. After the processing of all racks is complete, these
    // overflow blocks will be combined into splits.
    ArrayList<OneBlockInfo> overflowBlocks = new ArrayList<OneBlockInfo>();
    ArrayList<String> racks = new ArrayList<String>();

    // Process all racks over and over again until there is no more work to do.
    while (blockToNodes.size() > 0) {

      // Create one split for this rack before moving over to the next rack.
      // Come back to this rack after creating a single split for each of the
      // remaining racks.
      // Process one rack location at a time, Combine all possible blocks that
      // reside on this rack as one split. (constrained by minimum and maximum
      // split size).

      // iterate over all racks
      for (Iterator<Map.Entry<String, List<OneBlockInfo>>> iter =
           rackToBlocks.entrySet().iterator(); iter.hasNext(); ) {

        Map.Entry<String, List<OneBlockInfo>> one = iter.next();
        racks.add(one.getKey());
        List<OneBlockInfo> blocks = one.getValue();

        // for each block, copy it into validBlocks. Delete it from
        // blockToNodes so that the same block does not appear in
        // two different splits.
        boolean createdSplit = false;
        for (OneBlockInfo oneblock : blocks) {
          if (blockToNodes.containsKey(oneblock)) {
            validBlocks.add(oneblock);
            blockToNodes.remove(oneblock);
            curSplitSize += oneblock.length;

            // if the accumulated split size exceeds the maximum, then
            // create this split.
            if (maxSize != 0 && curSplitSize >= maxSize) {
              // create an input split and add it to the splits array
              addCreatedSplit(splits, getHosts(racks), validBlocks);
              createdSplit = true;
              break;
            }
          }
        }

        // if we created a split, then just go to the next rack
        if (createdSplit) {
          curSplitSize = 0;
          validBlocks.clear();
          racks.clear();
          continue;
        }

        if (!validBlocks.isEmpty()) {
          if (minSizeRack != 0 && curSplitSize >= minSizeRack) {
            // if there is a minimum size specified, then create a single split
            // otherwise, store these blocks into overflow data structure
            addCreatedSplit(splits, getHosts(racks), validBlocks);
          } else {
            // There were a few blocks in this rack that
            // remained to be processed. Keep them in 'overflow' block list.
            // These will be combined later.
            overflowBlocks.addAll(validBlocks);
          }
        }
        curSplitSize = 0;
        validBlocks.clear();
        racks.clear();
      }
    }

    assert blockToNodes.isEmpty();
    assert curSplitSize == 0;
    assert validBlocks.isEmpty();
    assert racks.isEmpty();

    // Process all overflow blocks
    for (OneBlockInfo oneblock : overflowBlocks) {
      validBlocks.add(oneblock);
      curSplitSize += oneblock.length;

      // This might cause an exiting rack location to be re-added,
      // but it should be ok.
      for (int i = 0; i < oneblock.racks.length; i++) {
        racks.add(oneblock.racks[i]);
      }

      // if the accumulated split size exceeds the maximum, then
      // create this split.
      if (maxSize != 0 && curSplitSize >= maxSize) {
        // create an input split and add it to the splits array
        addCreatedSplit(splits, getHosts(racks), validBlocks);
        curSplitSize = 0;
        validBlocks.clear();
        racks.clear();
      }
    }

    // Process any remaining blocks, if any.
    if (!validBlocks.isEmpty()) {
      addCreatedSplit(splits, getHosts(racks), validBlocks);
    }
  }

  /**
   * Return all the splits in the specified set of paths.
   * data is split into blocks
   */
  private void getMoreSplitsForSplitableCompressed(Configuration conf, Path[] paths,
                                                   long maxSize, long minSizeNode, long minSizeRack,
                                                   List<InputSplit> splits)
      throws IOException {

    // first we should check the input files:
    // 1.  .index files should be special treated.
    // 2. unindexed lzo file should be treated as "UnSplitable"
    Map<Path, LzoIndex> indexes = new HashMap<Path, LzoIndex>();

    FileSystem fs = FileSystem.get(conf);

    String fileExtension = new LzopCodec().getDefaultExtension();
    ArrayList<Path> unsplitableFiles = new ArrayList<Path>(paths.length);
    ArrayList<Path> splitableFiles = new ArrayList<Path>(paths.length);
    boolean hasSkippedPath = false;
    for (Path path : paths) {
      if (!path.getName().endsWith(fileExtension)) {
        hasSkippedPath = true;
      } else {
        //read the index file
        LzoIndex index = LzoIndex.readIndex(fs, path);
        if (index.isEmpty()) {
          unsplitableFiles.add(path);
          hasSkippedPath = true;
        } else {
          indexes.put(path, index);
          splitableFiles.add(path);
        }
      }
    }
    // the remaing files
    if (hasSkippedPath) {
      paths = splitableFiles.toArray(new Path[0]);
      if (unsplitableFiles.size() > 0) {
        Path[] unsplitablePaths = unsplitableFiles.toArray(new Path[0]);
        getMoreSplitsForUnsplitableCompressed(conf, unsplitablePaths, maxSize, minSizeNode, minSizeRack, splits);
      }
    }
    unsplitableFiles.clear();
    splitableFiles.clear();

    getMoreSplits(conf, paths, maxSize, minSizeNode, minSizeRack, splits, false, indexes);
  }

  /**
   * Create a single split from the list of blocks specified in validBlocks
   * Add this new split into splitList.
   */
  private void addCreatedSplit(List<InputSplit> splitList,
                               List<String> locations,
                               ArrayList<OneBlockInfo> validBlocks) {
    // create an input split
    Path[] fl = new Path[validBlocks.size()];
    long[] offset = new long[validBlocks.size()];
    long[] length = new long[validBlocks.size()];
    for (int i = 0; i < validBlocks.size(); i++) {
      fl[i] = validBlocks.get(i).onepath;
      offset[i] = validBlocks.get(i).offset;
      length[i] = validBlocks.get(i).length;
    }

    // add this split to the list that is returned
    CombineFileSplit thissplit = new CombineFileSplit(fl, offset,
        length, locations.toArray(new String[0]));
    splitList.add(thissplit);
  }

  /**
   * information about one file from the File System
   */
  private static class OneFileInfo {
    private long fileSize;               // size of the file
    private OneBlockInfo[] blocks;       // all blocks in this file

    OneFileInfo(Path path, Configuration conf,
                HashMap<String, List<OneBlockInfo>> rackToBlocks,
                HashMap<OneBlockInfo, String[]> blockToNodes,
                HashMap<String, List<OneBlockInfo>> nodeToBlocks,
                HashMap<String, Set<String>> rackToNodes,
                boolean treatOneFileAsOneBlock, Map<Path, LzoIndex> indexes)
        throws IOException {
      this.fileSize = 0;

      // get block locations from file system
      FileSystem fs = path.getFileSystem(conf);
      FileStatus stat = fs.getFileStatus(path);
      BlockLocation[] locations = fs.getFileBlockLocations(stat, 0,
          stat.getLen());
      // create a list of all block and their locations
      if (locations == null) {
        blocks = new OneBlockInfo[0];
      } else if (locations.length > 1 && treatOneFileAsOneBlock) {
        blocks = new OneBlockInfo[1];
        for (int i = 0; i < locations.length; i++) {
          fileSize += locations[i].getLength();
        }

        OneBlockInfo oneblock = new OneBlockInfo(path,
            0,
            fileSize,
            locations[0].getHosts(),
            locations[0].getTopologyPaths());
        blocks[0] = oneblock;

        updateBlock(oneblock, rackToBlocks, blockToNodes, nodeToBlocks, rackToNodes);
      } else {
        blocks = new OneBlockInfo[locations.length];
        LzoIndex index = null;
        // if lzo indexes, check indexes first
        if (indexes != null) {
          index = indexes.get(path);
          if (index == null) {
            throw new IOException("Index not found for " + path);
          }
        }

        for (int i = 0; i < locations.length; i++) {

          long start = locations[i].getOffset();
          long end = start + locations[i].getLength();

          // be compatible with lzo indexed file
          if (index != null) {
            long lzoStart = index.alignSliceStartToIndex(start, end);
            long lzoEnd = index.alignSliceEndToIndex(end, stat.getLen());

            if (lzoStart == LzoIndex.NOT_FOUND || lzoEnd == LzoIndex.NOT_FOUND) {
              continue;
            }
            end = lzoEnd;
            start = lzoStart;
          }

          fileSize += end - start;
          OneBlockInfo oneblock = new OneBlockInfo(path,
              start,
              end - start,
              locations[i].getHosts(),
              locations[i].getTopologyPaths());
          blocks[i] = oneblock;

          updateBlock(oneblock, rackToBlocks, blockToNodes, nodeToBlocks, rackToNodes);
        }
      }
    }

    private void updateBlock(OneBlockInfo oneblock,
                             HashMap<String, List<OneBlockInfo>> rackToBlocks,
                             HashMap<OneBlockInfo, String[]> blockToNodes,
                             HashMap<String, List<OneBlockInfo>> nodeToBlocks,
                             HashMap<String, Set<String>> rackToNodes) {

      // add this block to the block --> node locations map
      blockToNodes.put(oneblock, oneblock.hosts);

      // add this block to the rack --> block map
      for (int j = 0; j < oneblock.racks.length; j++) {
        String rack = oneblock.racks[j];
        List<OneBlockInfo> blklist = rackToBlocks.get(rack);
        if (blklist == null) {
          blklist = new ArrayList<OneBlockInfo>();
          rackToBlocks.put(rack, blklist);
        }
        blklist.add(oneblock);
        // Add this host to rackToNodes map
        addHostToRack(rackToNodes, oneblock.racks[j], oneblock.hosts[j]);
      }

      // add this block to the node --> block map
      for (int j = 0; j < oneblock.hosts.length; j++) {
        String node = oneblock.hosts[j];
        List<OneBlockInfo> blklist = nodeToBlocks.get(node);
        if (blklist == null) {
          blklist = new ArrayList<OneBlockInfo>();
          nodeToBlocks.put(node, blklist);
        }
        blklist.add(oneblock);
      }
    }

    long getLength() {
      return fileSize;
    }

  }

  /**
   * information about one block from the File System
   */
  private static class OneBlockInfo {
    Path onepath;                // name of this file
    long offset;                 // offset in file
    long length;                 // length of this block
    String[] hosts;              // nodes on which this block resides
    String[] racks;              // network topology of hosts

    OneBlockInfo(Path path, long offset, long len,
                 String[] hosts, String[] topologyPaths) {
      this.onepath = path;
      this.offset = offset;
      this.hosts = hosts;
      this.length = len;
      assert (hosts.length == topologyPaths.length ||
          topologyPaths.length == 0);

      // if the file system does not have any rack information, then
      // use dummy rack location.
      if (topologyPaths.length == 0) {
        topologyPaths = new String[hosts.length];
        for (int i = 0; i < topologyPaths.length; i++) {
          topologyPaths[i] = (new NodeBase(hosts[i],
              NetworkTopology.DEFAULT_RACK)).toString();
        }
      }

      // The topology paths have the host name included as the last
      // component. Strip it.
      this.racks = new String[topologyPaths.length];
      for (int i = 0; i < topologyPaths.length; i++) {
        this.racks[i] = (new NodeBase(topologyPaths[i])).getNetworkLocation();
      }
    }
  }

  private static void addHostToRack(HashMap<String, Set<String>> rackToNodes,
                                    String rack, String host) {
    Set<String> hosts = rackToNodes.get(rack);
    if (hosts == null) {
      hosts = new HashSet<String>();
      rackToNodes.put(rack, hosts);
    }
    hosts.add(host);
  }

  private List<String> getHosts(List<String> racks) {
    List<String> hosts = new ArrayList<String>();
    for (String rack : racks) {
      hosts.addAll(rackToNodes.get(rack));
    }
    return hosts;
  }

  private static enum FileType {
    SplitablePlainText,
    UnsplitableCompressed,
    SplitableCompressed,
  }

  /**
   * Accept a path only if any one of filters given in the
   * constructor do.
   */
  private static class MultiPathFilter implements PathFilter {
    private List<PathFilter> filters;
    private FileType fileType;

    public FileType getFileType() {
      return fileType;
    }

    public void setFileType(FileType fileType) {
      this.fileType = fileType;
    }

    public MultiPathFilter() {
      this(new ArrayList<PathFilter>());
    }

    public MultiPathFilter(List<PathFilter> filters) {
      this(filters, FileType.SplitablePlainText);
    }

    public MultiPathFilter(List<PathFilter> filters, FileType ft) {
      this.filters = filters;
      this.fileType = ft;
    }

    public void add(PathFilter one) {
      filters.add(one);
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (filter.accept(path)) {
          return true;
        }
      }
      return false;
    }

    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append("[");
      for (PathFilter f : filters) {
        buf.append(f);
        buf.append(",");
      }
      buf.append("]");
      return buf.toString();
    }
  }
}
