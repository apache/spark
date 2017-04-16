/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.gridmix;

import java.io.IOException;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.gridmix.RandomAlgorithms.Selector;

/**
 * Class for caching a pool of input data to be used by synthetic jobs for
 * simulating read traffic.
 */
class FilePool {

  public static final Log LOG = LogFactory.getLog(FilePool.class);

  /**
   * The minimum file size added to the pool. Default 128MiB.
   */
  public static final String GRIDMIX_MIN_FILE = "gridmix.min.file.size";

  /**
   * The maximum size for files added to the pool. Defualts to 100TiB.
   */
  public static final String GRIDMIX_MAX_TOTAL = "gridmix.max.total.scan";

  private Node root;
  private final Path path;
  private final FileSystem fs;
  private final Configuration conf;
  private final ReadWriteLock updateLock;

  /**
   * Initialize a filepool under the path provided, but do not populate the
   * cache.
   */
  public FilePool(Configuration conf, Path input) throws IOException {
    root = null;
    this.conf = conf;
    this.path = input;
    this.fs = path.getFileSystem(conf);
    updateLock = new ReentrantReadWriteLock();
  }

  /**
   * Gather a collection of files at least as large as minSize.
   * @return The total size of files returned.
   */
  public long getInputFiles(long minSize, Collection<FileStatus> files)
      throws IOException {
    updateLock.readLock().lock();
    try {
      return root.selectFiles(minSize, files);
    } finally {
      updateLock.readLock().unlock();
    }
  }

  /**
   * (Re)generate cache of input FileStatus objects.
   */
  public void refresh() throws IOException {
    updateLock.writeLock().lock();
    try {
      root = new InnerDesc(fs, fs.getFileStatus(path),
        new MinFileFilter(conf.getLong(GRIDMIX_MIN_FILE, 128 * 1024 * 1024),
                          conf.getLong(GRIDMIX_MAX_TOTAL, 100L * (1L << 40))));
      if (0 == root.getSize()) {
        throw new IOException("Found no satisfactory file in " + path);
      }
    } finally {
      updateLock.writeLock().unlock();
    }
  }

  /**
   * Get a set of locations for the given file.
   */
  public BlockLocation[] locationsFor(FileStatus stat, long start, long len)
      throws IOException {
    // TODO cache
    return fs.getFileBlockLocations(stat, start, len);
  }

  static abstract class Node {

    protected final static Random rand = new Random();

    /**
     * Total size of files and directories under the current node.
     */
    abstract long getSize();

    /**
     * Return a set of files whose cumulative size is at least
     * <tt>targetSize</tt>.
     * TODO Clearly size is not the only criterion, e.g. refresh from
     * generated data without including running task output, tolerance
     * for permission issues, etc.
     */
    abstract long selectFiles(long targetSize, Collection<FileStatus> files)
        throws IOException;
  }

  /**
   * Files in current directory of this Node.
   */
  static class LeafDesc extends Node {
    final long size;
    final ArrayList<FileStatus> curdir;

    LeafDesc(ArrayList<FileStatus> curdir, long size) {
      this.size = size;
      this.curdir = curdir;
    }

    @Override
    public long getSize() {
      return size;
    }

    @Override
    public long selectFiles(long targetSize, Collection<FileStatus> files)
        throws IOException {
      if (targetSize >= getSize()) {
        files.addAll(curdir);
        return getSize();
      }

      Selector selector = new Selector(curdir.size(), (double) targetSize
          / getSize(), rand);
      
      ArrayList<Integer> selected = new ArrayList<Integer>();
      long ret = 0L;
      do {
        int index = selector.next();
        selected.add(index);
        ret += curdir.get(index).getLen();
      } while (ret < targetSize);

      for (Integer i : selected) {
        files.add(curdir.get(i));
      }

      return ret;
    }
  }

  /**
   * A subdirectory of the current Node.
   */
  static class InnerDesc extends Node {
    final long size;
    final double[] dist;
    final Node[] subdir;

    private static final Comparator<Node> nodeComparator =
      new Comparator<Node>() {
          public int compare(Node n1, Node n2) {
            return n1.getSize() < n2.getSize() ? -1
                 : n1.getSize() > n2.getSize() ? 1 : 0;
          }
    };

    InnerDesc(final FileSystem fs, FileStatus thisDir, MinFileFilter filter)
        throws IOException {
      long fileSum = 0L;
      final ArrayList<FileStatus> curFiles = new ArrayList<FileStatus>();
      final ArrayList<FileStatus> curDirs = new ArrayList<FileStatus>();
      for (FileStatus stat : fs.listStatus(thisDir.getPath())) {
        if (stat.isDir()) {
          curDirs.add(stat);
        } else if (filter.accept(stat)) {
          curFiles.add(stat);
          fileSum += stat.getLen();
        }
      }
      ArrayList<Node> subdirList = new ArrayList<Node>();
      if (!curFiles.isEmpty()) {
        subdirList.add(new LeafDesc(curFiles, fileSum));
      }
      for (Iterator<FileStatus> i = curDirs.iterator();
          !filter.done() && i.hasNext();) {
        // add subdirectories
        final Node d = new InnerDesc(fs, i.next(), filter);
        final long dSize = d.getSize();
        if (dSize > 0) {
          fileSum += dSize;
          subdirList.add(d);
        }
      }
      size = fileSum;
      LOG.debug(size + " bytes in " + thisDir.getPath());
      subdir = subdirList.toArray(new Node[subdirList.size()]);
      Arrays.sort(subdir, nodeComparator);
      dist = new double[subdir.length];
      for (int i = dist.length - 1; i > 0; --i) {
        fileSum -= subdir[i].getSize();
        dist[i] = fileSum / (1.0 * size);
      }
    }

    @Override
    public long getSize() {
      return size;
    }

    @Override
    public long selectFiles(long targetSize, Collection<FileStatus> files)
        throws IOException {
      long ret = 0L;
      if (targetSize >= getSize()) {
        // request larger than all subdirs; add everything
        for (Node n : subdir) {
          long added = n.selectFiles(targetSize, files);
          ret += added;
          targetSize -= added;
        }
        return ret;
      }

      // can satisfy request in proper subset of contents
      // select random set, weighted by size
      final HashSet<Node> sub = new HashSet<Node>();
      do {
        assert sub.size() < subdir.length;
        final double r = rand.nextDouble();
        int pos = Math.abs(Arrays.binarySearch(dist, r) + 1) - 1;
        while (sub.contains(subdir[pos])) {
          pos = (pos + 1) % subdir.length;
        }
        long added = subdir[pos].selectFiles(targetSize, files);
        ret += added;
        targetSize -= added;
        sub.add(subdir[pos]);
      } while (targetSize > 0);
      return ret;
    }
  }

  /**
   * Filter enforcing the minFile/maxTotal parameters of the scan.
   */
  private static class MinFileFilter {

    private long totalScan;
    private final long minFileSize;

    public MinFileFilter(long minFileSize, long totalScan) {
      this.minFileSize = minFileSize;
      this.totalScan = totalScan;
    }
    public boolean done() {
      return totalScan <= 0;
    }
    public boolean accept(FileStatus stat) {
      final boolean done = done();
      if (!done && stat.getLen() >= minFileSize) {
        totalScan -= stat.getLen();
        return true;
      }
      return false;
    }
  }

}
