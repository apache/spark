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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Partitioner effecting a total order by reading split points from
 * an externally generated source.
 */
public class TotalOrderPartitioner<K extends WritableComparable,V>
    implements Partitioner<K,V> {

  private Node partitions;
  public static final String DEFAULT_PATH = "_partition.lst";

  public TotalOrderPartitioner() { }

  /**
   * Read in the partition file and build indexing data structures.
   * If the keytype is {@link org.apache.hadoop.io.BinaryComparable} and
   * <tt>total.order.partitioner.natural.order</tt> is not false, a trie
   * of the first <tt>total.order.partitioner.max.trie.depth</tt>(2) + 1 bytes
   * will be built. Otherwise, keys will be located using a binary search of
   * the partition keyset using the {@link org.apache.hadoop.io.RawComparator}
   * defined for this job. The input file must be sorted with the same
   * comparator and contain {@link
     org.apache.hadoop.mapred.JobConf#getNumReduceTasks} - 1 keys.
   */
  @SuppressWarnings("unchecked") // keytype from conf not static
  public void configure(JobConf job) {
    try {
      String parts = getPartitionFile(job);
      final Path partFile = new Path(parts);
      final FileSystem fs = (DEFAULT_PATH.equals(parts))
        ? FileSystem.getLocal(job)     // assume in DistributedCache
        : partFile.getFileSystem(job);

      Class<K> keyClass = (Class<K>)job.getMapOutputKeyClass();
      K[] splitPoints = readPartitions(fs, partFile, keyClass, job);
      if (splitPoints.length != job.getNumReduceTasks() - 1) {
        throw new IOException("Wrong number of partitions in keyset");
      }
      RawComparator<K> comparator =
        (RawComparator<K>) job.getOutputKeyComparator();
      for (int i = 0; i < splitPoints.length - 1; ++i) {
        if (comparator.compare(splitPoints[i], splitPoints[i+1]) >= 0) {
          throw new IOException("Split points are out of order");
        }
      }
      boolean natOrder =
        job.getBoolean("total.order.partitioner.natural.order", true);
      if (natOrder && BinaryComparable.class.isAssignableFrom(keyClass)) {
        partitions = buildTrie((BinaryComparable[])splitPoints, 0,
            splitPoints.length, new byte[0],
            job.getInt("total.order.partitioner.max.trie.depth", 2));
      } else {
        partitions = new BinarySearchNode(splitPoints, comparator);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Can't read partitions file", e);
    }
  }

                                 // by construction, we know if our keytype
  @SuppressWarnings("unchecked") // is memcmp-able and uses the trie
  public int getPartition(K key, V value, int numPartitions) {
    return partitions.findPartition(key);
  }

  /**
   * Set the path to the SequenceFile storing the sorted partition keyset.
   * It must be the case that for <tt>R</tt> reduces, there are <tt>R-1</tt>
   * keys in the SequenceFile.
   */
  public static void setPartitionFile(JobConf job, Path p) {
    job.set("total.order.partitioner.path", p.toString());
  }

  /**
   * Get the path to the SequenceFile storing the sorted partition keyset.
   * @see #setPartitionFile(JobConf,Path)
   */
  public static String getPartitionFile(JobConf job) {
    return job.get("total.order.partitioner.path", DEFAULT_PATH);
  }

  /**
   * Interface to the partitioner to locate a key in the partition keyset.
   */
  interface Node<T> {
    /**
     * Locate partition in keyset K, st [Ki..Ki+1) defines a partition,
     * with implicit K0 = -inf, Kn = +inf, and |K| = #partitions - 1.
     */
    int findPartition(T key);
  }

  /**
   * Base class for trie nodes. If the keytype is memcomp-able, this builds
   * tries of the first <tt>total.order.partitioner.max.trie.depth</tt>
   * bytes.
   */
  static abstract class TrieNode implements Node<BinaryComparable> {
    private final int level;
    TrieNode(int level) {
      this.level = level;
    }
    int getLevel() {
      return level;
    }
  }

  /**
   * For types that are not {@link org.apache.hadoop.io.BinaryComparable} or
   * where disabled by <tt>total.order.partitioner.natural.order</tt>,
   * search the partition keyset with a binary search.
   */
  class BinarySearchNode implements Node<K> {
    private final K[] splitPoints;
    private final RawComparator<K> comparator;
    BinarySearchNode(K[] splitPoints, RawComparator<K> comparator) {
      this.splitPoints = splitPoints;
      this.comparator = comparator;
    }
    public int findPartition(K key) {
      final int pos = Arrays.binarySearch(splitPoints, key, comparator) + 1;
      return (pos < 0) ? -pos : pos;
    }
  }

  /**
   * An inner trie node that contains 256 children based on the next
   * character.
   */
  class InnerTrieNode extends TrieNode {
    private TrieNode[] child = new TrieNode[256];

    InnerTrieNode(int level) {
      super(level);
    }
    public int findPartition(BinaryComparable key) {
      int level = getLevel();
      if (key.getLength() <= level) {
        return child[0].findPartition(key);
      }
      return child[0xFF & key.getBytes()[level]].findPartition(key);
    }
  }

  /**
   * A leaf trie node that scans for the key between lower..upper.
   */
  class LeafTrieNode extends TrieNode {
    final int lower;
    final int upper;
    final BinaryComparable[] splitPoints;
    LeafTrieNode(int level, BinaryComparable[] splitPoints, int lower, int upper) {
      super(level);
      this.lower = lower;
      this.upper = upper;
      this.splitPoints = splitPoints;
    }
    public int findPartition(BinaryComparable key) {
      final int pos = Arrays.binarySearch(splitPoints, lower, upper, key) + 1;
      return (pos < 0) ? -pos : pos;
    }
  }


  /**
   * Read the cut points from the given IFile.
   * @param fs The file system
   * @param p The path to read
   * @param keyClass The map output key class
   * @param job The job config
   * @throws IOException
   */
                                 // matching key types enforced by passing in
  @SuppressWarnings("unchecked") // map output key class
  private K[] readPartitions(FileSystem fs, Path p, Class<K> keyClass,
      JobConf job) throws IOException {
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, job);
    ArrayList<K> parts = new ArrayList<K>();
    K key = (K) ReflectionUtils.newInstance(keyClass, job);
    NullWritable value = NullWritable.get();
    while (reader.next(key, value)) {
      parts.add(key);
      key = (K) ReflectionUtils.newInstance(keyClass, job);
    }
    reader.close();
    return parts.toArray((K[])Array.newInstance(keyClass, parts.size()));
  }

  /**
   * Given a sorted set of cut points, build a trie that will find the correct
   * partition quickly.
   * @param splits the list of cut points
   * @param lower the lower bound of partitions 0..numPartitions-1
   * @param upper the upper bound of partitions 0..numPartitions-1
   * @param prefix the prefix that we have already checked against
   * @param maxDepth the maximum depth we will build a trie for
   * @return the trie node that will divide the splits correctly
   */
  private TrieNode buildTrie(BinaryComparable[] splits, int lower,
      int upper, byte[] prefix, int maxDepth) {
    final int depth = prefix.length;
    if (depth >= maxDepth || lower == upper) {
      return new LeafTrieNode(depth, splits, lower, upper);
    }
    InnerTrieNode result = new InnerTrieNode(depth);
    byte[] trial = Arrays.copyOf(prefix, prefix.length + 1);
    // append an extra byte on to the prefix
    int currentBound = lower;
    for(int ch = 0; ch < 255; ++ch) {
      trial[depth] = (byte) (ch + 1);
      lower = currentBound;
      while (currentBound < upper) {
        if (splits[currentBound].compareTo(trial, 0, trial.length) >= 0) {
          break;
        }
        currentBound += 1;
      }
      trial[depth] = (byte) ch;
      result.child[0xFF & ch] = buildTrie(splits, lower, currentBound, trial,
                                   maxDepth);
    }
    // pick up the rest
    trial[depth] = 127;
    result.child[255] = buildTrie(splits, currentBound, upper, trial,
                                  maxDepth);
    return result;
  }
}
