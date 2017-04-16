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

package org.apache.hadoop.mapreduce.lib.partition;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Partitioner effecting a total order by reading split points from
 * an externally generated source.
 */
public class TotalOrderPartitioner<K extends WritableComparable<?>,V>
    extends Partitioner<K,V> implements Configurable {

  private Node partitions;
  public static final String DEFAULT_PATH = "_partition.lst";
  public static final String PARTITIONER_PATH = 
    "mapreduce.totalorderpartitioner.path";
  public static final String MAX_TRIE_DEPTH = 
    "mapreduce.totalorderpartitioner.trie.maxdepth"; 
  public static final String NATURAL_ORDER = 
    "mapreduce.totalorderpartitioner.naturalorder";
  Configuration conf;

  public TotalOrderPartitioner() { }

  /**
   * Read in the partition file and build indexing data structures.
   * If the keytype is {@link org.apache.hadoop.io.BinaryComparable} and
   * <tt>total.order.partitioner.natural.order</tt> is not false, a trie
   * of the first <tt>total.order.partitioner.max.trie.depth</tt>(2) + 1 bytes
   * will be built. Otherwise, keys will be located using a binary search of
   * the partition keyset using the {@link org.apache.hadoop.io.RawComparator}
   * defined for this job. The input file must be sorted with the same
   * comparator and contain {@link Job#getNumReduceTasks()} - 1 keys.
   */
  @SuppressWarnings("unchecked") // keytype from conf not static
  public void setConf(Configuration conf) {
    try {
      this.conf = conf;
      String parts = getPartitionFile(conf);
      final Path partFile = new Path(parts);
      final FileSystem fs = (DEFAULT_PATH.equals(parts))
        ? FileSystem.getLocal(conf)     // assume in DistributedCache
        : partFile.getFileSystem(conf);

      Job job = new Job(conf);
      Class<K> keyClass = (Class<K>)job.getMapOutputKeyClass();
      K[] splitPoints = readPartitions(fs, partFile, keyClass, conf);
      if (splitPoints.length != job.getNumReduceTasks() - 1) {
        throw new IOException("Wrong number of partitions in keyset");
      }
      RawComparator<K> comparator =
        (RawComparator<K>) job.getSortComparator();
      for (int i = 0; i < splitPoints.length - 1; ++i) {
        if (comparator.compare(splitPoints[i], splitPoints[i+1]) >= 0) {
          throw new IOException("Split points are out of order");
        }
      }
      boolean natOrder =
        conf.getBoolean(NATURAL_ORDER, true);
      if (natOrder && BinaryComparable.class.isAssignableFrom(keyClass)) {
        partitions = buildTrie((BinaryComparable[])splitPoints, 0,
            splitPoints.length, new byte[0],
            // Now that blocks of identical splitless trie nodes are 
            // represented reentrantly, and we develop a leaf for any trie
            // node with only one split point, the only reason for a depth
            // limit is to refute stack overflow or bloat in the pathological
            // case where the split points are long and mostly look like bytes 
            // iii...iixii...iii   .  Therefore, we make the default depth
            // limit large but not huge.
            conf.getInt(MAX_TRIE_DEPTH, 200));
      } else {
        partitions = new BinarySearchNode(splitPoints, comparator);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Can't read partitions file", e);
    }
  }

  public Configuration getConf() {
    return conf;
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
  public static void setPartitionFile(Configuration conf, Path p) {
    conf.set(PARTITIONER_PATH, p.toString());
  }

  /**
   * Get the path to the SequenceFile storing the sorted partition keyset.
   * @see #setPartitionFile(Configuration, Path)
   */
  public static String getPartitionFile(Configuration conf) {
    return conf.get(PARTITIONER_PATH, DEFAULT_PATH);
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
   * @param level        the tree depth at this node
   * @param splitPoints  the full split point vector, which holds
   *                     the split point or points this leaf node
   *                     should contain
   * @param lower        first INcluded element of splitPoints
   * @param upper        first EXcluded element of splitPoints
   * @return  a leaf node.  They come in three kinds: no split points 
   *          [and the findParttion returns a canned index], one split
   *          point [and we compare with a single comparand], or more
   *          than one [and we do a binary search].  The last case is
   *          rare.
   */
  private TrieNode LeafTrieNodeFactory
             (int level, BinaryComparable[] splitPoints, int lower, int upper) {
      switch (upper - lower) {
      case 0:
          return new UnsplitTrieNode(level, lower);
          
      case 1:
          return new SinglySplitTrieNode(level, splitPoints, lower);
          
      default:
          return new LeafTrieNode(level, splitPoints, lower, upper);
      }
  }

  /**
   * A leaf trie node that scans for the key between lower..upper.
   * 
   * We don't generate many of these now, since we usually continue trie-ing 
   * when more than one split point remains at this level. and we make different
   * objects for nodes with 0 or 1 split point.
   */
  private class LeafTrieNode extends TrieNode {
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
  
  private class UnsplitTrieNode extends TrieNode {
      final int result;
      
      UnsplitTrieNode(int level, int value) {
          super(level);
          this.result = value;
      }
      
      public int findPartition(BinaryComparable key) {
          return result;
      }
  }
  
  private class SinglySplitTrieNode extends TrieNode {
      final int               lower;
      final BinaryComparable  mySplitPoint;
      
      SinglySplitTrieNode(int level, BinaryComparable[] splitPoints, int lower) {
          super(level);
          this.lower = lower;
          this.mySplitPoint = splitPoints[lower];
      }
      
      public int findPartition(BinaryComparable key) {
          return lower + (key.compareTo(mySplitPoint) < 0 ? 0 : 1);
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
      Configuration conf) throws IOException {
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, conf);
    ArrayList<K> parts = new ArrayList<K>();
    K key = ReflectionUtils.newInstance(keyClass, conf);
    NullWritable value = NullWritable.get();
    while (reader.next(key, value)) {
      parts.add(key);
      key = ReflectionUtils.newInstance(keyClass, conf);
    }
    reader.close();
    return parts.toArray((K[])Array.newInstance(keyClass, parts.size()));
  }
  
  /**
   * 
   * This object contains a TrieNodeRef if there is such a thing that
   * can be repeated.  Two adjacent trie node slots that contain no 
   * split points can be filled with the same trie node, even if they
   * are not on the same level.  See buildTreeRec, below.
   *
   */  
  private class CarriedTrieNodeRef
  {
      TrieNode   content;
      
      CarriedTrieNodeRef() {
          content = null;
      }
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
      return buildTrieRec
               (splits, lower, upper, prefix, maxDepth, new CarriedTrieNodeRef());
  }
  
  /**
   * This is the core of buildTrie.  The interface, and stub, above, just adds
   * an empty CarriedTrieNodeRef.  
   * 
   * We build trie nodes in depth first order, which is also in key space
   * order.  Every leaf node is referenced as a slot in a parent internal
   * node.  If two adjacent slots [in the DFO] hold leaf nodes that have
   * no split point, then they are not separated by a split point either, 
   * because there's no place in key space for that split point to exist.
   * 
   * When that happens, the leaf nodes would be semantically identical, and
   * we reuse the object.  A single CarriedTrieNodeRef "ref" lives for the 
   * duration of the tree-walk.  ref carries a potentially reusable, unsplit
   * leaf node for such reuse until a leaf node with a split arises, which 
   * breaks the chain until we need to make a new unsplit leaf node.
   * 
   * Note that this use of CarriedTrieNodeRef means that for internal nodes, 
   * for internal nodes if this code is modified in any way we still need 
   * to make or fill in the subnodes in key space order.
   */
  private TrieNode buildTrieRec(BinaryComparable[] splits, int lower,
      int upper, byte[] prefix, int maxDepth, CarriedTrieNodeRef ref) {
    final int depth = prefix.length;
    // We generate leaves for a single split point as well as for 
    // no split points.
    if (depth >= maxDepth || lower >= upper - 1) {
        // If we have two consecutive requests for an unsplit trie node, we
        // can deliver the same one the second time.
        if (lower == upper && ref.content != null) {
            return ref.content;
        }
        TrieNode  result = LeafTrieNodeFactory(depth, splits, lower, upper);
        ref.content = lower == upper ? result : null;
        return result;
    }
    InnerTrieNode result = new InnerTrieNode(depth);
    byte[] trial = Arrays.copyOf(prefix, prefix.length + 1);
    // append an extra byte on to the prefix
    int         currentBound = lower;
    for(int ch = 0; ch < 0xFF; ++ch) {
      trial[depth] = (byte) (ch + 1);
      lower = currentBound;
      while (currentBound < upper) {
        if (splits[currentBound].compareTo(trial, 0, trial.length) >= 0) {
          break;
        }
        currentBound += 1;
      }
      trial[depth] = (byte) ch;
      result.child[0xFF & ch]
                   = buildTrieRec(splits, lower, currentBound, trial, maxDepth, ref);
    }
    // pick up the rest
    trial[depth] = (byte)0xFF;
    result.child[0xFF] 
                 = buildTrieRec(splits, lower, currentBound, trial, maxDepth, ref);
    
    return result;
  }
}
