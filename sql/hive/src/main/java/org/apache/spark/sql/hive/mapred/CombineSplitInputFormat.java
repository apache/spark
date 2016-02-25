/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.mapred;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.mapred.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class CombineSplitInputFormat<K, V> implements InputFormat<K, V> {

  private InputFormat<K, V> delegate;
  private long splitCombineSize = 0;

  public CombineSplitInputFormat(InputFormat<K, V> inputformat, long sSize) {
    this.delegate = inputformat;
    this.splitCombineSize = sSize;
  }

  private CombineSplit createCombineSplit(
      long totalLen,
      Collection<String> locations,
      List<InputSplit> combineSplitBuffer) {
      return new CombineSplit(combineSplitBuffer.toArray(new InputSplit[0]),
        totalLen, locations.toArray(new String[0]));
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    InputSplit[] splits = delegate.getSplits(job, numSplits);
    Map<String, List<InputSplit>> nodeToSplits = Maps.newHashMap();
    Set<InputSplit> splitsSet = Sets.newHashSet();
    for (InputSplit split: splits) {
      for (String node: split.getLocations()) {
        if (!nodeToSplits.containsKey(node)) {
          nodeToSplits.put(node, new ArrayList<InputSplit>());
        }
        nodeToSplits.get(node).add(split);
      }
      splitsSet.add(split);
    }
    // Iterate the nodes to combine in order to evenly distributing the splits
    // Ideally splits within the same combination should be in the same node
    List<CombineSplit> combineSparkSplits = Lists.newArrayList();
    List<InputSplit> combinedSplitBuffer = Lists.newArrayList();
    long accumulatedSplitSize = 0L;
    for (Map.Entry<String, List<InputSplit>> entry: nodeToSplits.entrySet()) {
      String node = entry.getKey();
      List<InputSplit> splitsPerNode = entry.getValue();
      for (InputSplit split: splitsPerNode) {
        // this split has been combined
        if (!splitsSet.contains(split)) {
          continue;
        } else {
          accumulatedSplitSize += split.getLength();
          combinedSplitBuffer.add(split);
          splitsSet.remove(split);
        }
        if (splitCombineSize > 0 && accumulatedSplitSize >= splitCombineSize) {
          // TODO: optimize this by providing the second/third preference locations
          combineSparkSplits.add(createCombineSplit(
            accumulatedSplitSize, Collections.singleton(node), combinedSplitBuffer));
          accumulatedSplitSize = 0;
          combinedSplitBuffer.clear();
        }
      }
      // populate the remaining splits into one combined split
      if (!combinedSplitBuffer.isEmpty()) {
        long remainLen = 0;
        for (InputSplit s: combinedSplitBuffer) {
          remainLen += s.getLength();
        }
        combineSparkSplits.add(createCombineSplit(
          remainLen, Collections.singleton(node), combinedSplitBuffer));
        accumulatedSplitSize = 0;
        combinedSplitBuffer.clear();
      }
    }
    return combineSparkSplits.toArray(new InputSplit[0]);
  }

  @Override
  public RecordReader<K, V> getRecordReader(final InputSplit split,
      final JobConf jobConf, final Reporter reporter) throws IOException {
    return new RecordReader<K, V>() {
      protected int idx = 0;
      protected long progressedBytes = 0;
      protected RecordReader<K, V> curReader;
      protected CombineSplit combineSplit;
      {
        combineSplit = (CombineSplit)split;
        initNextRecordReader();
      }

      @Override
      public boolean next(K key, V value) throws IOException {
        while ((curReader == null) || !curReader.next(key, value)) {
          if (!initNextRecordReader()) {
            return false;
          }
        }
        return true;
      }

    public K createKey() {
      return curReader.createKey();
    }

    public V createValue() {
      return curReader.createValue();
    }

    /**
     * return the amount of data processed
     */
    public long getPos() throws IOException {
      return progressedBytes + curReader.getPos();
    }

    public void close() throws IOException {
      if (curReader != null) {
        curReader.close();
        curReader = null;
      }
    }

    /**
     * return progress based on the amount of data processed so far.
     */
    public float getProgress() throws IOException {
      return Math.min(1.0f,  progressedBytes /(float)(split.getLength()));
    }

    /**
     * Get the record reader for the next split in this CombineSplit.
     */
    protected boolean initNextRecordReader() throws IOException {

      if (curReader != null) {
        curReader.close();
        curReader = null;
        if (idx > 0) {
          progressedBytes += combineSplit.getSplit(idx-1).getLength(); // done processing so far
        }
      }

      // if all splits have been processed, nothing more to do.
      if (idx == combineSplit.getSplitNum()) {
        return false;
      }

      // get a record reader for the idx-th split
      try {
        curReader = delegate.getRecordReader(combineSplit.getSplit(idx), jobConf, Reporter.NULL);
      } catch (Exception e) {
        throw new RuntimeException (e);
      }
      idx++;
      return true;
    }
    };
  }

  public static class CombineSplit implements InputSplit {
    private InputSplit[] splits;
    private long totalLen;
    private String[] locations;

    public CombineSplit() {
    }

    public CombineSplit(InputSplit[] ss, long totalLen, String[] locations) {
      splits = ss;
      this.totalLen = totalLen;
      this.locations = locations;
    }

    public InputSplit getSplit(int idx) {
      return splits[idx];
    }

    public int getSplitNum() {
      return splits.length;
    }

    @Override
    public long getLength() {
      return totalLen;
    }

    @Override
    public String[] getLocations() throws IOException {
      return locations;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      // We only process combination within a single table partition,
      // so all of the class name of the splits should be identical.
      out.writeUTF(splits[0].getClass().getCanonicalName());
      out.writeLong(totalLen);
      out.writeInt(locations.length);
      for (String location : locations) {
        out.writeUTF(location);
      }
      out.writeInt(splits.length);
      for (InputSplit split : splits) {
        split.write(out);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      String className = in.readUTF();
      this.totalLen = in.readLong();
      this.locations = new String[in.readInt()];
      for (int i = 0; i < locations.length; i++) {
        locations[i] = in.readUTF();
      }
      splits = new InputSplit[in.readInt()];
      Class<? extends Writable> clazz = null;
      try {
        clazz = (Class<? extends Writable>) Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      for (int i = 0; i < splits.length; i++) {
        Writable value = WritableFactories.newInstance(clazz, null);
        value.readFields(in);
        splits[i] = (InputSplit) value;
      }
    }
  }
}
