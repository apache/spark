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

import java.io.IOException;
import java.util.*;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.mapred.*;

public class CombineSplitInputFormat<K, V> implements InputFormat<K, V> {

  private InputFormat<K, V> inputformat;
  private long splitSize = 0;

  public CombineSplitInputFormat(InputFormat<K, V> inputformat, long splitSize) {
    this.inputformat = inputformat;
    this.splitSize = splitSize;
  }

  /**
   * Create a single split from the list of blocks specified in validBlocks
   * Add this new split into splitList.
   */
  private void addCreatedSplit(List<CombineSplit> splitList,
                               long totalLen,
                               Collection<String> locations,
                               List<InputSplit> validSplits) {
    CombineSplit combineSparkSplit =
      new CombineSplit(validSplits.toArray(new InputSplit[0]),
        totalLen, locations.toArray(new String[0]));
    splitList.add(combineSparkSplit);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    InputSplit[] splits = inputformat.getSplits(job, numSplits);
    // populate nodeToSplits and splitsSet
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
    List<CombineSplit> combineSparkSplits = Lists.newArrayList();
    List<InputSplit> oneCombinedSplits = Lists.newArrayList();
    long currentSplitSize = 0L;
    for (Map.Entry<String, List<InputSplit>> entry: nodeToSplits.entrySet()) {
      String node = entry.getKey();
      List<InputSplit> splitsPerNode = entry.getValue();
      for (InputSplit split: splitsPerNode) {
        if (splitSize != 0 && currentSplitSize > splitSize) {
          addCreatedSplit(combineSparkSplits,
            currentSplitSize, Collections.singleton(node), oneCombinedSplits);
          currentSplitSize = 0;
          oneCombinedSplits.clear();
        }
        // this split has been combined
        if (!splitsSet.contains(split)) {
          continue;
        } else {
          currentSplitSize += split.getLength();
          oneCombinedSplits.add(split);
          splitsSet.remove(split);
        }
      }
      // populate the remaining splits into one combined split
      if (!oneCombinedSplits.isEmpty()) {
        long remainLen = 0;
        for (InputSplit s: oneCombinedSplits) {
          remainLen += s.getLength();
        }
        addCreatedSplit(combineSparkSplits,
          remainLen, Collections.singleton(node), oneCombinedSplits);
        currentSplitSize = 0;
        oneCombinedSplits.clear();
      }
    }
    return combineSparkSplits.toArray(new InputSplit[0]);
  }

  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
     return new CombineSplitRecordReader(job, (CombineSplit)split, inputformat);
  }
}
