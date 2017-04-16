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

package org.apache.hadoop.contrib.index.mapred;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * This partitioner class puts the values of the same key - in this case the
 * same shard - in the same partition.
 */
public class IndexUpdatePartitioner implements
    Partitioner<Shard, IntermediateForm> {

  private Shard[] shards;
  private Map<Shard, Integer> map;

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.Partitioner#getPartition(java.lang.Object, java.lang.Object, int)
   */
  public int getPartition(Shard key, IntermediateForm value, int numPartitions) {
    int partition = map.get(key).intValue();
    if (partition < numPartitions) {
      return partition;
    } else {
      return numPartitions - 1;
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.JobConfigurable#configure(org.apache.hadoop.mapred.JobConf)
   */
  public void configure(JobConf job) {
    shards = Shard.getIndexShards(new IndexUpdateConfiguration(job));
    map = new HashMap<Shard, Integer>();
    for (int i = 0; i < shards.length; i++) {
      map.put(shards[i], i);
    }
  }

}
