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

package org.apache.hadoop.contrib.index.example;

import org.apache.hadoop.contrib.index.mapred.DocumentID;
import org.apache.hadoop.contrib.index.mapred.IDistributionPolicy;
import org.apache.hadoop.contrib.index.mapred.Shard;

/**
 * Choose a shard for each insert in a round-robin fashion. Choose all the
 * shards for each delete because we don't know where it is stored.
 */
public class RoundRobinDistributionPolicy implements IDistributionPolicy {

  private int numShards;
  private int rr; // round-robin implementation

  /* (non-Javadoc)
   * @see org.apache.hadoop.contrib.index.mapred.IDistributionPolicy#init(org.apache.hadoop.contrib.index.mapred.Shard[])
   */
  public void init(Shard[] shards) {
    numShards = shards.length;
    rr = 0;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.contrib.index.mapred.IDistributionPolicy#chooseShardForInsert(org.apache.hadoop.contrib.index.mapred.DocumentID)
   */
  public int chooseShardForInsert(DocumentID key) {
    int chosen = rr;
    rr = (rr + 1) % numShards;
    return chosen;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.contrib.index.mapred.IDistributionPolicy#chooseShardForDelete(org.apache.hadoop.contrib.index.mapred.DocumentID)
   */
  public int chooseShardForDelete(DocumentID key) {
    // -1 represents all the shards
    return -1;
  }
}
