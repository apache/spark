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

/**
 * A distribution policy decides, given a document with a document id, which
 * one shard the request should be sent to if the request is an insert, and
 * which shard(s) the request should be sent to if the request is a delete.
 */
public interface IDistributionPolicy {

  /**
   * Initialization. It must be called before any chooseShard() is called.
   * @param shards
   */
  void init(Shard[] shards);

  /**
   * Choose a shard to send an insert request.
   * @param key
   * @return the index of the chosen shard
   */
  int chooseShardForInsert(DocumentID key);

  /**
   * Choose a shard or all shards to send a delete request. E.g. a round-robin
   * distribution policy would send a delete request to all the shards.
   * -1 represents all the shards.
   * @param key
   * @return the index of the chosen shard, -1 if all the shards are chosen
   */
  int chooseShardForDelete(DocumentID key);

}
