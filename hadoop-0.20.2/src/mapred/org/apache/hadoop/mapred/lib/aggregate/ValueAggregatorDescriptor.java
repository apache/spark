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

package org.apache.hadoop.mapred.lib.aggregate;

import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

/**
 * This interface defines the contract a value aggregator descriptor must
 * support. Such a descriptor can be configured with a JobConf object. Its main
 * function is to generate a list of aggregation-id/value pairs. An aggregation
 * id encodes an aggregation type which is used to guide the way to aggregate
 * the value in the reduce/combiner phrase of an Aggregate based job.The mapper in
 * an Aggregate based map/reduce job may create one or more of
 * ValueAggregatorDescriptor objects at configuration time. For each input
 * key/value pair, the mapper will use those objects to create aggregation
 * id/value pairs.
 * 
 */
public interface ValueAggregatorDescriptor {

  public static final String TYPE_SEPARATOR = ":";

  public static final Text ONE = new Text("1");

  /**
   * Generate a list of aggregation-id/value pairs for the given key/value pair.
   * This function is usually called by the mapper of an Aggregate based job.
   * 
   * @param key
   *          input key
   * @param val
   *          input value
   * @return a list of aggregation id/value pairs. An aggregation id encodes an
   *         aggregation type which is used to guide the way to aggregate the
   *         value in the reduce/combiner phrase of an Aggregate based job.
   */
  public ArrayList<Entry<Text, Text>> generateKeyValPairs(Object key,
                                                          Object val);

  /**
   * Configure the object
   * 
   * @param job
   *          a JobConf object that may contain the information that can be used
   *          to configure the object.
   */
  public void configure(JobConf job);
}
