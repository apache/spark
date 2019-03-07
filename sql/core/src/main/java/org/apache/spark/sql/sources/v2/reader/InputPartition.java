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

package org.apache.spark.sql.sources.v2.reader;

import java.io.Serializable;

import org.apache.spark.annotation.Evolving;

/**
 * A serializable representation of an input partition returned by
 * {@link Batch#planInputPartitions()} and the corresponding ones in streaming .
 *
 * Note that {@link InputPartition} will be serialized and sent to executors, then
 * {@link PartitionReader} will be created by
 * {@link PartitionReaderFactory#createReader(InputPartition)} or
 * {@link PartitionReaderFactory#createColumnarReader(InputPartition)} on executors to do
 * the actual reading. So {@link InputPartition} must be serializable while {@link PartitionReader}
 * doesn't need to be.
 */
@Evolving
public interface InputPartition extends Serializable {

  /**
   * The preferred locations where the input partition reader returned by this partition can run
   * faster, but Spark does not guarantee to run the input partition reader on these locations.
   * The implementations should make sure that it can be run on any location.
   * The location is a string representing the host name.
   *
   * Note that if a host name cannot be recognized by Spark, it will be ignored as it was not in
   * the returned locations. The default return value is empty string array, which means this
   * input partition's reader has no location preference.
   *
   * If this method fails (by throwing an exception), the action will fail and no Spark job will be
   * submitted.
   */
  default String[] preferredLocations() {
    return new String[0];
  }
}
