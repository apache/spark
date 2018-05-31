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

import org.apache.spark.annotation.InterfaceStability;

/**
 * An input partition returned by {@link DataSourceReader#planInputPartitions()} and is
 * responsible for creating the actual data reader of one RDD partition.
 * The relationship between {@link InputPartition} and {@link InputPartitionReader}
 * is similar to the relationship between {@link Iterable} and {@link java.util.Iterator}.
 *
 * Note that {@link InputPartition}s will be serialized and sent to executors, then
 * {@link InputPartitionReader}s will be created on executors to do the actual reading. So
 * {@link InputPartition} must be serializable while {@link InputPartitionReader} doesn't need to
 * be.
 */
@InterfaceStability.Evolving
public interface InputPartition<T> extends Serializable {

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

  /**
   * Returns an input partition reader to do the actual reading work.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  InputPartitionReader<T> createPartitionReader();
}
