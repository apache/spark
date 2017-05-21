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

package org.apache.spark.io;

import org.apache.spark.TaskContext;
import org.apache.spark.annotation.Experimental;

/**
 * An input source for Spark that specifies how the input is partitioned and how data can be read.
 * Implementations of this class must be serializable.
 *
 * An {@link InputSource} is described by:
 * <ul>
 *   <li>an array of {@link InputPartition} that specifies the data partitioning</li>
 *   <li>a {@link RecordReader} that specifies how data on each partition can be read</li>
 * </ul>
 *
 * This is similar to Hadoop's InputFormat, except there is no explicit key, value pairs.
 */
@Experimental
public abstract class InputSource<T> {

  /**
   * Returns the list of {@link InputPartition} for this input data source.
   */
  public abstract InputPartition[] getPartitions() throws Exception;

  /**
   * Creates a new {@link RecordReader} for reading data in the given partition.
   */
  public abstract RecordReader<T> createRecordReader(
      InputPartition partition, TaskContext taskContext);
}
