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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.types.StructType;

/**
 * The base interface for all the batch and streaming read supports. Data sources should implement
 * concrete read support interfaces like {@link BatchReadSupport}.
 *
 * If Spark fails to execute any methods in the implementations of this interface (by throwing an
 * exception), the read action will fail and no Spark job will be submitted.
 */
@Evolving
public interface ReadSupport {

  /**
   * Returns the full schema of this data source, which is usually the physical schema of the
   * underlying storage. This full schema should not be affected by column pruning or other
   * optimizations.
   */
  StructType fullSchema();

  /**
   * Returns a list of {@link InputPartition input partitions}. Each {@link InputPartition}
   * represents a data split that can be processed by one Spark task. The number of input
   * partitions returned here is the same as the number of RDD partitions this scan outputs.
   *
   * Note that, this may not be a full scan if the data source supports optimization like filter
   * push-down. Implementations should check the input {@link ScanConfig} and adjust the resulting
   * {@link InputPartition input partitions}.
   */
  InputPartition[] planInputPartitions(ScanConfig config);
}
