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

package org.apache.spark.sql.connector.write;

/**
 * A logical representation of a data source which collects written partition metrics
 * <p>
 * Data sources must implement the corresponding methods in this interface to collect and report
 * metrics specific to each partition.
 */
public interface PartitionMetricsCollector {

  /**
   * Record the partition metrics into {@code PartitionMetricsWriteInfo} which were collected during
   * commit.
   *
   * @param metrics The stats for each written partition
   * @param messages The commit messages returned by {@code DataWriter.commit()}
   */
  void partitionMetrics(PartitionMetricsWriteInfo metrics, WriterCommitMessage[] messages);

}
