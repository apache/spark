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

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.UnspecifiedDistribution;
import org.apache.spark.sql.connector.expressions.SortOrder;

/**
 * A write that requires a specific distribution and ordering of data.
 *
 * @since 3.2.0
 */
@Experimental
public interface RequiresDistributionAndOrdering extends Write {
  /**
   * Returns the distribution required by this write.
   * <p>
   * Spark will distribute incoming records across partitions to satisfy the required distribution
   * before passing the records to the data source table on write.
   * <p>
   * Batch and micro-batch writes can request a particular data distribution.
   * If a distribution is requested in the micro-batch context, incoming records in each micro batch
   * will satisfy the required distribution (but not across micro batches). The continuous execution
   * mode continuously processes streaming data and does not support distribution requirements.
   * <p>
   * Implementations may return {@link UnspecifiedDistribution} if they don't require any specific
   * distribution of data on write.
   *
   * @return the required distribution
   */
  Distribution requiredDistribution();

  /**
   * Returns if the distribution required by this write is strictly required or best effort only.
   * <p>
   * If true, Spark will strictly distribute incoming records across partitions to satisfy
   * the required distribution before passing the records to the data source table on write.
   * Otherwise, Spark may apply certain optimizations to speed up the query but break
   * the distribution requirement.
   *
   * @return true if the distribution required by this write is strictly required; false otherwise.
   */
  default boolean distributionStrictlyRequired() { return true; }

  /**
   * Returns the number of partitions required by this write.
   * <p>
   * Implementations may override this to require a specific number of input partitions.
   * <p>
   * Note that Spark doesn't support the number of partitions on {@link UnspecifiedDistribution},
   * the query will fail if the number of partitions are provided but the distribution is
   * unspecified. Data sources may either request a particular number of partitions or
   * a preferred partition size via {@link #advisoryPartitionSizeInBytes}, not both.
   *
   * @return the required number of partitions, any value less than 1 mean no requirement.
   */
  default int requiredNumPartitions() { return 0; }

  /**
   * Returns the advisory (not guaranteed) shuffle partition size in bytes for this write.
   * <p>
   * Implementations may override this to indicate the preferable partition size in shuffles
   * performed to satisfy the requested distribution. Note that Spark doesn't support setting
   * the advisory partition size for {@link UnspecifiedDistribution}, the query will fail if
   * the advisory partition size is set but the distribution is unspecified. Data sources may
   * either request a particular number of partitions via {@link #requiredNumPartitions()} or
   * a preferred partition size, not both.
   * <p>
   * Data sources should be careful with large advisory sizes as it will impact the writing
   * parallelism and may degrade the overall job performance.
   * <p>
   * Note this value only acts like a guidance and Spark does not guarantee the actual and advisory
   * shuffle partition sizes will match. Ignored if the adaptive execution is disabled.
   *
   * @return the advisory partition size, any value less than 1 means no preference.
   */
  default long advisoryPartitionSizeInBytes() { return 0; }

  /**
   * Returns the ordering required by this write.
   * <p>
   * Spark will order incoming records within partitions to satisfy the required ordering
   * before passing those records to the data source table on write.
   * <p>
   * Batch and micro-batch writes can request a particular data ordering.
   * If an ordering is requested in the micro-batch context, incoming records in each micro batch
   * will satisfy the required ordering (but not across micro batches). The continuous execution
   * mode continuously processes streaming data and does not support ordering requirements.
   * <p>
   * Implementations may return an empty array if they don't require any specific ordering of data
   * on write.
   *
   * @return the required ordering
   */
  SortOrder[] requiredOrdering();
}
