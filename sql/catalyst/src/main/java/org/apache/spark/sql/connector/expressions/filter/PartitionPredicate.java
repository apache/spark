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

package org.apache.spark.sql.connector.expressions.filter;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.PartitionFieldReference;

/**
 * Represents a partition predicate that can be evaluated using {@link Table#partitioning()}.
 * <p>
 * Connectors are expected to leverage partition predicates for pruning whenever they have
 * partition metadata to evaluate them. Use {@link #eval(InternalRow)} to evaluate this
 * predicate against a single partition's keys.
 * </p>
 *
 * @since 4.2.0
 */
@Evolving
public abstract class PartitionPredicate extends Predicate {

  public static final String NAME = "PARTITION_PREDICATE";

  protected PartitionPredicate() {
    super(NAME, EMPTY_EXPRESSION);
  }

  /**
   * {@inheritDoc}
   * <p>
   * For PartitionPredicate, returns {@link PartitionFieldReference} instances that identify
   * the partition fields (from {@link Table#partitioning()}) referenced by this predicate.
   * Each reference's {@link PartitionFieldReference#fieldNames()} gives the partition field
   * name; {@link PartitionFieldReference#ordinal()} gives the 0-based position in
   * {@link Table#partitioning()}.
   * <p>
   * <b>Example:</b> Suppose {@code Table.partitioning()} returns three partition
   * transforms: {@code [years(ts), months(ts), bucket(32, id)]} with ordinals 0, 1, 2.
   * Each {@link PartitionFieldReference} has {@link PartitionFieldReference#fieldNames()}
   * (the transform display name, e.g. {@code years(ts)}) and
   * {@link PartitionFieldReference#ordinal()}:
   * <ul>
   *   <li>{@code years(ts) = 2026} returns one reference: (fieldNames=[years(ts)], ordinal=0).</li>
   *   <li>{@code years(ts) = 2026 and months(ts) = 01} returns two references:
   *       (fieldNames=[years(ts)], ordinal=0), (fieldNames=[months(ts)], ordinal=1).</li>
   *   <li>{@code bucket(32, id) = 1} returns one reference:
   *       (fieldNames=[bucket(32, id)], ordinal=2).</li>
   * </ul>
   * <p>
   * Data sources can use these references to decide whether to return a predicate for post-scan
   * filtering. For example, sources supporting partition spec evolution should return
   * PartitionPredicates that reference later-added partition transforms (incompletely
   * partitioned data) to Spark for post-scan filter, while predicates that reference only
   * initially-added partition transforms may be fully pushed.
   *
   * @return array of partition field references
   */
  @Override
  public abstract NamedReference[] references();

  /**
   * Evaluates this predicate against a single partition's keys.
   * <p>
   * The caller must pass the <b>full</b> partition key: one value per partition transform in
   * {@link Table#partitioning()}, in order. A key for only a subset of referenced fields is not
   * supported.
   *
   * @param partitionKey the full partition key for one partition, ordered according to
   *                     {@link Table#partitioning()}.
   * @return true if the partition represented by these keys satisfies this predicate.
   */
  public abstract boolean eval(InternalRow partitionKey);
}
