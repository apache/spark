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
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.PartitionColumnReference;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;

/**
 * Represents a partition predicate that can be evaluated using {@link Table#partitioning()}.
 * <p>
 * Connectors are expected to leverage partition predicates for pruning whenever they have
 * partition metadata to evaluate them. Use {@link #accept(InternalRow)} to evaluate this
 * predicate against a single partition's keys.
 * </p>
 *
 * @since 4.2.0
 */
@Evolving
public abstract class PartitionPredicate extends Predicate {

  /**
   * Default predicate name for partition predicates. Consistent with other predicate
   * expression names (e.g. {@code IS_NULL}, {@code AND}) which use upper case.
   */
  public static final String NAME = "PARTITION_PREDICATE";

  /**
   * @param name predicate name; subclasses should pass {@link #NAME}.
   * @param children child expressions; use {@link Expression#EMPTY_EXPRESSION} for implementations
   *                 that store the predicate in a form that does not expose children.
   */
  public PartitionPredicate(String name, Expression[] children) {
    super(name, children);
  }

  /**
   * {@inheritDoc}
   * <p>
   * For PartitionPredicate, returns {@link PartitionColumnReference} instances that identify
   * the partition columns (from {@link Table#partitioning()}) referenced by this predicate.
   * Each reference's {@link PartitionColumnReference#fieldNames()} gives the partition column
   * name; {@link PartitionColumnReference#ordinal()} gives the 0-based position, paralleling
   * {@link #referencedPartitionColumnOrdinals()}.
   *
   * @return array of partition column references (never null)
   */
  @Override
  public abstract NamedReference[] references();

  /**
   * Evaluates this predicate against a single partition's keys.
   *
   * @param partitionKey the full partition key for one partition, ordered according to
   *                     {@link Table#partitioning()}. Must include all partition columns,
   *                     not just those referenced by this predicate.
   * @return true if the partition represented by these keys satisfies this predicate.
   */
  public abstract boolean accept(InternalRow partitionKey);

  /**
   * Returns the ordinal position(s) of the partition transform(s) in
   * {@link Table#partitioning()} that are
   * referenced by this partition filter expression.
   *
   * <p><b>Example:</b> Suppose {@code Table.partitioning()} returns three partition
   * transforms: {@code [years(ts), months(ts), bucket(32, id)]} with ordinals 0, 1, 2.
   * <ul>
   *   <li>A filter expression {@code years(ts) = 2026} returns {@code [0]}.</li>
   *   <li>A filter expression {@code years(ts) = 2026 and months(ts) = 01}
   *       returns {@code [0, 1]}.</li>
   *   <li>A filter expression {@code bucket(32, id) = 1} returns {@code [2]}.</li>
   * </ul>
   * <p>
   * Data sources can use this to evaluate PartitionPredicates pushed down by
   * {@link SupportsPushDownV2Filters#pushPredicates(Predicate[])}
   * to determine whether the PartitionPredicate can be satisfied completely,
   * or whether it must be returned to Spark for post-scan filtering.
   * <p>
   * For example, data sources supporting partition spec evolution
   * should return PartitionPredicates that reference later-added partition
   * transforms (for which data in the table is incompletely partitioned)
   * to Spark for post-scan filter, while PartitionPredicates that reference initially-added
   * partition transforms (for which data in the table is completely partitioned) do not need
   * to be returned.
   * @return array of 0-based ordinal position(s) of the transform(s) in
   * {@link Table#partitioning()} referenced by this
   * PartitionPredicate's partition filter expression.
   */
  public abstract int[] referencedPartitionColumnOrdinals();
}
