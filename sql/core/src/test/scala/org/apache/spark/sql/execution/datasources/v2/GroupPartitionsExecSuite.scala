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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{KeyedPartitioning, PartitioningCollection}
import org.apache.spark.sql.execution.DummySparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

class GroupPartitionsExecSuite extends SharedSparkSession {

  private val exprA = AttributeReference("a", IntegerType)()
  private val exprB = AttributeReference("b", IntegerType)()
  private val exprC = AttributeReference("c", IntegerType)()

  private def row(a: Int): InternalRow = InternalRow.fromSeq(Seq(a))
  private def row(a: Int, b: Int): InternalRow = InternalRow.fromSeq(Seq(a, b))

  test("SPARK-56241: non-coalescing passes through child ordering unchanged") {
    // Each partition has a distinct key — no coalescing happens.
    val partitionKeys = Seq(row(1), row(2), row(3))
    val childOrdering = Seq(SortOrder(exprA, Ascending))
    val child = DummySparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA), partitionKeys),
      outputOrdering = childOrdering)
    val gpe = GroupPartitionsExec(child)

    assert(gpe.groupedPartitions.forall(_._2.size <= 1), "expected non-coalescing")
    assert(gpe.outputOrdering === childOrdering)
  }

  test("SPARK-56241: coalescing without reducers keeps key-expression orders from child") {
    // Key 1 appears on partitions 0 and 2, causing coalescing.
    val partitionKeys = Seq(row(1), row(2), row(1))
    val child = DummySparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA), partitionKeys),
      outputOrdering = Seq(SortOrder(exprA, Ascending)))
    val gpe = GroupPartitionsExec(child)

    assert(!gpe.groupedPartitions.forall(_._2.size <= 1), "expected coalescing")
    // With the config disabled (default), key-expression filtering is skipped.
    assert(gpe.outputOrdering === Nil)
    // When enabled, the key-expression order is preserved through coalescing.
    withSQLConf(SQLConf.V2_BUCKETING_PRESERVE_KEY_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      val ordering = gpe.outputOrdering
      assert(ordering.length === 1)
      assert(ordering.head.child === exprA)
      assert(ordering.head.direction === Ascending)
      assert(ordering.head.sameOrderExpressions.isEmpty)
    }
  }

  test("SPARK-56241: coalescing without reducers keeps one SortOrder per key expression") {
    // Multi-key partition: key (1,10) appears on partitions 0 and 2, causing coalescing.
    val partitionKeys = Seq(row(1, 10), row(2, 20), row(1, 10))
    val child = DummySparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA, exprB), partitionKeys),
      outputOrdering = Seq(SortOrder(exprA, Ascending), SortOrder(exprB, Ascending)))
    val gpe = GroupPartitionsExec(child)

    assert(!gpe.groupedPartitions.forall(_._2.size <= 1), "expected coalescing")
    assert(gpe.outputOrdering === Nil)
    withSQLConf(SQLConf.V2_BUCKETING_PRESERVE_KEY_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      val ordering = gpe.outputOrdering
      assert(ordering.length === 2)
      assert(ordering.head.child === exprA)
      assert(ordering(1).child === exprB)
      assert(ordering.head.sameOrderExpressions.isEmpty)
      assert(ordering(1).sameOrderExpressions.isEmpty)
    }
  }

  test("SPARK-56241: coalescing join case preserves sameOrderExpressions from child") {
    // PartitioningCollection wraps two KeyedPartitionings (one per join side), sharing the same
    // partition keys. Key 1 coalesces partitions 0 and 2. The child (e.g. SortMergeJoinExec)
    // already carries sameOrderExpressions linking both sides' key expressions.
    val partitionKeys = Seq(row(1), row(2), row(1))
    val leftKP = KeyedPartitioning(Seq(exprA), partitionKeys)
    val rightKP = KeyedPartitioning(Seq(exprB), partitionKeys)
    val child = DummySparkPlan(
      outputPartitioning = PartitioningCollection(Seq(leftKP, rightKP)),
      outputOrdering = Seq(SortOrder(exprA, Ascending, sameOrderExpressions = Seq(exprB))))
    val gpe = GroupPartitionsExec(child)

    assert(!gpe.groupedPartitions.forall(_._2.size <= 1), "expected coalescing")
    assert(gpe.outputOrdering === Nil)
    withSQLConf(SQLConf.V2_BUCKETING_PRESERVE_KEY_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      val ordering = gpe.outputOrdering
      assert(ordering.length === 1)
      assert(ordering.head.child === exprA)
      assert(ordering.head.sameOrderExpressions === Seq(exprB))
    }
  }

  test("SPARK-56241: coalescing drops non-key sort orders from child") {
    // exprA is the partition key; exprC is a non-key sort order the child also reports
    // (e.g. a secondary sort within each partition). After coalescing, exprC ordering is lost
    // by concatenation, so only the exprA order should survive.
    val partitionKeys = Seq(row(1), row(2), row(1))
    val child = DummySparkPlan(
      outputPartitioning = KeyedPartitioning(Seq(exprA), partitionKeys),
      outputOrdering = Seq(SortOrder(exprA, Ascending), SortOrder(exprC, Ascending)))
    val gpe = GroupPartitionsExec(child)

    assert(!gpe.groupedPartitions.forall(_._2.size <= 1), "expected coalescing")
    assert(gpe.outputOrdering === Nil)
    withSQLConf(SQLConf.V2_BUCKETING_PRESERVE_KEY_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      val ordering = gpe.outputOrdering
      assert(ordering.length === 1)
      assert(ordering.head.child === exprA)
    }
  }

  test("SPARK-56241: coalescing with reducers returns empty ordering") {
    // When reducers are present, the original key expressions are not constant within the merged
    // partition, so outputOrdering falls back to the default (empty).
    val partitionKeys = Seq(row(1), row(2), row(1))
    val child = DummySparkPlan(outputPartitioning = KeyedPartitioning(Seq(exprA), partitionKeys))
    // reducers = Some(Seq(None)) - None element means identity reducer; the important thing is
    // that reducers.isDefined, which triggers the fallback.
    val gpe = GroupPartitionsExec(child, reducers = Some(Seq(None)))

    assert(!gpe.groupedPartitions.forall(_._2.size <= 1), "expected coalescing")
    assert(gpe.outputOrdering === Nil)
  }
}
