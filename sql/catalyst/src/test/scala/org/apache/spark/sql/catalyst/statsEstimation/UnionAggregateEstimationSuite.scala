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

package org.apache.spark.sql.catalyst.statsEstimation

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Union}
import org.apache.spark.sql.types.IntegerType

/**
 * Verifies that UnionEstimation propagates distinctCount, enabling
 * AggregateEstimation to produce accurate CBO estimates for
 * `SELECT ... GROUP BY ... FROM (... UNION ALL ...)` queries.
 */
class UnionAggregateEstimationSuite extends StatsEstimationTestBase {

  private val colKey = AttributeReference("key", IntegerType)()
  private val colVal = AttributeReference("val", IntegerType)()

  private def makeChild(
      keyAttr: AttributeReference,
      valAttr: AttributeReference,
      rows: Int,
      distinctKeys: Int): StatsTestPlan = {
    StatsTestPlan(
      outputList = Seq(keyAttr, valAttr),
      rowCount = rows,
      attributeStats = AttributeMap(Seq(
        keyAttr -> ColumnStat(
          distinctCount = Some(distinctKeys),
          min = Some(1),
          max = Some(distinctKeys),
          nullCount = Some(0),
          avgLen = Some(4),
          maxLen = Some(4)),
        valAttr -> ColumnStat(
          distinctCount = Some(rows),
          min = Some(1),
          max = Some(rows),
          nullCount = Some(0),
          avgLen = Some(4),
          maxLen = Some(4))
      )),
      size = Some(rows.toLong * 8)
    )
  }

  test("SPARK-56047: Aggregate directly on a single child retains CBO distinctCount") {
    // Aggregate(child) -- no Union involved, CBO should work fine
    val child = makeChild(colKey, colVal, rows = 1000000, distinctKeys = 100)
    val agg = child.groupBy(child.output.head)(count(child.output(1)).as("cnt"))
    val aggStats = agg.stats

    // AggregateEstimation should succeed: rowCount = distinctKeys = 100
    assert(aggStats.rowCount.isDefined, "rowCount should be defined (CBO succeeded)")
    assert(aggStats.rowCount.get == 100,
      s"Expected rowCount=100, got ${aggStats.rowCount.get}")
  }

  test("SPARK-56047: Union propagates distinctCount, enabling AggregateEstimation CBO") {
    val key1 = AttributeReference("key1", IntegerType)()
    val val1 = AttributeReference("val1", IntegerType)()
    val key2 = AttributeReference("key2", IntegerType)()
    val val2 = AttributeReference("val2", IntegerType)()

    val child1 = makeChild(key1, val1, rows = 500000, distinctKeys = 100)
    val child2 = makeChild(key2, val2, rows = 500000, distinctKeys = 100)

    val union = Union(Seq(child1, child2))

    // Verify: Union stats should have rowCount AND distinctCount
    val unionStats = union.stats
    assert(unionStats.rowCount.isDefined, "Union rowCount should be defined")
    assert(unionStats.rowCount.get == 1000000,
      s"Union rowCount should be 1M, got ${unionStats.rowCount.get}")

    val unionKeyAttr = union.output.head
    val unionKeyStat = unionStats.attributeStats.get(unionKeyAttr)
    assert(unionKeyStat.isDefined, "Union should have attributeStats for key column")
    assert(unionKeyStat.get.distinctCount.isDefined,
      "Union should have distinctCount (propagated as max across children)")
    assert(unionKeyStat.get.distinctCount.get == 100,
      s"distinctCount should be max(100, 100) = 100, got ${unionKeyStat.get.distinctCount.get}")
    assert(unionKeyStat.get.hasCountStats,
      "hasCountStats should be true since both distinctCount and nullCount are defined")

    // Now: Aggregate on top of Union -- CBO should work
    val agg = union.groupBy(unionKeyAttr)(count(union.output(1)).as("cnt"))
    val aggStats = agg.stats

    assert(aggStats.rowCount.isDefined,
      "AggregateEstimation CBO should succeed (rowCount defined)")
    assert(aggStats.rowCount.get == 100,
      s"Expected rowCount=100, got ${aggStats.rowCount.get}")
    // With CBO, sizeInBytes should be small (100 rows), not millions of rows
    assert(aggStats.sizeInBytes < 10000,
      s"With CBO, aggregate sizeInBytes should be small, got ${aggStats.sizeInBytes}")
  }

  test("SPARK-56047: Same Aggregate without Union retains accurate CBO estimate") {
    // Control group: same data volume but as a single child, no Union
    val child = makeChild(colKey, colVal, rows = 1000000, distinctKeys = 100)
    val agg = child.groupBy(child.output.head)(count(child.output(1)).as("cnt"))
    val aggStats = agg.stats

    // CBO works: rowCount = 100, sizeInBytes is small
    assert(aggStats.rowCount.isDefined && aggStats.rowCount.get == 100)
    assert(aggStats.sizeInBytes < 10000,
      s"With CBO, aggregate sizeInBytes should be small, got ${aggStats.sizeInBytes}")
  }
}
