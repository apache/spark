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

package org.apache.spark.sql.execution.columnar

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkFunSuite
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSessionBase
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

class InMemoryRelationSuite extends SparkFunSuite
  with SharedSparkSessionBase with AdaptiveSparkPlanHelper {

  test("SPARK-46779: InMemoryRelations with the same cached plan are semantically equivalent") {
    val d = spark.range(1)
    val r1 = InMemoryRelation(StorageLevel.MEMORY_ONLY, d.queryExecution, None)
    val r2 = r1.withOutput(r1.output.map(_.newInstance()))
    assert(r1.sameResult(r2))
  }

  test("SPARK-59009: newInstance() re-maps outputOrdering onto the new attributes") {
    val d = spark.range(10).selectExpr("id", "id % 3 AS k").orderBy("k", "id")
    val r1 = InMemoryRelation(StorageLevel.MEMORY_ONLY, d.queryExecution, None)
    assert(r1.outputOrdering.nonEmpty)

    val r2 = r1.newInstance()
    assert(r2.output.map(_.exprId) != r1.output.map(_.exprId))
    // The ordering must be re-mapped onto the new attributes, not left referencing the old ones.
    assert(r2.outputOrdering.nonEmpty)
    assert(AttributeSet(r2.outputOrdering.flatMap(_.references)).subsetOf(AttributeSet(r2.output)))
    // `sameResult` is what CacheManager lookups and exchange reuse rely on. It goes through
    // `doCanonicalize`, which re-maps `outputOrdering` through `output`, so a stale ordering
    // throws here rather than merely producing an unequal plan.
    assert(r1.sameResult(r2))
  }

  test("SPARK-59024: plan id cached name for anonymous cached tables") {
    val d = spark.range(1)
    withSQLConf(SQLConf.DATAFRAME_CACHE_PLAN_ID_NAME_ENABLED.key -> "true") {
      val r1 = InMemoryRelation(StorageLevel.MEMORY_ONLY, d.queryExecution, None)
      // Caches of the same physical plan instance share the plan id.
      val r1Again = InMemoryRelation(StorageLevel.MEMORY_ONLY, d.queryExecution, None)
      val r2 = InMemoryRelation(StorageLevel.MEMORY_ONLY, spark.range(2).queryExecution, None)
      assert(r1.cacheBuilder.cachedName.matches("CachedRDD \\(plan_id=\\d+\\)"))
      assert(r1Again.cacheBuilder.cachedName == r1.cacheBuilder.cachedName)
      assert(r1.cacheBuilder.cachedName != r2.cacheBuilder.cachedName)
      // Named tables keep the usual name.
      val r3 = InMemoryRelation(StorageLevel.MEMORY_ONLY, d.queryExecution, Some("t1"))
      assert(r3.cacheBuilder.cachedName == "In-memory table t1")
    }
    // When disabled, the cached name keeps the abbreviated plan tree string.
    withSQLConf(SQLConf.DATAFRAME_CACHE_PLAN_ID_NAME_ENABLED.key -> "false") {
      val r4 = InMemoryRelation(StorageLevel.MEMORY_ONLY, d.queryExecution, None)
      assert(r4.cacheBuilder.cachedName ==
        Utils.abbreviate(r4.cacheBuilder.cachedPlan.toString, 1024))
    }
  }

  test("SPARK-59024: anonymous cached name is not rendered before materialization") {
    val plan = ToStringCountingPlan()
    val relation = InMemoryRelation(new DefaultCachedBatchSerializer, StorageLevel.MEMORY_ONLY,
      plan, None, spark.range(1).queryExecution.optimizedPlan)
    assert(plan.toStringCount == 0)
    // Forcing the name renders the tree string exactly once.
    relation.cacheBuilder.cachedName
    assert(plan.toStringCount == 1)
  }

  test("SPARK-47177: Cached SQL plan do not display final AQE plan in explain string") {
    def findIMRInnerChild(p: SparkPlan): SparkPlan = {
      val tableCache = find(p) {
        case _: InMemoryTableScanExec => true
        case _ => false
      }
      assert(tableCache.isDefined)
      tableCache.get.asInstanceOf[InMemoryTableScanExec].relation.innerChildren.head
    }

    val d1 = spark.range(1).withColumn("key", expr("id % 100"))
      .groupBy("key").agg(Map("key" -> "count"))
    val cached_d2 = d1.cache()
    val df = cached_d2.withColumn("key2", expr("key % 10"))
      .groupBy("key2").agg(Map("key2" -> "count"))

    assert(findIMRInnerChild(df.queryExecution.executedPlan).treeString
      .contains("AdaptiveSparkPlan isFinalPlan=false"))
    df.collect()
    assert(findIMRInnerChild(df.queryExecution.executedPlan).treeString
      .contains("AdaptiveSparkPlan isFinalPlan=true"))
  }
}

case class ToStringCountingPlan() extends LeafExecNode {
  private val _toStringCount = new AtomicInteger(0)

  def toStringCount: Int = _toStringCount.get()

  override def output: Seq[Attribute] = Seq.empty

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException

  override def toString: String = {
    _toStringCount.incrementAndGet()
    "ToStringCountingPlan"
  }
}
