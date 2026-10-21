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

import java.util
import java.util.OptionalLong

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.{GreaterThan, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, Subquery}
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryTable, InMemoryTableCatalog, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{Batch, Scan, ScanBuilder, ScanBuilderContext, Statistics, SupportsPushDownAggregates, SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsReportStatistics}
import org.apache.spark.sql.sources
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class V2ScanRelationPushDownSubquerySuite extends QueryTest with SharedSparkSession {
  import SubqueryScanTestCatalog.calls

  override protected def sparkConf: SparkConf = super.sparkConf.set(
    "spark.sql.catalog.subquery_stats", classOf[SubqueryScanTestCatalog].getName)

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    calls.clear()
  }

  private def newRelation(
      options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty): DataSourceV2Relation = {
    val schema = new StructType().add("id", IntegerType).add("value", IntegerType)
    val table = new InMemoryTable("test", schema, Array.empty, util.Collections.emptyMap())
    DataSourceV2Relation.create(new SubqueryScanTestTable(table), None, None, options)
  }

  private def optimize(query: String): LogicalPlan = sql(query).queryExecution.optimizedPlan

  private def assertContexts(expected: Set[Boolean]): Unit = {
    assert(calls.nonEmpty)
    assert(calls.forall(_.context.isDefined))
    assert(calls.map(_.context.get.isInSubquery()).toSet == expected)
    calls.indices.foreach { i =>
      (0 until i).foreach { j => assert(calls(i).context.get ne calls(j).context.get) }
    }
  }

  test("the context overload delegates to an existing options-only implementation") {
    val options = new CaseInsensitiveStringMap(Map("MiXeD" -> "value").asJava)
    var receivedOptions: CaseInsensitiveStringMap = null
    val table: SupportsRead = new InMemoryTable(
        "legacy", new StructType().add("id", IntegerType), Array.empty,
        util.Collections.emptyMap()) {
      override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        receivedOptions = options
        super.newScanBuilder(options)
      }
    }
    table.newScanBuilder(new ScanBuilderContext(true), options).build()
    assert(receivedOptions eq options)
    assert(receivedOptions.get("mixed") == "value")
  }

  test("direct options-only calls leave subquery context unspecified") {
    val relation = newRelation()
    val table = relation.table.asInstanceOf[SupportsRead]
    table.newScanBuilder(relation.options).build()
    assert(calls.size == 1)
    assert(calls.head.context.isEmpty)
    assert(calls.head.options eq relation.options)
  }

  for (inSubquery <- Seq(false, true)) {
    test(s"builder context is available before pushdown: inSubquery=$inSubquery") {
      val input = Map("MiXeD" -> "value", "isInSubquery" -> "user option")
      val options = new CaseInsensitiveStringMap(input.asJava)
      val relation = newRelation(options)
      val plan = Project(Seq(relation.output.head),
        Filter(GreaterThan(relation.output.head, Literal(0)), relation))
      val result = V2ScanRelationPushDown(
        if (inSubquery) Subquery(plan, correlated = false) else plan)

      assertContexts(Set(inSubquery))
      val call = calls.head
      assert(call.options eq options)
      assert(options.asCaseSensitiveMap().asScala.toMap == input)
      assert(call.events.head == "select statistics")
      assert(call.events.contains("filters"))
      assert(call.events.contains("prune"))
      assert(call.requiredSchema.fieldNames.toSeq == Seq("id"))
      val scan = result.collectFirst { case r: DataSourceV2ScanRelation => r.scan }.get
      val stats = scan.asInstanceOf[SupportsReportStatistics].estimateStatistics()
      assert(stats.sizeInBytes().getAsLong == (if (inSubquery) 10L else 100L))
    }
  }

  test("each planning pass receives a fresh context even when it reuses a relation") {
    val relation = newRelation()
    V2ScanRelationPushDown(relation)
    V2ScanRelationPushDown(Subquery(relation, correlated = false))
    V2ScanRelationPushDown(relation)
    assert(calls.map(_.context.get.isInSubquery()).toSeq == Seq(false, true, false))
    assertContexts(Set(false, true))
    assert(calls.forall(_.options eq relation.options))
  }

  test("rebuilding a scan uses its destination context") {
    val relation = newRelation()
    V2ScanRelationPushDown(Subquery(relation, correlated = false))
    for (inSubquery <- Seq(false, true)) {
      val rebuilt = V2ScanRelationPushDown.rebuildScan(
        relation, Seq(relation.output.head), Nil, isSubquery = inSubquery)
      assert(rebuilt.isDefined)
      assert(calls.last.context.get.isInSubquery() == inSubquery)
      assert(calls.last.options eq relation.options)
    }
    assertContexts(Set(false, true))
  }

  test("scalar, IN and EXISTS subqueries provide context during SQL optimization") {
    withTable("subquery_stats.t") {
      sql("CREATE TABLE subquery_stats.t (id INT, value INT) USING foo")
      Seq(
        "SELECT (SELECT max(id) FROM subquery_stats.t)",
        "SELECT 1 WHERE 1 IN (SELECT id FROM subquery_stats.t)",
        "SELECT 1 WHERE EXISTS (SELECT id FROM subquery_stats.t)").foreach { query =>
        calls.clear()
        optimize(query)
        assertContexts(Set(true))
      }

      calls.clear()
      optimize("SELECT max(id) FROM subquery_stats.t")
      assertContexts(Set(false))
      assert(calls.exists(_.events.contains("aggregate")))

      calls.clear()
      optimize("SELECT (SELECT max(id) FROM subquery_stats.t)")
      assertContexts(Set(true))
      assert(calls.exists(_.events.contains("aggregate")))
    }
  }

  test("the same table can be planned in the main query and a scalar subquery") {
    withTable("subquery_stats.t") {
      sql("CREATE TABLE subquery_stats.t (id INT, value INT) USING foo")
      optimize("SELECT id, (SELECT max(id) FROM subquery_stats.t) FROM subquery_stats.t")
      assertContexts(Set(false, true))
    }
  }

  for (global <- Seq(false, true)) {
    test(s"temporary view reuse preserves the planning context: global=$global") {
      val viewName = "subquery_stats_view"
      val qualifiedView = if (global) s"global_temp.$viewName" else viewName
      def withView(f: => Unit): Unit = {
        if (global) withGlobalTempView(viewName)(f) else withTempView(viewName)(f)
      }
      withTable("subquery_stats.t") {
        withView {
          sql("CREATE TABLE subquery_stats.t (id INT, value INT) USING foo")
          val df = sql("SELECT id, (SELECT max(id) FROM subquery_stats.t) AS maximum " +
            "FROM subquery_stats.t")
          df.queryExecution.optimizedPlan
          assertContexts(Set(false, true))
          if (global) df.createOrReplaceGlobalTempView(viewName)
          else df.createOrReplaceTempView(viewName)

          calls.clear()
          optimize(s"SELECT * FROM $qualifiedView")
          assertContexts(Set(false, true))

          calls.clear()
          optimize(s"SELECT (SELECT max(id) FROM $qualifiedView)")
          assertContexts(Set(true))

          calls.clear()
          optimize(s"SELECT * FROM $qualifiedView")
          assertContexts(Set(false, true))

          val replacement = sql("SELECT id FROM subquery_stats.t")
          if (global) replacement.createOrReplaceGlobalTempView(viewName)
          else replacement.createOrReplaceTempView(viewName)
          calls.clear()
          optimize(s"SELECT * FROM $qualifiedView")
          assertContexts(Set(false))
        }
      }
    }
  }
}

class SubqueryScanTestCatalog extends InMemoryTableCatalog {
  override def loadTable(ident: Identifier): Table = {
    new SubqueryScanTestTable(super.loadTable(ident).asInstanceOf[InMemoryTable])
  }
}

object SubqueryScanTestCatalog {
  val calls: ArrayBuffer[SubqueryScanTestCall] = ArrayBuffer.empty
}

class SubqueryScanTestCall(
    val context: Option[ScanBuilderContext],
    val options: CaseInsensitiveStringMap) {
  val events: ArrayBuffer[String] = ArrayBuffer("select statistics")
  val sizeInBytes: Long = if (context.exists(_.isInSubquery())) 10L else 100L
  var requiredSchema: StructType = new StructType()
}

class SubqueryScanTestTable(table: InMemoryTable) extends Table with SupportsRead {
  override def name(): String = table.name
  override def schema(): StructType = table.schema()
  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(TableCapability.BATCH_READ)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    newBuilder(None, options)
  }

  override def newScanBuilder(
      context: ScanBuilderContext,
      options: CaseInsensitiveStringMap): ScanBuilder = {
    newBuilder(Some(context), options)
  }

  private def newBuilder(
      context: Option[ScanBuilderContext],
      options: CaseInsensitiveStringMap): ScanBuilder = {
    val call = new SubqueryScanTestCall(context, options)
    SubqueryScanTestCatalog.calls += call
    val delegate = table.newScanBuilder(options)
    new ScanBuilder with SupportsPushDownRequiredColumns with SupportsPushDownFilters
        with SupportsPushDownAggregates {
      override def pruneColumns(requiredSchema: StructType): Unit = {
        call.events += "prune"
        call.requiredSchema = requiredSchema
        delegate.asInstanceOf[SupportsPushDownRequiredColumns].pruneColumns(requiredSchema)
      }

      override def pushFilters(filters: Array[sources.Filter]): Array[sources.Filter] = {
        call.events += "filters"
        delegate.asInstanceOf[SupportsPushDownFilters].pushFilters(filters)
      }

      override def pushedFilters(): Array[sources.Filter] = {
        delegate.asInstanceOf[SupportsPushDownFilters].pushedFilters()
      }

      override def pushAggregation(aggregation: Aggregation): Boolean = {
        call.events += "aggregate"
        false
      }

      override def build(): Scan = {
        call.events += "build"
        val scan = delegate.build()
        new Scan with SupportsReportStatistics {
          override def readSchema(): StructType = scan.readSchema()
          override def toBatch: Batch = scan.toBatch
          override def estimateStatistics(): Statistics = new Statistics {
            override def sizeInBytes(): OptionalLong = OptionalLong.of(call.sizeInBytes)
            override def numRows(): OptionalLong = OptionalLong.empty()
          }
        }
      }
    }
  }
}
