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

package org.apache.spark.sql.connector

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Attribute, CreateNamedStruct, DynamicPruningExpression, Expression, GetStructFieldObject}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.{BufferedRows, InMemoryRowLevelOperationTable}
import org.apache.spark.sql.connector.expressions.LogicalExpressions.{identity, reference}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.InSubqueryExec
import org.apache.spark.sql.execution.ReusedSubqueryExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/**
 * Verifies that row-level runtime group filtering injects filters for scans that implement
 * [[org.apache.spark.sql.internal.connector.SupportsRuntimeCatalystFiltering]], where the filter
 * reaches the connector as a Catalyst expression instead of a connector predicate.
 *
 * The tests here apply to both group-based and delta-based row-level operations. DELETE is left
 * to the concrete suites because delta-based deletes only scan the row ID and the condition
 * columns, so the group key is not available to filter on.
 */
abstract class RowLevelOperationCatalystRuntimeFilterSuiteBase
  extends RowLevelOperationSuiteBase {

  import testImplicits._

  override protected def extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("use-catalyst-runtime-filtering", "true")
    props
  }

  test("update runtime group filtering with SupportsRuntimeCatalystFiltering") {
    withTempView("updated_id") {
      // the table is partitioned by dep, so hr and software are the two groups
      createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
        """{ "pk": 1, "id": 1, "salary": 300, "dep": "hr" }
          |{ "pk": 2, "id": 2, "salary": 150, "dep": "software" }
          |{ "pk": 3, "id": 3, "salary": 120, "dep": "hr" }
          |""".stripMargin)

      // the subquery blocks planning-time pushdown, leaving group filtering to do the pruning;
      // only id 1 matches, and it lives in hr
      val updatedIdDF = Seq(Some(1), None).toDF()
      updatedIdDF.createOrReplaceTempView("updated_id")

      val executedPlan = executeAndKeepPlan {
        sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE id IN (SELECT * FROM updated_id)")
      }
      assertCatalystGroupFilter(
        executedPlan,
        expectedFilterAttrs = Seq("dep"),
        expectedFilter = GroupFilter(scanSchema = "id INT, dep STRING", groups = Seq("hr")))

      // software was never read, so its rows must come back untouched
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, -1, "hr") :: Row(2, 2, 150, "software") :: Row(3, 3, 120, "hr") :: Nil)
    }
  }

  test("merge runtime group filtering with SupportsRuntimeCatalystFiltering") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
        """{ "pk": 1, "id": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "id": 2, "salary": 200, "dep": "hr" }
          |{ "pk": 3, "id": 3, "salary": 300, "dep": "hr" }
          |{ "pk": 4, "id": 4, "salary": 400, "dep": "software" }
          |{ "pk": 5, "id": 5, "salary": 500, "dep": "software" }
          |""".stripMargin)

      // pk 1 to 3 match rows in hr, pk 6 matches nothing and becomes an insert, so hr is the
      // only group that has to be rewritten
      val sourceDF = Seq(1, 2, 3, 6).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      val executedPlan = executeAndKeepPlan {
        sql(
          s"""MERGE INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | UPDATE SET t.salary = t.salary + 1
             |WHEN NOT MATCHED THEN
             | INSERT (pk, id, salary, dep) VALUES (s.pk, 0, 0, 'hr')
             |""".stripMargin)
      }
      assertCatalystGroupFilter(
        executedPlan,
        expectedFilterAttrs = Seq("dep"),
        expectedFilter = GroupFilter(scanSchema = "pk INT, dep STRING", groups = Seq("hr")))

      // software was never read, so its rows must come back untouched
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 1, 101, "hr"),
          Row(2, 2, 201, "hr"),
          Row(3, 3, 301, "hr"),
          Row(4, 4, 400, "software"),
          Row(5, 5, 500, "software"),
          Row(6, 0, 0, "hr")))
    }
  }

  test("merge runtime group filtering by a nested attribute") {
    withTempView("source") {
      val schema = "pk INT NOT NULL, id INT, salary INT, " +
        "dep STRUCT<name: STRING, region: STRING>"
      createTable(schema, Array[Transform](identity(reference(Seq("dep", "name")))))
      append(schema,
        """{"pk":1,"id":1,"salary":100,"dep":{"name":"hr","region":"west"}}
          |{"pk":2,"id":2,"salary":200,"dep":{"name":"hr","region":"east"}}
          |{"pk":3,"id":3,"salary":300,"dep":{"name":"software","region":"west"}}
          |""".stripMargin)

      Seq(1, 2).toDF("pk").createOrReplaceTempView("source")

      val executedPlan = executeAndKeepPlan {
        sql(
          s"""MERGE INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | UPDATE SET t.salary = t.salary + 1
             |""".stripMargin)
      }
      assertCatalystGroupFilter(
        executedPlan,
        expectedFilterAttrs = Seq("dep.name"),
        expectedFilter = GroupFilter(
          scanSchema = "pk INT, dep STRUCT<name: STRING>", groups = Seq("hr")),
        expectedFilterPaths = Some(Seq(Seq("dep", "name"))))

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, 101, Row("hr", "west")) ::
          Row(2, 2, 201, Row("hr", "east")) ::
          Row(3, 3, 300, Row("software", "west")) :: Nil)
    }
  }

  /**
   * Asserts the injected group filter down to its contents: the scan declares
   * `expectedFilterAttrs` in `filterAttributes`, every scan node carries one dynamic pruning
   * filter matching `expectedFilter`, the connector received that same filter as a Catalyst
   * expression, and the scan then read only `expectedFilter.groups`.
   *
   * A group-based UPDATE is rewritten as a union of two branches sharing one scan, so the plan
   * can hold more than one scan node. Each carries its own copy of the filter, keyed on that
   * branch's own attributes and with its own expr IDs, and pushes it separately (see the UPDATE
   * case in RowLevelOperationRuntimeGroupFiltering.buildMatchingRowsPlan). Every copy is checked
   * against `expectedFilter` rather than against one another, so the expectation stays explicit
   * and does not depend on how many copies the rewrite happens to produce.
   */
  protected def assertCatalystGroupFilter(
      executedPlan: SparkPlan,
      expectedFilterAttrs: Seq[String],
      expectedFilter: GroupFilter,
      expectedFilterPaths: Option[Seq[Seq[String]]] = None): Unit = {
    val batchScans = collect(executedPlan) { case s: BatchScanExec => s }
    assert(batchScans.nonEmpty, "expected a batch scan for the row-level operation")
    val scan = catalystScan(batchScans.head)
    assert(batchScans.forall(_.scan eq scan),
      s"expected all ${batchScans.size} scan nodes to share one scan")

    val filterAttrs = scan.filterAttributes().map(_.fieldNames.mkString(".")).toSeq
    assert(filterAttrs === expectedFilterAttrs,
      s"expected the scan to declare $expectedFilterAttrs as filter attributes, got $filterAttrs")
    val filterPaths = expectedFilterPaths.getOrElse(expectedFilterAttrs.map(Seq(_)))

    batchScans.foreach { batchScan =>
      batchScan.runtimeFilters match {
        case Seq(DynamicPruningExpression(inSubquery: InSubqueryExec)) =>
          assertGroupFilter(inSubquery, filterPaths, expectedFilter)
        case other => fail(s"expected a single dynamic pruning group filter, got $other")
      }
    }

    // the scan must receive the Catalyst subquery Spark planned, not a translated connector
    // predicate, once per scan node
    val pushed = scan.pushedCatalystPredicates
    assert(pushed.size === batchScans.size,
      s"expected each of the ${batchScans.size} scan node(s) to push the filter once, got $pushed")
    pushed.foreach {
      case inSubquery: InSubqueryExec =>
        assertGroupFilter(inSubquery, filterPaths, expectedFilter)
      case other =>
        fail(s"expected the group filter pushed as an InSubqueryExec, got $other")
    }

    val scannedGroups = scan.data.map(_.asInstanceOf[BufferedRows].keyString()).distinct
    assert(scannedGroups.sorted === expectedFilter.groups.sorted,
      s"scan must read only the filtered groups, got ${scannedGroups.mkString(", ")}")
  }

  /**
   * The expected shape of a group filter subquery: the columns it reads, which must be only those
   * needed to evaluate the row-level condition, and the groups it resolves to at runtime.
   */
  protected case class GroupFilter(scanSchema: String, groups: Seq[String])

  private def assertGroupFilter(
      filter: InSubqueryExec,
      expectedFilterPaths: Seq[Seq[String]],
      expectedFilter: GroupFilter): Unit = {
    assert(fieldPaths(filter.child).contains(expectedFilterPaths),
      s"expected the group filter keyed on ${expectedFilterPaths.map(_.mkString("."))}, " +
        s"got ${filter.child}")

    // the second branch of a group-based UPDATE reuses the first branch's subquery, and
    // ReusedSubqueryExec is a leaf node, so unwrap it to reach the plan underneath
    val subqueryPlan = filter.plan match {
      case reused: ReusedSubqueryExec => reused.child
      case plan => plan
    }
    val subqueryScan = find(subqueryPlan) { case _: BatchScanExec => true; case _ => false }
      .getOrElse(fail(s"could not find the scan of group filter subquery ${filter.plan.name}"))
    assert(
      DataTypeUtils.sameType(subqueryScan.schema, StructType.fromDDL(expectedFilter.scanSchema)),
      s"unexpected group filter subquery scan schema ${subqueryScan.schema.sql}")

    val groups = filter.values()
      .getOrElse(fail("group filter subquery produced no values"))
      .map(_.asInstanceOf[UTF8String].toString)
    assert(groups.toSeq.sorted === expectedFilter.groups.sorted,
      s"group filter must select the groups holding matching rows, got ${groups.mkString(", ")}")
  }

  private def fieldPath(expr: Expression): Option[Seq[String]] = expr match {
    case attr: Attribute => Some(Seq(attr.name))
    case GetStructFieldObject(child, field) => fieldPath(child).map(_ :+ field.name)
    case _ => None
  }

  private def fieldPaths(expr: Expression): Option[Seq[Seq[String]]] = expr match {
    case struct: CreateNamedStruct =>
      val paths = struct.valExprs.map(fieldPath)
      Option.when(paths.forall(_.isDefined))(paths.flatten)
    case _ => fieldPath(expr).map(Seq(_))
  }

  /** Asserts no group filter was injected, e.g. because the scan does not read the group key. */
  protected def assertNoCatalystGroupFilter(executedPlan: SparkPlan): Unit = {
    val batchScan = collect(executedPlan) { case s: BatchScanExec => s }.head
    val scan = catalystScan(batchScan)
    assert(scan.filterAttributes().isEmpty,
      s"expected no filter attributes, got ${scan.filterAttributes().mkString(", ")}")
    assert(batchScan.runtimeFilters.isEmpty,
      s"expected no runtime filters, got ${batchScan.runtimeFilters}")
    assert(scan.pushedCatalystPredicates.isEmpty,
      s"expected no pushed predicates, got ${scan.pushedCatalystPredicates}")
  }

  private type CatalystRowLevelScan =
    InMemoryRowLevelOperationTable#InMemoryCatalystRowLevelBatchScan

  private def catalystScan(batchScan: BatchScanExec): CatalystRowLevelScan = {
    batchScan.scan match {
      case s: InMemoryRowLevelOperationTable#InMemoryCatalystRowLevelBatchScan => s
      case other => fail(s"expected InMemoryCatalystRowLevelBatchScan, got ${other.getClass}")
    }
  }
}
