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
import org.apache.spark.sql.catalyst.plans.logical.{RebalancePartitions, RepartitionByExpression, Sort, WriteDelta}
import org.apache.spark.sql.catalyst.util.METADATA_COL_ATTR_KEY
import org.apache.spark.sql.connector.catalog.CatalogV2Util
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, V2Writes}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

abstract class DeltaBasedNestedRowIdTableSuiteBase(splitUpdates: Boolean)
  extends RowLevelOperationSuiteBase {

  import testImplicits._

  override protected def extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("supports-deltas", "true")
    props.put("nested-row-id", "true")
    if (splitUpdates) props.put("split-updates", "true")
    props
  }

  // the nested row ID pk shares its leaf name with the top-level pk data column, so a name-based
  // bind would confuse the two; the suite checks that the nested field is used
  private val tableSchema = StructType(Seq(
    StructField("pk", IntegerType, nullable = false),
    StructField("nested", StructType(Seq(
      StructField("pk", IntegerType, nullable = false))),
      nullable = false),
    StructField("id", IntegerType),
    StructField("dep", StringType)))

  // use different top-level and nested PK values to expose name-based binding
  private val initialRows =
    """{ "pk": 10, "nested": { "pk": 1 }, "id": 1, "dep": "hr" }
      |{ "pk": 20, "nested": { "pk": 2 }, "id": 2, "dep": "software" }
      |{ "pk": 30, "nested": { "pk": 3 }, "id": 3, "dep": "hr" }
      |""".stripMargin

  private def createNestedRowIdTable(): Unit = {
    createTable(CatalogV2Util.structTypeToV2Columns(tableSchema))
    append(tableSchema.toDDL, initialRows)
  }

  private def checkTable(expected: Seq[Row]): Unit = {
    checkAnswer(
      sql(s"SELECT pk, nested.pk, id, dep FROM $tableNameAsString ORDER BY pk"),
      expected)
  }

  test("delete with nested row id") {
    createNestedRowIdTable()
    // use a multi-value IN to force the row-level delta path instead of metadata-only deleteWhere
    sql(s"DELETE FROM $tableNameAsString WHERE id IN (1, 100)")
    checkTable(Seq(
      Row(20, 2, 2, "software"),
      Row(30, 3, 3, "hr")))
    // the row id written to the delta is the nested pk (1), not the top-level pk (10)
    checkLastWriteInfo(
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))
    checkLastWriteLog(deleteWriteLogEntry(id = 1, metadata = Row("hr", null)))
  }

  test("update with nested row id") {
    createNestedRowIdTable()
    sql(s"UPDATE $tableNameAsString SET dep = 'it' WHERE id = 1")
    checkTable(Seq(
      Row(10, 1, 1, "it"),
      Row(20, 2, 2, "software"),
      Row(30, 3, 3, "hr")))
    // the row id written to the delta is the nested pk (1), not the top-level pk (10)
    if (splitUpdates) {
      checkLastWriteLog(
        deleteWriteLogEntry(id = 1, metadata = Row("hr", null)),
        reinsertWriteLogEntry(metadata = Row("hr", null), data = Row(10, Row(1), 1, "it")))
    } else {
      checkLastWriteLog(
        updateWriteLogEntry(id = 1, metadata = Row("hr", null), data = Row(10, Row(1), 1, "it")))
    }
  }

  test("write distribution and ordering use the nested row id") {
    createNestedRowIdTable()

    val command = sql(s"UPDATE $tableNameAsString SET dep = 'it' WHERE id = 1")
    val writeDelta = V2Writes(command.queryExecution.analyzed).collectFirst {
      case write: WriteDelta => write
    }.getOrElse(fail("Cannot find WriteDelta"))
    val rowIdAttr = writeDelta.query.output(
      writeDelta.projections.rowIdProjection.colOrdinals.head)

    val distributionAttr = writeDelta.query.collectFirst {
      case repartition: RepartitionByExpression =>
        repartition.partitionExpressions.flatMap(_.references).head
      case rebalance: RebalancePartitions =>
        rebalance.partitionExpressions.flatMap(_.references).head
    }.getOrElse(fail("Cannot find required distribution"))
    val orderingAttr = writeDelta.query.collectFirst {
      case sort: Sort => sort.order.flatMap(_.references).head
    }.getOrElse(fail("Cannot find required ordering"))

    assert(distributionAttr.exprId == rowIdAttr.exprId)
    assert(orderingAttr.exprId == rowIdAttr.exprId)
  }

  test("update replacing the struct that holds the nested row id") {
    createNestedRowIdTable()
    sql(
      s"""UPDATE $tableNameAsString
         |SET nested = named_struct('pk', 11)
         |WHERE id = 1
         |""".stripMargin)
    checkTable(Seq(
      Row(10, 11, 1, "hr"),
      Row(20, 2, 2, "software"),
      Row(30, 3, 3, "hr")))
  }

  test("merge update with nested row id") {
    withTempView("source") {
      createNestedRowIdTable()
      Seq((1, "it")).toDF("id", "dep").createOrReplaceTempView("source")
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET t.dep = s.dep
           |""".stripMargin)
      checkTable(Seq(
        Row(10, 1, 1, "it"),
        Row(20, 2, 2, "software"),
        Row(30, 3, 3, "hr")))
    }
  }

  test("merge delete with nested row id") {
    withTempView("source") {
      createNestedRowIdTable()
      Seq(2).toDF("id").createOrReplaceTempView("source")
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id
           |WHEN MATCHED THEN DELETE
           |""".stripMargin)
      checkTable(Seq(
        Row(10, 1, 1, "hr"),
        Row(30, 3, 3, "hr")))
    }
  }

  test("merge not matched by source update with nested row id") {
    withTempView("source") {
      createNestedRowIdTable()
      Seq(1).toDF("id").createOrReplaceTempView("source")
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id
           |WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.dep = 'gone'
           |""".stripMargin)
      checkTable(Seq(
        Row(10, 1, 1, "hr"),
        Row(20, 2, 2, "gone"),
        Row(30, 3, 3, "gone")))
    }
  }

  test("merge with matched update and not-matched insert with nested row id") {
    withTempView("source") {
      createNestedRowIdTable()
      Seq((1, "it"), (4, "new")).toDF("id", "dep").createOrReplaceTempView("source")
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET t.dep = s.dep
           |WHEN NOT MATCHED THEN
           |INSERT (pk, nested, id, dep) VALUES (s.id * 10, named_struct('pk', s.id), s.id, s.dep)
           |""".stripMargin)
      checkTable(Seq(
        Row(10, 1, 1, "it"),
        Row(20, 2, 2, "software"),
        Row(30, 3, 3, "hr"),
        Row(40, 4, 4, "new")))
    }
  }

  // A data column `index` collides with the in-memory table's `index` metadata column: the row
  // level rewrite must still resolve the nested row id and the metadata column correctly.
  private def createDataMetadataConflictTable(): Unit = {
    val schema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("nested", StructType(Seq(
        StructField("pk", IntegerType, nullable = false))),
        nullable = false),
      StructField("id", IntegerType),
      StructField("index", IntegerType),
      StructField("dep", StringType)))
    createTable(CatalogV2Util.structTypeToV2Columns(schema))
    append(schema.toDDL,
      """{ "pk": 10, "nested": { "pk": 1 }, "id": 1, "index": 100, "dep": "hr" }
        |{ "pk": 20, "nested": { "pk": 2 }, "id": 2, "index": 200, "dep": "software" }
        |{ "pk": 30, "nested": { "pk": 3 }, "id": 3, "index": 300, "dep": "hr" }
        |""".stripMargin)
  }

  private def checkConflictTable(expected: Seq[Row]): Unit = {
    checkAnswer(
      sql(s"SELECT pk, nested.pk, id, index, dep FROM $tableNameAsString ORDER BY pk"),
      expected)
  }

  test("delete with a data column named like a metadata column") {
    createDataMetadataConflictTable()
    sql(s"DELETE FROM $tableNameAsString WHERE id IN (1, 100)")
    checkConflictTable(Seq(
      Row(20, 2, 2, 200, "software"),
      Row(30, 3, 3, 300, "hr")))
  }

  test("update with a data column named like a metadata column") {
    createDataMetadataConflictTable()
    sql(s"UPDATE $tableNameAsString SET dep = 'it' WHERE id = 1")
    checkConflictTable(Seq(
      Row(10, 1, 1, 100, "it"),
      Row(20, 2, 2, 200, "software"),
      Row(30, 3, 3, 300, "hr")))
  }

  test("merge with a data column named like a metadata column") {
    withTempView("source") {
      createDataMetadataConflictTable()
      Seq((1, "it")).toDF("id", "dep").createOrReplaceTempView("source")
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET t.dep = s.dep
           |""".stripMargin)
      checkConflictTable(Seq(
        Row(10, 1, 1, 100, "it"),
        Row(20, 2, 2, 200, "software"),
        Row(30, 3, 3, 300, "hr")))
      if (splitUpdates) {
        checkLastWriteLog(
          deleteWriteLogEntry(id = 1, metadata = Row("hr", null)),
          reinsertWriteLogEntry(
            metadata = Row("hr", null), data = Row(10, Row(1), 1, 100, "it")))
      } else {
        checkLastWriteLog(
          updateWriteLogEntry(id = 1, metadata = Row("hr", null),
            data = Row(10, Row(1), 1, 100, "it")))
      }
    }
  }

  test("merge target-presence marker is not shadowed by a data column") {
    withTempView("source") {
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("nested", StructType(Seq(
          StructField("pk", IntegerType, nullable = false))), nullable = false),
        StructField("id", IntegerType),
        StructField("__row_from_target", StringType),
        StructField("dep", StringType)))
      createTable(CatalogV2Util.structTypeToV2Columns(schema))
      append(schema.toDDL,
        """{ "pk": 10, "nested": { "pk": 1 }, "id": 1, "__row_from_target": "user", "dep": "hr" }
          |""".stripMargin)
      Seq((1, "it")).toDF("id", "dep").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET t.dep = s.dep
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT pk, __row_from_target, dep FROM $tableNameAsString"),
        Seq(Row(10, "user", "it")))
    }
  }

  test("merge source-presence marker is not shadowed by a source column") {
    withTempView("source") {
      createNestedRowIdTable()
      Seq((1, "source", "it"))
        .toDF("id", "__row_from_source", "dep")
        .createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET t.dep = s.dep
           |""".stripMargin)

      checkTable(Seq(
        Row(10, 1, 1, "it"),
        Row(20, 2, 2, "software"),
        Row(30, 3, 3, "hr")))
    }
  }

  test("merge cardinality row id is not shadowed by a data column") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      withTempView("source") {
      val schema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("nested", StructType(Seq(
          StructField("pk", IntegerType, nullable = false))), nullable = false),
        StructField("id", IntegerType),
        StructField("__row_id", LongType),
        StructField("dep", StringType)))
      createTable(CatalogV2Util.structTypeToV2Columns(schema))
      append(schema.toDDL,
        """{ "pk": 10, "nested": { "pk": 1 }, "id": 1, "__row_id": 0, "dep": "hr" }
          |{ "pk": 20, "nested": { "pk": 2 }, "id": 2, "__row_id": 0, "dep": "software" }
          |""".stripMargin)
      Seq((1, "it"), (2, "sales")).toDF("id", "dep").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET t.dep = s.dep
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT pk, __row_id, dep FROM $tableNameAsString ORDER BY pk"),
        Seq(Row(10, 0L, "it"), Row(20, 0L, "sales")))
      }
    }
  }
}

class DeltaBasedNestedRowIdTableSuite
  extends DeltaBasedNestedRowIdTableSuiteBase(splitUpdates = false)

class DeltaBasedNestedRowIdUpdateAsDeleteAndInsertTableSuite
  extends DeltaBasedNestedRowIdTableSuiteBase(splitUpdates = true)

// Row id is nested.index, whose leaf name collides with the `index` metadata column
// (PRESERVE_ON_DELETE = false). The metadata must be nulled on write, not bound to the row id
// value it shares a name with.
abstract class DeltaBasedNestedRowIdMetadataCollisionSuiteBase(splitUpdates: Boolean)
  extends RowLevelOperationSuiteBase {

  import testImplicits._

  override protected def extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("supports-deltas", "true")
    props.put("nested-metadata-name-row-id", "true")
    if (splitUpdates) props.put("split-updates", "true")
    props
  }

  private def createCollisionTable(): Unit = {
    val schema = StructType(Seq(
      StructField("pk", IntegerType, nullable = false),
      StructField("nested", StructType(Seq(
        StructField("index", IntegerType, nullable = false))),
        nullable = false),
      StructField("id", IntegerType),
      StructField("dep", StringType)))
    createTable(CatalogV2Util.structTypeToV2Columns(schema))
    append(schema.toDDL,
      """{ "pk": 10, "nested": { "index": 1 }, "id": 1, "dep": "hr" }
        |{ "pk": 20, "nested": { "index": 2 }, "id": 2, "dep": "software" }
        |{ "pk": 30, "nested": { "index": 3 }, "id": 3, "dep": "hr" }
        |""".stripMargin)
  }

  test("delete nulls a metadata column colliding with the nested row id") {
    createCollisionTable()
    sql(s"DELETE FROM $tableNameAsString WHERE id IN (1, 100)")
    checkLastWriteLog(deleteWriteLogEntry(id = 1, metadata = Row("hr", null)))
  }

  test("update nulls a metadata column colliding with the nested row id") {
    createCollisionTable()
    sql(s"UPDATE $tableNameAsString SET dep = 'it' WHERE id = 1")
    if (splitUpdates) {
      checkLastWriteLog(
        deleteWriteLogEntry(id = 1, metadata = Row("hr", null)),
        reinsertWriteLogEntry(metadata = Row("hr", null), data = Row(10, Row(1), 1, "it")))
    } else {
      checkLastWriteLog(
        updateWriteLogEntry(id = 1, metadata = Row("hr", null), data = Row(10, Row(1), 1, "it")))
    }
  }

  test("merge delete nulls a metadata column colliding with the nested row id") {
    withTempView("source") {
      createCollisionTable()
      Seq(1).toDF("id").createOrReplaceTempView("source")
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id
           |WHEN MATCHED THEN DELETE
           |""".stripMargin)
      checkLastWriteLog(deleteWriteLogEntry(id = 1, metadata = Row("hr", null)))
    }
  }
}

class DeltaBasedNestedRowIdMetadataCollisionSuite
  extends DeltaBasedNestedRowIdMetadataCollisionSuiteBase(splitUpdates = false)

class DeltaBasedNestedRowIdMetadataCollisionUpdateAsDeleteAndInsertSuite
  extends DeltaBasedNestedRowIdMetadataCollisionSuiteBase(splitUpdates = true)

abstract class DeltaBasedMetadataRowIdTableSuiteBase(
    nested: Boolean,
    splitUpdates: Boolean) extends RowLevelOperationSuiteBase {

  import testImplicits._

  override protected def extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("supports-deltas", "true")
    if (nested) {
      props.put("nested-metadata", "true")
      props.put("nested-metadata-filter", "true")
      props.put("nested-metadata-row-id", "true")
    } else {
      props.put("metadata-row-id", "true")
    }
    if (splitUpdates) props.put("split-updates", "true")
    props
  }

  protected val schema: String = if (nested) {
    "pk INT NOT NULL, id INT, _metadata STRUCT<row_index: INT>, dep STRING"
  } else {
    "pk INT NOT NULL, id INT, dep STRING"
  }

  protected def createMetadataRowIdTable(): Unit = {
    createAndInitTable(schema,
      (if (nested) {
        """{ "pk": 0, "id": 0, "_metadata": { "row_index": 100 }, "dep": "hr" }
          |{ "pk": 1, "id": 1, "_metadata": { "row_index": 200 }, "dep": "hr" }
          |{ "pk": 2, "id": 2, "_metadata": { "row_index": 300 }, "dep": "hr" }
          |""".stripMargin
      } else {
        """{ "pk": 0, "id": 0, "dep": "hr" }
          |{ "pk": 1, "id": 1, "dep": "hr" }
          |{ "pk": 2, "id": 2, "dep": "hr" }
          |""".stripMargin
      }))
  }

  protected def checkMetadataRowIdTable(expected: Seq[Row]): Unit = {
    checkAnswer(
      sql(s"SELECT pk, id, dep FROM $tableNameAsString ORDER BY pk"),
      expected)
  }

  test("delete with a metadata row id") {
    createMetadataRowIdTable()
    sql(s"DELETE FROM $tableNameAsString WHERE id IN (1, 100)")
    checkMetadataRowIdTable(Seq(Row(0, 0, "hr"), Row(2, 2, "hr")))
  }

  test("update with a metadata row id") {
    createMetadataRowIdTable()
    sql(s"UPDATE $tableNameAsString SET dep = 'it' WHERE id = 1")
    checkMetadataRowIdTable(
      Seq(Row(0, 0, "hr"), Row(1, 1, "it"), Row(2, 2, "hr")))
  }

  test("merge with a metadata row id") {
    withTempView("source") {
      createMetadataRowIdTable()
      Seq((1, "it")).toDF("id", "dep").createOrReplaceTempView("source")
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET t.dep = s.dep
           |""".stripMargin)
      checkMetadataRowIdTable(
        Seq(Row(0, 0, "hr"), Row(1, 1, "it"), Row(2, 2, "hr")))
    }
  }
}

class DeltaBasedMetadataRowIdTableSuite
  extends DeltaBasedMetadataRowIdTableSuiteBase(nested = false, splitUpdates = false)

class DeltaBasedMetadataRowIdUpdateAsDeleteAndInsertTableSuite
  extends DeltaBasedMetadataRowIdTableSuiteBase(nested = false, splitUpdates = true)

abstract class DeltaBasedNestedMetadataRowIdTableSuiteBase(splitUpdates: Boolean)
  extends DeltaBasedMetadataRowIdTableSuiteBase(nested = true, splitUpdates = splitUpdates) {

  test("shared nested metadata and row ID reference has one binding") {
    createMetadataRowIdTable()
    val command = sql(s"UPDATE $tableNameAsString SET id = -1 WHERE id = 1")
    val writeDelta = V2Writes(command.queryExecution.analyzed).collectFirst {
      case write: WriteDelta => write
    }.getOrElse(fail("Cannot find WriteDelta"))

    val rowIdAttr = writeDelta.query.output(
      writeDelta.projections.rowIdProjection.colOrdinals.head)
    val metadataProjection = writeDelta.projections.metadataProjection.get
    val metadataOrdinal = metadataProjection.schema.fields.indexWhere { field =>
      field.name == "row_index" &&
        field.metadata.contains(METADATA_COL_ATTR_KEY) &&
        field.metadata.getString(METADATA_COL_ATTR_KEY) == "_metadata"
    }
    assert(metadataOrdinal >= 0)
    val metadataAttr = writeDelta.query.output(
      metadataProjection.colOrdinals(metadataOrdinal))

    val distributionAttr = writeDelta.query.collectFirst {
      case repartition: RepartitionByExpression =>
        repartition.partitionExpressions.flatMap(_.references).head
      case rebalance: RebalancePartitions =>
        rebalance.partitionExpressions.flatMap(_.references).head
    }.getOrElse(fail("Cannot find required distribution"))
    val orderingAttr = writeDelta.query.collectFirst {
      case sort: Sort => sort.order.flatMap(_.references).head
    }.getOrElse(fail("Cannot find required ordering"))

    assert(metadataAttr.exprId == rowIdAttr.exprId)
    assert(distributionAttr.exprId == rowIdAttr.exprId)
    assert(orderingAttr.exprId == rowIdAttr.exprId)
  }

  test("runtime group filtering supports nested metadata") {
    createMetadataRowIdTable()
    val executedPlan = executeAndKeepPlan {
      sql(s"UPDATE $tableNameAsString SET id = -1 WHERE id = 1")
    }
    val filteredScans = collect(executedPlan) {
      case scan: BatchScanExec if scan.runtimeFilters.nonEmpty => scan
    }
    assert(filteredScans.nonEmpty, "could not find a runtime-filtered scan")
    checkMetadataRowIdTable(
      Seq(Row(0, 0, "hr"), Row(1, -1, "hr"), Row(2, 2, "hr")))
  }
}

class DeltaBasedNestedMetadataRowIdTableSuite
  extends DeltaBasedNestedMetadataRowIdTableSuiteBase(splitUpdates = false)

class DeltaBasedNestedMetadataRowIdUpdateAsDeleteAndInsertTableSuite
  extends DeltaBasedNestedMetadataRowIdTableSuiteBase(splitUpdates = true)
