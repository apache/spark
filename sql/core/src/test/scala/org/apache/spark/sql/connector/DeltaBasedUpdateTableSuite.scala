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
import org.apache.spark.sql.catalyst.expressions.InSubquery
import org.apache.spark.sql.catalyst.plans.logical.{RebalancePartitions, RepartitionByExpression, Sort, WriteDelta}
import org.apache.spark.sql.catalyst.util.METADATA_COL_ATTR_KEY
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, TableInfo}
import org.apache.spark.sql.connector.expressions.LogicalExpressions.{identity, reference}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.v2.V2Writes
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

class DeltaBasedUpdateTableSuite extends DeltaBasedUpdateTableSuiteBase {

  import testImplicits._

  override protected lazy val extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("supports-deltas", "true")
    props
  }

  test("update handles metadata columns correctly") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE id IN (1, 100)")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, -1, "hr") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)

    checkLastWriteInfo(
      expectedRowSchema = StructType(table.schema.map {
        case attr if attr.name == "id" => attr.copy(nullable = false) // input is a constant
        case attr => attr
      }),
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))

    checkLastWriteLog(
      updateWriteLogEntry(id = 1, metadata = Row("hr", null), data = Row(1, -1, "hr")))
  }

  test("updated row ID is not shadowed by a data column named like its saved value") {
    createAndInitTable("pk INT NOT NULL, __original_row_id_pk INT, id INT, dep STRING",
      """{ "pk": 1, "__original_row_id_pk": 100, "id": 1, "dep": "hr" }
        |{ "pk": 2, "__original_row_id_pk": 200, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET pk = pk + 10 WHERE id = 1")

    checkAnswer(
      sql(s"SELECT pk, __original_row_id_pk, id, dep FROM $tableNameAsString ORDER BY pk"),
      Seq(Row(2, 200, 2, "software"), Row(11, 100, 1, "hr")))
    checkLastWriteLog(
      updateWriteLogEntry(id = 1, metadata = Row("hr", null),
        data = Row(11, 100, 1, "hr")))
  }

  test("write distribution is not shadowed by a same-named data column") {
    createAndInitTable("pk INT NOT NULL, id INT, _partition STRING, dep STRING",
      """{ "pk": 1, "id": 1, "_partition": "user", "dep": "hr" }
        |{ "pk": 2, "id": 2, "_partition": "user", "dep": "software" }
        |""".stripMargin)

    val command = sql(s"UPDATE $tableNameAsString SET id = -1 WHERE id = 1")
    val preparedCommand = V2Writes(command.queryExecution.analyzed)
    val writeDelta = preparedCommand.collectFirst {
      case write: WriteDelta => write
    }.getOrElse(fail("Cannot find WriteDelta"))
    val metadataProjection = writeDelta.projections.metadataProjection.get
    val metadataOrdinal = metadataProjection.schema.fields.indexWhere { field =>
      field.metadata.contains(METADATA_COL_ATTR_KEY) &&
        field.metadata.getString(METADATA_COL_ATTR_KEY) == "_partition"
    }
    assert(metadataOrdinal >= 0)
    val metadataAttr = writeDelta.query.output(metadataProjection.colOrdinals(metadataOrdinal))
    val rowProjection = writeDelta.projections.rowProjection.get
    val rowOrdinal = rowProjection.schema.fieldIndex("_partition")
    val rowAttr = writeDelta.query.output(rowProjection.colOrdinals(rowOrdinal))

    val distributionAttr = writeDelta.query.collectFirst {
      case repartition: RepartitionByExpression =>
        repartition.partitionExpressions.flatMap(_.references).head
      case rebalance: RebalancePartitions =>
        rebalance.partitionExpressions.flatMap(_.references).head
    }.getOrElse(fail("Cannot find required distribution"))
    val orderingAttr = writeDelta.query.collectFirst {
      case sort: Sort => sort.order.flatMap(_.references).head
    }.getOrElse(fail("Cannot find required ordering"))

    assert(distributionAttr.exprId == metadataAttr.exprId)
    assert(orderingAttr.exprId == metadataAttr.exprId)
    assert(distributionAttr.exprId != rowAttr.exprId)
  }

  test("write distribution resolves a nested metadata field past a colliding data column") {
    val schema = "pk INT NOT NULL, id INT, _metadata STRUCT<row_index: INT>, dep STRING"
    val props = new java.util.HashMap[String, String](extraTableProps)
    props.put("nested-metadata", "true")
    val tableInfo = new TableInfo.Builder()
      .withColumns(CatalogV2Util.structTypeToV2Columns(StructType.fromDDL(schema)))
      .withPartitions(Array[Transform](identity(reference(Seq("dep")))))
      .withProperties(props)
      .build()
    catalog.createTable(ident, tableInfo)
    append(schema,
      """{ "pk": 1, "id": 1, "_metadata": { "row_index": 100 }, "dep": "hr" }
        |{ "pk": 2, "id": 2, "_metadata": { "row_index": 200 }, "dep": "software" }
        |""".stripMargin)

    val command = sql(s"UPDATE $tableNameAsString SET id = -1 WHERE id = 1")
    val writeDelta = V2Writes(command.queryExecution.analyzed).collectFirst {
      case write: WriteDelta => write
    }.getOrElse(fail("Cannot find WriteDelta"))
    val metadataProjection = writeDelta.projections.metadataProjection.get
    val nestedMetadataOrdinal = metadataProjection.schema.fields.indexWhere { field =>
      field.name == "row_index" &&
        field.metadata.contains(METADATA_COL_ATTR_KEY) &&
        field.metadata.getString(METADATA_COL_ATTR_KEY) == "_metadata"
    }
    assert(nestedMetadataOrdinal >= 0)
    val nestedMetadataAttr = writeDelta.query.output(
      metadataProjection.colOrdinals(nestedMetadataOrdinal))
    val rowProjection = writeDelta.projections.rowProjection.get
    val dataMetadataAttr = writeDelta.query.output(
      rowProjection.colOrdinals(rowProjection.schema.fieldIndex("_metadata")))

    val distributionAttr = writeDelta.query.collectFirst {
      case repartition: RepartitionByExpression =>
        repartition.partitionExpressions.flatMap(_.references).head
      case rebalance: RebalancePartitions =>
        rebalance.partitionExpressions.flatMap(_.references).head
    }.getOrElse(fail("Cannot find required distribution"))
    val orderingAttr = writeDelta.query.collectFirst {
      case sort: Sort => sort.order.flatMap(_.references).head
    }.getOrElse(fail("Cannot find required ordering"))

    assert(distributionAttr.exprId == nestedMetadataAttr.exprId)
    assert(orderingAttr.exprId == nestedMetadataAttr.exprId)
    assert(distributionAttr.exprId != dataMetadataAttr.exprId)
  }

  test("update with subquery handles metadata columns correctly") {
    withTempView("updated_dep") {
      createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |{ "pk": 2, "id": 2, "dep": "software" }
          |{ "pk": 3, "id": 3, "dep": "hr" }
          |""".stripMargin)

      val updatedIdDF = Seq(Some("hr"), Some("it")).toDF()
      updatedIdDF.createOrReplaceTempView("updated_dep")

      sql(
        s"""UPDATE $tableNameAsString
           |SET id = -1
           |WHERE
           | id IN (1, 20)
           | AND
           | dep IN (SELECT * FROM updated_dep)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, -1, "hr") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)

      checkLastWriteInfo(
        expectedRowSchema = StructType(table.schema.map {
          case attr if attr.name == "id" => attr.copy(nullable = false) // input is a constant
          case attr => attr
        }),
        expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
        expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))

      checkLastWriteLog(
        updateWriteLogEntry(id = 1, metadata = Row("hr", null), data = Row(1, -1, "hr")))
    }
  }

  test("update runtime group filtering (DPP enabled)") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true") {
      checkUpdateRuntimeGroupFiltering()
    }
  }

  test("update runtime group filtering (DPP disabled)") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false") {
      checkUpdateRuntimeGroupFiltering()
    }
  }

  test("update runtime group filtering (AQE enabled)") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      checkUpdateRuntimeGroupFiltering()
    }
  }

  test("update runtime group filtering (AQE disabled)") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      checkUpdateRuntimeGroupFiltering()
    }
  }

  private def checkUpdateRuntimeGroupFiltering(): Unit = {
    withTable(tableNameAsString) {
      withTempView("deleted_id") {
        createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
          """{ "pk": 1, "id": 1, "salary": 300, "dep": "hr" }
            |{ "pk": 2, "id": 2, "salary": 150, "dep": "software" }
            |{ "pk": 3, "id": 3, "salary": 120, "dep": "hr" }
            |""".stripMargin)

        val deletedIdDF = Seq(Some(1), None).toDF()
        deletedIdDF.createOrReplaceTempView("deleted_id")

        executeAndCheckScans(
          s"UPDATE $tableNameAsString SET salary = -1 WHERE id IN (SELECT * FROM deleted_id)",
          primaryScanSchema = "pk INT, id INT, dep STRING, _partition STRING",
          groupFilterScanSchema = Some("id INT, dep STRING"))

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Row(1, 1, -1, "hr") :: Row(2, 2, 150, "software") :: Row(3, 3, 120, "hr") :: Nil)
      }
    }
  }

  test("update does not double plan table") {
    createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
      """{ "pk": 1, "id": 1, "salary": 300, "dep": 'hr' }
        |{ "pk": 2, "id": 2, "salary": 150, "dep": 'software' }
        |{ "pk": 3, "id": 3, "salary": 120, "dep": 'hr' }
        |""".stripMargin)

    val (cond, groupFilterCond) = executeAndKeepConditions {
      sql(
        s"""UPDATE $tableNameAsString SET salary = -1
           |WHERE id IN (SELECT id FROM $tableNameAsString WHERE salary > 200)
           |""".stripMargin)
    }

    cond match {
      case InSubquery(_, query) => assertNoScanPlanning(query.plan)
      case _ => fail(s"unexpected condition: $cond")
    }

    groupFilterCond match {
      case Some(InSubquery(_, query)) => assertNoScanPlanning(query.plan)
      case _ => fail(s"unexpected group filter: $groupFilterCond")
    }

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, 1, -1, "hr") :: Row(2, 2, 150, "software") :: Row(3, 3, 120, "hr") :: Nil)
  }
}
