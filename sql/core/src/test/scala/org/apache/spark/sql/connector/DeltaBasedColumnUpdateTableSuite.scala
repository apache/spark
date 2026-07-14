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
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, TableInfo}
import org.apache.spark.sql.connector.expressions.LogicalExpressions.{identity, reference}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Tests for UPDATE statements targeting connectors that return true from
 * [[org.apache.spark.sql.connector.write.RowLevelOperation#supportsColumnUpdates]].
 *
 * When a connector supports column updates, Spark narrows the row projection
 * (LogicalWriteInfo.schema()) to contain only the assigned/changed columns rather than
 * the full table row.
 */
class DeltaBasedColumnUpdateTableSuite extends RowLevelOperationSuiteBase {

  override protected lazy val extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("column-update", "true")
    props
  }

  test("column-update: rowSchema contains only the single assigned column") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    // info.schema() is empty: UPDATE-only column-update writes have no INSERT-shaped rows,
    // so nothing flows through the writer's write() path. The narrow update-row layout is
    // carried by info.updateSchema().
    checkLastWriteInfo(
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))),
      expectedUpdateSchema = Some(StructType(Seq(
        PK_FIELD,
        StructField("id", IntegerType, nullable = false)
      ))))
  }

  test("column-update: rowSchema contains multiple assigned columns") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1, dep = 'engineering' WHERE pk = 1")

    checkLastWriteInfo(
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))),
      expectedUpdateSchema = Some(StructType(Seq(
        PK_FIELD,
        StructField("id", IntegerType, nullable = false),
        StructField("dep", StringType, nullable = false)
      ))))
  }

  test("column-update: rowSchema is empty for a full identity update") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = id, dep = dep WHERE pk = 1")

    // All assignments are identity, so updatedColumns is empty -- the connector still declares
    // pk for row lookup, so the narrow update schema is just [pk].
    checkLastWriteInfo(
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))),
      expectedUpdateSchema = Some(StructType(Array(PK_FIELD))))
  }

  test("column-update: row filter condition is orthogonal to column narrowing") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET dep = 'engineering' WHERE pk IN (1, 3)")

    checkLastWriteInfo(
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))),
      expectedUpdateSchema = Some(StructType(Seq(
        PK_FIELD,
        StructField("dep", StringType, nullable = false)
      ))))
  }

  test("column-update: update all rows (no WHERE clause)") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = salary * 2")

    checkLastWriteInfo(
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))),
      expectedUpdateSchema = Some(StructType(Seq(
        PK_FIELD,
        StructField("salary", IntegerType, nullable = true)
      ))))
  }

  test("column-update: rowSchema excludes identity assignments in a mixed UPDATE") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = id, dep = 'engineering' WHERE pk = 1")

    checkLastWriteInfo(
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))),
      expectedUpdateSchema = Some(StructType(Seq(
        PK_FIELD,
        StructField("dep", StringType, nullable = false)
      ))))
  }

  test("column-update: cross-column assignment is not treated as identity") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET dep = dep, id = -1 WHERE pk = 1")

    checkLastWriteInfo(
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))),
      expectedUpdateSchema = Some(StructType(Seq(
        PK_FIELD,
        StructField("id", IntegerType, nullable = false)
      ))))
  }

  test("column-update: nested struct field update narrows to the root struct column") {
    createAndInitTable("pk INT NOT NULL, s STRUCT<c1: INT, c2: INT>, dep STRING",
      """{ "pk": 1, "s": { "c1": 1, "c2": 2 }, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET s.c1 = -1 WHERE pk = 1")

    val updatedNames = table.lastUpdatedColumns.map(_.describe()).toSet
    assert(updatedNames == Set("s"),
      s"expected [s] in updatedColumns (root struct) but got: $updatedNames")

    // info.updateSchema() carries the narrow row layout; info.schema() is the full table.
    val updateSchema = table.lastWriteInfo.updateSchema().get()
    assert(updateSchema.fieldNames.contains("s"),
      s"s must be in update schema: $updateSchema")
    assert(!updateSchema.fieldNames.contains("dep"),
      s"dep must not be in update schema: $updateSchema")
  }

  test("column-update: updatedColumns contains non-identity assigned columns") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1, dep = 'eng' WHERE pk = 1")

    val updatedNames = table.lastUpdatedColumns.map(_.describe()).toSet
    assert(updatedNames == Set("id", "dep"),
      s"expected [id, dep] in updatedColumns but got: $updatedNames")
  }

  test("column-update: updatedColumns excludes identity assignments") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1, dep = dep WHERE pk = 1")

    val updatedNames = table.lastUpdatedColumns.map(_.describe()).toSet
    assert(updatedNames == Set("id"),
      s"expected only [id] in updatedColumns but got: $updatedNames")
  }

  test("column-update: updatedColumns is empty for a full identity update") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = id, dep = dep WHERE pk = 1")

    assert(table.lastUpdatedColumns.isEmpty,
      s"expected empty updatedColumns but got: ${table.lastUpdatedColumns.mkString(", ")}")
  }

  test("column-update: updatedColumns is empty for DELETE (Javadoc contract)") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"DELETE FROM $tableNameAsString WHERE dep = 'hr'")

    assert(table.lastUpdatedColumns.isEmpty,
      s"DELETE must pass empty updatedColumns but got: ${table.lastUpdatedColumns.mkString(", ")}")
  }

  test("column-update: data correctness -- single column update") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)
    // 1 row matched WHERE pk = 1 -> 1 update. MoR emits no COPY rows.
    checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 0, deltaUpdate = true)
  }

  test("column-update: data correctness -- update all rows") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = salary * 2")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 200, "hr") :: Row(2, 400, "software") :: Row(3, 600, "hr") :: Nil)
    checkUpdateMetrics(numUpdatedRows = 3, numCopiedRows = 0, deltaUpdate = true)
  }

  test("column-update: data correctness -- mixed identity and real assignments") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = id, dep = 'engineering' WHERE pk = 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 1, "engineering") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)
    checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 0, deltaUpdate = true)
  }

  private def createAndInitTableWithReqAttrs(
      reqAttrs: String,
      schemaString: String,
      jsonData: String): Unit = {
    val props = new java.util.HashMap[String, String]()
    props.put("column-update-req-attrs", reqAttrs)
    val columns = CatalogV2Util.structTypeToV2Columns(StructType.fromDDL(schemaString))
    val transforms = Array[Transform](identity(reference(Seq("dep"))))
    val tableInfo = new TableInfo.Builder()
      .withColumns(columns)
      .withPartitions(transforms)
      .withProperties(props)
      .build()
    catalog.createTable(ident, tableInfo)
    append(schemaString, jsonData)
  }

  test("column-update: requiredDataAttributes - data correctness") {
    createAndInitTableWithReqAttrs("dep,id", "pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)
  }

  test("column-update: empty requiredDataAttributes throws AnalysisException") {
    val props = new java.util.HashMap[String, String]()
    props.put("column-update-empty-req-attrs", "true")
    val columns = CatalogV2Util.structTypeToV2Columns(
      StructType.fromDDL("pk INT NOT NULL, id INT, dep STRING"))
    val transforms = Array[Transform](identity(reference(Seq("dep"))))
    val tableInfo = new TableInfo.Builder()
      .withColumns(columns)
      .withPartitions(transforms)
      .withProperties(props)
      .build()
    catalog.createTable(ident, tableInfo)
    append("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }""".stripMargin)

    // A connector that mixes in SupportsColumnUpdates but returns an empty
    // requiredDataAttributes() violates the mix-in contract -- analysis must reject the
    // operation rather than silently falling through to the wide write path.
    val ex = intercept[org.apache.spark.sql.AnalysisException] {
      sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")
    }
    assert(ex.getCondition == "EMPTY_REQUIRED_DATA_ATTRIBUTES",
      s"expected EMPTY_REQUIRED_DATA_ATTRIBUTES but got: ${ex.getCondition}")
  }

  test("column-update: requiredDataAttributes throws AnalysisException for invalid column") {
    createAndInitTableWithReqAttrs("nonexistent_col", "pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |""".stripMargin)

    val ex = intercept[org.apache.spark.sql.AnalysisException] {
      sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")
    }
    assert(ex.getMessage.contains("nonexistent_col"),
      s"Expected error about unresolvable column but got: ${ex.getMessage}")
  }

  private def createAndInitTableFromInfo(schemaString: String, jsonData: String): Unit = {
    val props = new java.util.HashMap[String, String]()
    props.put("column-update-from-info", "true")
    val columns = CatalogV2Util.structTypeToV2Columns(StructType.fromDDL(schemaString))
    val transforms = Array[Transform](identity(reference(Seq("dep"))))
    val tableInfo = new TableInfo.Builder()
      .withColumns(columns)
      .withPartitions(transforms)
      .withProperties(props)
      .build()
    catalog.createTable(ident, tableInfo)
    append(schemaString, jsonData)
  }

  test("column-update from-info: write schema is updatedColumns + pk pass-through") {
    createAndInitTableFromInfo("pk INT NOT NULL, salary INT, id INT, dep STRING",
      """{ "pk": 1, "salary": 100, "id": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "id": 20, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")

    val updateSchema = table.lastWriteInfo.updateSchema().get()
    assert(updateSchema.fieldNames.contains("salary"),
      s"salary must be in update schema: $updateSchema")
    assert(updateSchema.fieldNames.contains("pk"),
      s"pk must be in update schema: $updateSchema")
    assert(!updateSchema.fieldNames.contains("id"),
      s"id must not be in update schema: $updateSchema")
    assert(!updateSchema.fieldNames.contains("dep"),
      s"dep must not be in update schema: $updateSchema")
  }

  test("column-update from-info: pk already in updatedColumns is not duplicated") {
    createAndInitTableFromInfo("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET pk = pk + 10, salary = -1 WHERE dep = 'hr'")

    val updateSchema = table.lastWriteInfo.updateSchema().get()
    val pkCount = updateSchema.fieldNames.count(_ == "pk")
    assert(pkCount == 1, s"pk must appear exactly once in update schema: $updateSchema")
  }

  test("column-update from-info: data correctness") {
    createAndInitTableFromInfo("pk INT NOT NULL, salary INT, id INT, dep STRING",
      """{ "pk": 1, "salary": 100, "id": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "id": 20, "dep": "software" }
        |{ "pk": 3, "salary": 300, "id": 30, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, 10, "hr") ::
      Row(2, 200, 20, "software") ::
      Row(3, -1, 30, "hr") :: Nil)
  }

  private def createAndInitTableSplit(schemaString: String, jsonData: String): Unit = {
    val props = new java.util.HashMap[String, String]()
    props.put("column-update-split", "true")
    val columns = CatalogV2Util.structTypeToV2Columns(StructType.fromDDL(schemaString))
    val transforms = Array[Transform](identity(reference(Seq("dep"))))
    val tableInfo = new TableInfo.Builder()
      .withColumns(columns)
      .withPartitions(transforms)
      .withProperties(props)
      .build()
    catalog.createTable(ident, tableInfo)
    append(schemaString, jsonData)
  }

  test("column-update split: write schema is narrow (assigned + pk pass-through)") {
    createAndInitTableSplit("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    // For representUpdateAsDeleteAndInsert connectors, reinsert-tagged rows still route through
    // writeUpdate() (info.updateSchema() = narrow), not write(). info.schema() stays empty.
    checkLastWriteInfo(
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))),
      expectedUpdateSchema = Some(StructType(Seq(
        PK_FIELD,
        StructField("id", IntegerType, nullable = false)
      ))))
  }

  test("column-update split: data correctness") {
    createAndInitTableSplit("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE dep = 'hr'")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 2, "software") :: Row(3, -1, "hr") :: Nil)
  }

  // ---------------------------------------------------------------------------
  // Scan-narrowing tests: verify that when the connector opts into column updates,
  // the physical scan reads only the columns actually needed by the operation.
  //
  // Assertions use `checkLastScanExcludes` (proves narrowing) and `checkLastScanIncludes`
  // (proves transparent widening) rather than exact-set matches -- ColumnPruning is
  // free to further tighten the scan beyond our analysis-time narrow set (e.g. when an
  // assignment RHS is a literal, the assigned column doesn't need to be read).
  // ---------------------------------------------------------------------------

  test("column-update: scan excludes columns outside connector-declared + cond + RHS refs") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    // requiredDataAttributes = [pk, id]; cond refs [pk] only; RHS is a literal.
    // `dep` is neither declared nor referenced -- it must not appear in the scan.
    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    checkLastScanExcludes("dep")
  }

  test("column-update: scan transparently widens for cond-referenced columns") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    // requiredDataAttributes = [pk, id]; cond refs [dep].
    // `dep` must appear even though the connector didn't declare it.
    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE dep = 'hr'")

    checkLastScanIncludes("dep")
  }

  test("column-update: scan transparently widens for assignment RHS references") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "hr" }
        |""".stripMargin)

    // requiredDataAttributes = [pk, salary]; RHS `salary + bonus` references `bonus`.
    // `bonus` must appear in the scan even though it's not declared as required.
    sql(s"UPDATE $tableNameAsString SET salary = salary + bonus WHERE pk = 1")

    checkLastScanIncludes("bonus", "salary")
    checkLastScanExcludes("dep")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 110, 10, "hr") :: Row(2, 200, 20, "hr") :: Nil)
  }

  test("column-update: analysis fails when assignment key is outside requiredDataAttributes") {
    // Connector declares only [pk] but the user assigns to `id`. We enforce
    // updatedColumns ⊆ requiredDataAttributes at analysis time (root-column granularity).
    createAndInitTableWithReqAttrs("pk", "pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }""".stripMargin)

    val ex = intercept[org.apache.spark.sql.AnalysisException] {
      sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")
    }
    assert(ex.getCondition == "REQUIRED_DATA_ATTRIBUTES_MISSING_UPDATED_COLUMNS",
      s"expected REQUIRED_DATA_ATTRIBUTES_MISSING_UPDATED_COLUMNS but got: ${ex.getCondition}")
    assert(ex.getMessage.contains("id"),
      s"error message must name the missing column `id`: ${ex.getMessage}")
  }
}
