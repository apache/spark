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

package org.apache.spark.sql.connector.catalog

import java.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, MetadataAttribute}
import org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.util.SchemaValidationMode.PROHIBIT_CHANGES
import org.apache.spark.util.ArrayImplicits.SparkArrayOps

class V2TableUtilSuite extends SparkFunSuite {

  test("validateCapturedColumns - no changes") {
    val cols = Array(
      col("id", LongType, nullable = false),
      col("name", StringType, nullable = true))
    val table = TestTableWithMetadataSupport("test", cols)

    val errors = validateCapturedColumns(table, cols)
    assert(errors.isEmpty, "No changes should produce no errors")
  }

  test("validateCapturedColumns - column type changed") {
    val originCols = Array(
      col("id", LongType, nullable = true), // original type
      col("name", StringType, nullable = true))
    val currentCols = Array(
      col("id", StringType, nullable = true), // changed from LongType
      col("name", StringType, nullable = true))
    val table = TestTableWithMetadataSupport("test", currentCols)

    val errors = validateCapturedColumns(table, originCols)
    assert(errors.size == 1)
    assert(errors.head.contains("`id` type has changed from BIGINT to STRING"))
  }

  test("validateCapturedColumns - column nullability changed to not null") {
    val originCols = Array(
      col("id", LongType, nullable = true), // originally nullable
      col("name", StringType, nullable = true))
    val currentCols = Array(
      col("id", LongType, nullable = false), // now NOT NULL
      col("name", StringType, nullable = true))
    val table = TestTableWithMetadataSupport("test", currentCols)

    val errors = validateCapturedColumns(table, originCols)
    assert(errors.size == 1)
    assert(errors.head == "`id` is no longer nullable")
  }

  test("validateCapturedColumns - column nullability changed to nullable") {
    val originCols = Array(
      col("id", LongType, nullable = false), // originally NOT NULL
      col("name", StringType, nullable = true))
    val currentCols = Array(
      col("id", LongType, nullable = true), // now nullable
      col("name", StringType, nullable = true))
    val table = TestTableWithMetadataSupport("test", currentCols)

    val errors = validateCapturedColumns(table, originCols)
    assert(errors.size == 1)
    assert(errors.head == "`id` is nullable now")
  }

  test("validateCapturedColumns - column removed") {
    val originCols = Array(
      col("id", LongType, nullable = true), // originally present
      col("name", StringType, nullable = true))
    val currentCols = Array(
      col("name", StringType, nullable = true))
    val table = TestTableWithMetadataSupport("test", currentCols)

    val errors = validateCapturedColumns(table, originCols)
    assert(errors.size == 1)
    assert(errors.head == "`id` BIGINT has been removed")
  }

  test("validateCapturedColumns - column added") {
    val originCols = Array(
      col("id", LongType, nullable = true),
      col("name", StringType, nullable = true))
    val currentCols = Array(
      col("id", LongType, nullable = true),
      col("name", StringType, nullable = true),
      col("age", IntegerType, nullable = true)) // new column
    val table = TestTableWithMetadataSupport("test", currentCols)

    val errors = validateCapturedColumns(table, originCols)
    assert(errors.size == 1)
    assert(errors.head == "`age` INT has been added")
  }

  test("validateCapturedColumns - multiple columns removed and added") {
    val originCols = Array(
      col("id", LongType, nullable = true),
      col("name", StringType, nullable = true),    // originally present
      col("address", StringType, nullable = true))  // originally present
    val currentCols = Array(
      col("id", LongType, nullable = true),
      col("email", StringType, nullable = true), // new column
      col("age", IntegerType, nullable = true))   // new column
    val table = TestTableWithMetadataSupport("test", currentCols)

    val errors = validateCapturedColumns(table, originCols)
    assert(errors.size == 4) // 2 removed + 2 added
    assert(errors.count(_.contains("removed")) == 2)
    assert(errors.count(_.contains("has been added")) == 2)
  }

  test("validateCapturedColumns - case insensitive column names") {
    val originCols = Array(
      col("id", LongType, nullable = true), // lowercase
      col("name", StringType, nullable = true))
    val currentCols = Array(
      col("ID", LongType, nullable = true), // uppercase
      col("NAME", StringType, nullable = true))
    val table = TestTableWithMetadataSupport("test", currentCols)

    val errors = validateCapturedColumns(table, originCols)
    assert(errors.isEmpty, "Case insensitive comparison should match")
  }

  test("validateCapturedColumns - duplicate columns with different case") {
    val originCols = Array(
      col("id", LongType, nullable = true),
      col("name", StringType, nullable = true))
    val currentCols = Array(
      col("id", LongType, nullable = true),
      col("ID", StringType, nullable = true), // duplicate with different case
      col("name", StringType, nullable = true))
    val table = TestTableWithMetadataSupport("test", currentCols)

    val e = intercept[AnalysisException] { validateCapturedColumns(table, originCols) }
    assert(e.message.contains("Choose another name or rename the existing column"))
  }

  test("validateCapturedColumns - complex types") {
    val structType = StructType(Seq(
      StructField("street", StringType),
      StructField("city", StringType)))
    val originCols = Array(
      col("id", LongType, nullable = true),
      col("address", structType, nullable = true))
    val currentCols = Array(
      col("id", LongType, nullable = true),
      col("address", structType, nullable = true))
    val table = TestTableWithMetadataSupport("test", currentCols)

    val errors = validateCapturedColumns(table, originCols)
    assert(errors.isEmpty)
  }

  test("validateCapturedColumns - complex type changed") {
    val originStructType = StructType(Seq(
      StructField("street", StringType),
      StructField("city", StringType))) // originally StringType
    val originCols = Array(
      col("id", LongType, nullable = true),
      col("address", originStructType, nullable = true))
    val currentStructType = StructType(Seq(
      StructField("street", StringType),
      StructField("city", IntegerType))) // changed type
    val currentCols = Array(
      col("id", LongType, nullable = true),
      col("address", currentStructType, nullable = true))
    val table = TestTableWithMetadataSupport("test", currentCols)

    val errors = V2TableUtil.validateCapturedColumns(table, originCols.toSeq)
    assert(errors.size == 1)
    assert(errors.head.contains("`address`.`city` type has changed from STRING to INT"))
  }

  test("validateCapturedMetadataColumns - no changes") {
    val originMetaCols = Seq(
      metaCol("_partition", StringType, nullable = false),
      metaCol("index", IntegerType, nullable = false))
    val currentMetaCols = Array(
      metaCol("_partition", StringType, nullable = false),
      metaCol("index", IntegerType, nullable = false))
    val table = TestTableWithMetadataSupport("test", Array.empty, currentMetaCols)

    val errors = V2TableUtil.validateCapturedMetadataColumns(table, originMetaCols)
    assert(errors.isEmpty, "No changes should produce no errors")
  }

  test("validateCapturedMetadataColumns - type changed") {
    val originMetaCols = Seq(
      metaCol("index", IntegerType, nullable = false)) // originally IntegerType
    val currentMetaCols = Array(
      metaCol("index", StringType, nullable = false)) // changed to StringType
    val table = TestTableWithMetadataSupport("test", Array.empty, currentMetaCols)

    val errors = V2TableUtil.validateCapturedMetadataColumns(table, originMetaCols)
    assert(errors.size == 1)
    assert(errors.head == "`index` type has changed from INT to STRING")
  }

  test("validateCapturedMetadataColumns - nullability changed to nullable") {
    val originMetaCols = Seq(
      metaCol("index", IntegerType, nullable = false)) // originally NOT NULL
    val currentMetaCols = Array(
      metaCol("index", IntegerType, nullable = true)) // now nullable
    val table = TestTableWithMetadataSupport("test", Array.empty, currentMetaCols)

    val errors = V2TableUtil.validateCapturedMetadataColumns(table, originMetaCols)
    assert(errors.size == 1)
    assert(errors.head == "`index` is nullable now")
  }

  test("validateCapturedMetadataColumns - nullability changed to not null") {
    val originMetaCols = Seq(metaCol("index", IntegerType, nullable = true)) // originally nullable
    val currentMetaCols = Array(metaCol("index", IntegerType, nullable = false)) // now NOT NULL
    val table = TestTableWithMetadataSupport("test", Array.empty, currentMetaCols)

    val errors = V2TableUtil.validateCapturedMetadataColumns(table, originMetaCols)
    assert(errors.size == 1)
    assert(errors.head == "`index` is no longer nullable")
  }

  test("validateCapturedMetadataColumns - column removed") {
    val originMetaCols = Seq(metaCol("index", IntegerType, nullable = true)) // originally present
    val currentMetaCols = Array.empty[MetadataColumn] // no metadata columns
    val table = TestTableWithMetadataSupport("test", Array.empty, currentMetaCols)

    val errors = V2TableUtil.validateCapturedMetadataColumns(table, originMetaCols)
    assert(errors.size == 1)
    assert(errors.head == "`index` INT has been removed")
  }

  test("validateCapturedMetadataColumns - table doesn't support metadata") {
    // table that doesn't implement SupportsMetadataColumns
    val table = TestTable("test", Array(col("id", LongType, nullable = true)))
    val originMetaCols = Seq(metaCol("index", IntegerType, nullable = false))

    val errors = V2TableUtil.validateCapturedMetadataColumns(table, originMetaCols)
    assert(errors.size == 1)
    assert(errors.head == "`index` INT NOT NULL has been removed")
  }

  test("validateCapturedMetadataColumns - multiple errors") {
    val originMetaCols = Seq(
      metaCol("_partition", StringType, nullable = false),
      metaCol("index", IntegerType, nullable = false)) // originally present
    val currentMetaCols = Array(
      metaCol("_partition", IntegerType, nullable = false)) // type changed from StringType
    val table = TestTableWithMetadataSupport("test", Array.empty, currentMetaCols)

    val errors = V2TableUtil.validateCapturedMetadataColumns(table, originMetaCols)
    assert(errors.size == 2)
    assert(errors.exists(e => e.contains("_partition") && e.contains("type has changed")))
    assert(errors.exists(e => e.contains("index") && e.contains("removed")))
  }

  test("validateCapturedMetadataColumns - case insensitive names") {
    val originMetaCols = Seq(metaCol("index", IntegerType, nullable = true)) // lowercase
    val currentMetaCols = Array(metaCol("INDEX", IntegerType, nullable = true)) // uppercase
    val table = TestTableWithMetadataSupport("test", Array.empty, currentMetaCols)

    val errors = V2TableUtil.validateCapturedMetadataColumns(table, originMetaCols)
    assert(errors.isEmpty, "Case insensitive comparison should match")
  }

  test("validateCapturedMetadataColumns - duplicate metadata columns with different case") {
    val originMetaCols = Seq(
      metaCol("_partition", StringType, nullable = false),
      metaCol("index", IntegerType, nullable = false))
    val currentMetaCols = Array(
      metaCol("_partition", StringType, nullable = false),
      metaCol("index", IntegerType, nullable = false),
      metaCol("INDEX", StringType, nullable = false)) // duplicate with different case
    val table = TestTableWithMetadataSupport("test", Array.empty, currentMetaCols)

    val e = intercept[AnalysisException] {
      V2TableUtil.validateCapturedMetadataColumns(table, originMetaCols)
    }
    assert(e.message.contains("Choose another name or rename the existing column"))
  }

  test("validateCapturedMetadataColumns - empty metadata columns") {
    val originMetaCols = Seq.empty[MetadataColumn]
    val currentMetaCols = Array.empty[MetadataColumn]
    val table = TestTableWithMetadataSupport("test", Array.empty, currentMetaCols)

    val errors = V2TableUtil.validateCapturedMetadataColumns(table, originMetaCols)
    assert(errors.isEmpty, "No metadata columns should produce no errors")
  }

  test("validateCapturedMetadataColumns - complex metadata type") {
    val structType = StructType(Seq(
      StructField("bucket", IntegerType),
      StructField("partition", IntegerType)))
    val originMetaCols = Seq(metaCol("_partition", structType, nullable = false))
    val currentMetaCols = Array(metaCol("_partition", structType, nullable = false))
    val table = TestTableWithMetadataSupport("test", Array.empty, currentMetaCols)

    val errors = V2TableUtil.validateCapturedMetadataColumns(table, originMetaCols)
    assert(errors.isEmpty)
  }

  test("validateCapturedMetadataColumns - complex metadata type changed") {
    val originStructType = StructType(Seq(
      StructField("bucket", IntegerType), // originally IntegerType
      StructField("partition", IntegerType)))
    val originMetaCols = Seq(metaCol("_partition", originStructType, nullable = false))
    val currentStructType = StructType(Seq(
      StructField("bucket", StringType), // changed type
      StructField("partition", IntegerType)))
    val currentMetaCols = Array(metaCol("_partition", currentStructType, nullable = false))
    val table = TestTableWithMetadataSupport("test", Array.empty, currentMetaCols)

    val errors = V2TableUtil.validateCapturedMetadataColumns(table, originMetaCols)
    assert(errors.size == 1)
    assert(errors.head.contains("`_partition`.`bucket` type has changed from INT to STRING"))
  }

  test("validateCapturedMetadataColumns - with DataSourceV2Relation") {
    val dataCols = Array(
      col("id", LongType, nullable = true),
      col("name", StringType, nullable = true))
    val originMetaCols = Array(
      metaCol("_partition", StringType, nullable = false),
      metaCol("index", IntegerType, nullable = false))
    val originTable = TestTableWithMetadataSupport("test", dataCols, originMetaCols)

    val dataAttrs = dataCols.map(c => AttributeReference(c.name, c.dataType, c.nullable)())
    val metadataAttrs = originMetaCols.map(c => MetadataAttribute(c.name, c.dataType, c.isNullable))
    val attrs = dataAttrs ++ metadataAttrs

    val relation = DataSourceV2Relation(
      originTable,
      attrs.toImmutableArraySeq,
      None,
      None,
      CaseInsensitiveStringMap.empty())

    val currentMetaCols = Array(
      metaCol("_partition", IntegerType, nullable = false), // type changed
      metaCol("index", IntegerType, nullable = false))
    val currentTable = TestTableWithMetadataSupport("test", dataCols, currentMetaCols)

    val errors = V2TableUtil.validateCapturedMetadataColumns(
      currentTable,
      relation,
      mode = PROHIBIT_CHANGES)
    assert(errors.size == 1)
    assert(errors.head.contains("`_partition` type has changed"))
  }

  test("validateCapturedMetadataColumns - with DataSourceV2Relation no metadata attrs") {
    val dataCols = Array(
      col("id", LongType, nullable = true),
      col("name", StringType, nullable = true))
    val originTable = TestTable("test", dataCols)

    val dataAttrs = dataCols.map(c => AttributeReference(c.name, c.dataType, c.nullable)())

    val relation = DataSourceV2Relation(
      originTable,
      dataAttrs.toImmutableArraySeq,
      None,
      None,
      CaseInsensitiveStringMap.empty())

    val currentTable = TestTable("test", dataCols)

    val errors = V2TableUtil.validateCapturedMetadataColumns(
      currentTable,
      relation,
      mode = PROHIBIT_CHANGES)
    assert(errors.isEmpty)
  }

  test("validateCapturedColumns - array element type changed") {
    val originCols = Array(
      col("id", LongType, nullable = true),
      col("data", ArrayType(IntegerType, containsNull = true), nullable = true))
    val currentCols = Array(
      col("id", LongType, nullable = true),
      col("data", ArrayType(LongType, containsNull = true), nullable = true))
    val table = TestTableWithMetadataSupport("test", currentCols)

    val errors = validateCapturedColumns(table, originCols)
    assert(errors.size == 1)
    assert(errors.head == "`data`.`element` type has changed from INT to BIGINT")
  }

  test("validateCapturedColumns - array containsNull changed to false") {
    val originCols = Array(
      col("id", LongType, nullable = true),
      col("data", ArrayType(IntegerType, containsNull = true), nullable = true))
    val currentCols = Array(
      col("id", LongType, nullable = true),
      col("data", ArrayType(IntegerType, containsNull = false), nullable = true))
    val table = TestTableWithMetadataSupport("test", currentCols)

    val errors = validateCapturedColumns(table, originCols)
    assert(errors.size == 1)
    assert(errors.head == "`data`.`element` is no longer nullable")
  }

  test("validateCapturedColumns - array containsNull changed to true") {
    val originCols = Array(
      col("id", LongType, nullable = true),
      col("data", ArrayType(IntegerType, containsNull = false), nullable = true))
    val currentCols = Array(
      col("id", LongType, nullable = true),
      col("data", ArrayType(IntegerType, containsNull = true), nullable = true))
    val table = TestTableWithMetadataSupport("test", currentCols)

    val errors = validateCapturedColumns(table, originCols)
    assert(errors.size == 1)
    assert(errors.head == "`data`.`element` is nullable now")
  }

  test("validateCapturedColumns - map key type changed") {
    val originCols = Array(
      col("id", LongType, nullable = true),
      col("data", MapType(IntegerType, StringType, valueContainsNull = true), nullable = true))
    val currentCols = Array(
      col("id", LongType, nullable = true),
      col("data", MapType(LongType, StringType, valueContainsNull = true), nullable = true))
    val table = TestTableWithMetadataSupport("test", currentCols)

    val errors = validateCapturedColumns(table, originCols)
    assert(errors.size == 1)
    assert(errors.head == "`data`.`key` type has changed from INT to BIGINT")
  }

  test("validateCapturedColumns - map value type changed") {
    val originCols = Array(
      col("id", LongType, nullable = true),
      col("data", MapType(StringType, IntegerType, valueContainsNull = true), nullable = true))
    val currentCols = Array(
      col("id", LongType, nullable = true),
      col("data", MapType(StringType, LongType, valueContainsNull = true), nullable = true))
    val table = TestTableWithMetadataSupport("test", currentCols)

    val errors = validateCapturedColumns(table, originCols)
    assert(errors.size == 1)
    assert(errors.head == "`data`.`value` type has changed from INT to BIGINT")
  }

  test("validateCapturedColumns - map valueContainsNull changed to false") {
    val originCols = Array(
      col("id", LongType, nullable = true),
      col("data", MapType(StringType, IntegerType, valueContainsNull = true), nullable = true))
    val currentCols = Array(
      col("id", LongType, nullable = true),
      col("data", MapType(StringType, IntegerType, valueContainsNull = false), nullable = true))
    val table = TestTableWithMetadataSupport("test", currentCols)

    val errors = validateCapturedColumns(table, originCols)
    assert(errors.size == 1)
    assert(errors.head == "`data`.`value` is no longer nullable")
  }

  test("validateCapturedColumns - map valueContainsNull changed to true") {
    val originCols = Array(
      col("id", LongType, nullable = true),
      col("data", MapType(StringType, IntegerType, valueContainsNull = false), nullable = true))
    val currentCols = Array(
      col("id", LongType, nullable = true),
      col("data", MapType(StringType, IntegerType, valueContainsNull = true), nullable = true))
    val table = TestTableWithMetadataSupport("test", currentCols)

    val errors = validateCapturedColumns(table, originCols)
    assert(errors.size == 1)
    assert(errors.head == "`data`.`value` is nullable now")
  }

  test("validateCapturedColumns - nested array in struct element type changed") {
    val originStructType = StructType(Seq(
      StructField("name", StringType),
      StructField("scores", ArrayType(IntegerType, containsNull = true))))
    val originCols = Array(
      col("id", LongType, nullable = true),
      col("person", originStructType, nullable = true))
    val currentStructType = StructType(Seq(
      StructField("name", StringType),
      StructField("scores", ArrayType(LongType, containsNull = true))))
    val currentCols = Array(
      col("id", LongType, nullable = true),
      col("person", currentStructType, nullable = true))
    val table = TestTableWithMetadataSupport("test", currentCols)

    val errors = validateCapturedColumns(table, originCols)
    assert(errors.size == 1)
    assert(errors.head == "`person`.`scores`.`element` type has changed from INT to BIGINT")
  }

  test("validateCapturedColumns - nested map in struct value type changed") {
    val originStructType = StructType(Seq(
      StructField("name", StringType),
      StructField("attrs", MapType(StringType, IntegerType, valueContainsNull = true))))
    val originCols = Array(
      col("id", LongType, nullable = true),
      col("person", originStructType, nullable = true))
    val currentStructType = StructType(Seq(
      StructField("name", StringType),
      StructField("attrs", MapType(StringType, LongType, valueContainsNull = true))))
    val currentCols = Array(
      col("id", LongType, nullable = true),
      col("person", currentStructType, nullable = true))
    val table = TestTableWithMetadataSupport("test", currentCols)

    val errors = validateCapturedColumns(table, originCols)
    assert(errors.size == 1)
    assert(errors.head == "`person`.`attrs`.`value` type has changed from INT to BIGINT")
  }

  // simple table without metadata column support
  private case class TestTable(
      override val name: String,
      override val columns: Array[Column])
      extends Table {
    override def capabilities: util.Set[TableCapability] = util.Set.of(BATCH_READ)
  }

  // simple table implementation with metadata column support
  private case class TestTableWithMetadataSupport(
      override val name: String,
      override val columns: Array[Column],
      override val metadataColumns: Array[MetadataColumn] = Array.empty)
      extends Table with SupportsMetadataColumns {
    override def capabilities: util.Set[TableCapability] = util.Set.of(BATCH_READ)
  }

  private case class TestMetadataColumn(
      override val name: String,
      override val dataType: DataType,
      override val isNullable: Boolean)
      extends MetadataColumn {
    override def comment: String = s"Test metadata column $name"
    override def metadataInJSON: String = "{}"
  }

  private def validateCapturedColumns(table: Table, originCols: Array[Column]): Seq[String] = {
    V2TableUtil.validateCapturedColumns(table, originCols.toImmutableArraySeq)
  }

  private def col(name: String, dataType: DataType, nullable: Boolean): Column = {
    Column.create(name, dataType, nullable)
  }

  private def metaCol(
      name: String,
      dataType: DataType,
      nullable: Boolean): MetadataColumn = {
    TestMetadataColumn(name, dataType, nullable)
  }
}
