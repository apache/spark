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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.schema.MessageTypeParser

import org.apache.spark.{SparkArrayIndexOutOfBoundsException, SparkConf}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.SchemaPruningSuite
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.tags.ExtendedSQLTest

abstract class ParquetSchemaPruningSuite extends SchemaPruningSuite with AdaptiveSparkPlanHelper {
  override protected val dataSourceName: String = "parquet"
  override protected val vectorizedReaderEnabledKey: String =
    SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key
  override protected val vectorizedReaderNestedEnabledKey: String =
    SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key

  test("dynamic array selection preserves two-level Parquet LIST elements after pruning") {
    withTempPath { file =>
      // After clipping legacy to just wanted, both LISTs have identical repeated groups.
      val fileSchema = MessageTypeParser.parseMessageType(
        """message m {
          |  optional group legacy (LIST) {
          |    repeated group entries { optional int32 sibling = 18; optional int32 wanted = 17; }
          |  }
          |  optional group standard (LIST) {
          |    repeated group entries { optional int32 wanted = 17; }
          |  }
          |  optional int32 idx;
          |}""".stripMargin)
      val writer = ExampleParquetWriter.builder(new Path(file.toURI))
        .withConf(spark.sessionState.newHadoopConf()).withType(fileSchema).build()
      try {
        val row = new SimpleGroupFactory(fileSchema).newGroup().append("idx", 1)
        val legacy = row.addGroup("legacy")
        legacy.addGroup("entries").append("sibling", 10).append("wanted", 11)
        legacy.addGroup("entries").append("sibling", 20).append("wanted", 22)
        val standard = row.addGroup("standard")
        standard.addGroup("entries").append("wanted", 30)
        standard.addGroup("entries").append("wanted", 33)
        writer.write(row)
      } finally {
        writer.close()
      }
      val fieldId = new MetadataBuilder().putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 17).build()
      val missingId = new MetadataBuilder().putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 19).build()
      val element = new StructType()
        .add("sibling", IntegerType, nullable = true, metadata = missingId)
        .add("renamed", IntegerType, nullable = true, metadata = fieldId)
      val readSchema = new StructType().add("legacy", ArrayType(element))
        .add("standard", ArrayType(IntegerType)).add("idx", IntegerType)
      Seq("true", "false").foreach { vectorized =>
        withSQLConf(
          vectorizedReaderEnabledKey -> vectorized,
          SQLConf.PARQUET_FIELD_ID_READ_ENABLED.key -> "true",
          SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
          val data = spark.read.schema(readSchema).parquet(file.getCanonicalPath)
          val query = data.selectExpr("legacy[idx].renamed", "standard[idx]")
          checkScanSchemata(query,
            "struct<legacy:array<struct<renamed:int>>,standard:array<int>,idx:int>")
          checkAnswer(query, Row(22, 33))

          // Only the missing ID is requested. The first physical field is read for levels
          // and shares its name, but has a different ID.
          checkAnswer(data.selectExpr("legacy[idx].sibling"), Row(null))
        }
      }
    }
  }

  test("a two-level Parquet LIST preserves enclosing struct nullness after pruning") {
    withTempPath { file =>
      val fileSchema = MessageTypeParser.parseMessageType(
        """message m {
          |  optional group record {
          |    optional group items (LIST) {
          |      repeated group entries { optional int32 x; optional int32 y; }
          |    }
          |  }
          |}""".stripMargin)
      val writer = ExampleParquetWriter.builder(new Path(file.toURI))
        .withConf(spark.sessionState.newHadoopConf()).withType(fileSchema).build()
      val factory = new SimpleGroupFactory(fileSchema)
      try {
        val row = factory.newGroup()
        row.addGroup("record").addGroup("items").addGroup("entries").append("x", 1).append("y", 2)
        writer.write(row)
        writer.write(factory.newGroup())
      } finally {
        writer.close()
      }
      Seq("true", "false").foreach { vectorized =>
        withSQLConf(vectorizedReaderEnabledKey -> vectorized) {
          // The only physical child available for the record's levels is the clipped LIST.
          val data = spark.read.schema("record STRUCT<missing: INT>").parquet(file.getCanonicalPath)
          checkAnswer(data.selectExpr("record.missing", "record IS NULL"),
            Seq(Row(null, false), Row(null, true)))
        }
      }
    }
  }

  test("dynamic array selection preserves root and nested Parquet field IDs") {
    withSQLConf(
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true",
      SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key -> "true",
      SQLConf.PARQUET_FIELD_ID_READ_ENABLED.key -> "true") {
      withTempPath { dir =>
        val rootId = new MetadataBuilder()
          .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 1).build()
        val leafId = new MetadataBuilder()
          .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 17).build()
        val leafType = new StructType()
          .add("old_id", IntegerType, nullable = true, metadata = leafId)
          .add("payload", StringType)
        def schema(rootName: String, leaf: StructType): StructType = new StructType()
          .add(rootName, ArrayType(new StructType().add("children", ArrayType(leaf))),
            nullable = true, metadata = rootId)
          .add("idx", IntegerType)
        val writeSchema = schema("old_items", leafType)
        val readSchema = schema("new_items", StructType(Seq(
          leafType("old_id").copy(name = "new_id"), leafType("payload"))))
        val rows = Seq(Row(Seq(Row(Seq(Row(42, "unused")))), 0))
        val path = dir.getCanonicalPath
        spark.createDataFrame(spark.sparkContext.parallelize(rows), writeSchema)
          .write.parquet(path)

        Seq("true", "false").foreach { vectorized =>
          withSQLConf(vectorizedReaderEnabledKey -> vectorized) {
            val query = spark.read.schema(readSchema).parquet(path)
              .selectExpr("new_items[idx].children.new_id")
            checkScanSchemata(query,
              "struct<new_items:array<struct<children:array<struct<new_id:int>>>>,idx:int>")
            checkAnswer(query, Row(Seq(42)))
          }
        }
      }
    }
  }

  test("dynamic lookups preserve complex column defaults and constant lookups still prune") {
    withTempPath { dir =>
      withTable("t") {
        val path = dir.getCanonicalPath
        sql(
          s"""CREATE TABLE t (idx INT, k STRING,
             |  items ARRAY<STRUCT<a: INT, b: INT>> DEFAULT array(),
             |  byKey MAP<STRING, STRUCT<a: INT, b: INT>> DEFAULT map()
             |) USING parquet LOCATION '$path'""".stripMargin)
        sql("INSERT INTO t SELECT 0, 'k', array(named_struct('a', 5, 'b', 6)), " +
          "map('k', named_struct('a', 7, 'b', 8))")
        sql(
          """ALTER TABLE t ADD COLUMNS (
            |  arr ARRAY<STRUCT<a: INT, b: INT>> DEFAULT array(named_struct('a', 11, 'b', 22)),
            |  m MAP<STRING, STRUCT<a: INT, b: INT>>
            |    DEFAULT map('k', named_struct('a', 33, 'b', 44))
            |)""".stripMargin)

        val data = spark.read.schema(spark.table("t").schema).parquet(path)
        Seq("true", "false").foreach { vectorized =>
          withSQLConf(vectorizedReaderEnabledKey -> vectorized) {
            val query = data.selectExpr("arr[idx].b", "m[k].b", "items[0].b", "byKey['k'].b")
            checkAnswer(query, Row(22, 44, 6, 8))
            checkScanSchemata(query, "struct<idx:int,k:string,items:array<struct<b:int>>," +
              "byKey:map<string,struct<b:int>>,arr:array<struct<a:int,b:int>>," +
              "m:map<string,struct<a:int,b:int>>>")
          }
        }
      }
    }
  }

  test("dynamic lookups preserve defaults on nested fields") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      sql(
        """SELECT 0 AS i,
          |  named_struct('arr', array(named_struct('a', 1, 'b', 22))) AS s,
          |  map(0, named_struct('anchor', 7, 'n', named_struct('a', 3, 'b', 44))) AS m
          |""".stripMargin).write.parquet(path)

      val pair = StructType.fromDDL("a INT, b INT")
      val default = "named_struct('a', 5, 'b', 6)"
      val arr = StructField("arr", ArrayType(pair)).withExistenceDefaultValue(s"array($default)")
      val n = StructField("n", pair).withExistenceDefaultValue(default)
      val schema = new StructType()
        .add("i", IntegerType)
        .add("s", new StructType().add(arr))
        .add("m", MapType(IntegerType, new StructType().add("anchor", IntegerType).add(n)))

      withSQLConf(vectorizedReaderEnabledKey -> "false") {
        val data = spark.read.schema(schema).parquet(path)
        checkAnswer(data.selectExpr("s.arr[i].b", "m[i].n.b"), Row(22, 44))
        checkAnswer(data.selectExpr("m[i].anchor", "m[0].n.b"), Row(7, 44))
      }
    }
  }

  test("dynamic array selection preserves bounds in legacy missing-field mode") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      sql("SELECT array(named_struct('present', 1)) AS items, 1 AS idx").write.parquet(path)
      val legacy = SQLConf.LEGACY_PARQUET_RETURN_NULL_STRUCT_IF_ALL_FIELDS_MISSING.key
      for (vectorized <- Seq("true", "false")) {
        withSQLConf(legacy -> "true", SQLConf.ANSI_ENABLED.key -> "true",
            vectorizedReaderEnabledKey -> vectorized) {
          val query = spark.read
            .schema("items ARRAY<STRUCT<present: INT, missing: INT>>, idx INT")
            .parquet(path).selectExpr("items[idx].missing")
          checkError(
            exception = intercept[SparkArrayIndexOutOfBoundsException](query.collect()),
            condition = "INVALID_ARRAY_INDEX",
            parameters = Map("indexValue" -> "1", "arraySize" -> "1"),
            context = ExpectedContext(fragment = "items[idx]", start = 0, stop = 9))
        }
      }
    }
  }
}

@ExtendedSQLTest
class ParquetV1SchemaPruningSuite extends ParquetSchemaPruningSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "parquet")

  test("SPARK-59171: SchemaPruning is idempotent with metadata and a filtered variant") {
    withTempPath { path =>
      spark.range(10)
        .selectExpr("parse_json(cast(id as string)) as v")
        .write.parquet(path.getCanonicalPath)

      val alwaysTrue = udf(() => true).asNondeterministic()
      val query = spark.read.parquet(path.getCanonicalPath)
        .where("v::int = 3")
        .select(col("_metadata.file_path"))
        .filter(alwaysTrue())
        .distinct()

      assert(query.collect().nonEmpty)
    }
  }
}

@ExtendedSQLTest
class ParquetV2SchemaPruningSuite extends ParquetSchemaPruningSuite {
  // TODO: enable Parquet V2 write path after file source V2 writers are workable.
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")

  test("row reader uses the scan's field ID setting") {
    withTempPath { dir =>
      def fieldId(id: Long): Metadata =
        new MetadataBuilder().putLong(ParquetUtils.FIELD_ID_METADATA_KEY, id).build()
      val path = dir.getCanonicalPath
      spark.range(1).select(col("id").as("x", fieldId(1))).write.parquet(path)
      val schema = new StructType().add("x", LongType, nullable = true, metadata = fieldId(2))
      val key = SQLConf.PARQUET_FIELD_ID_READ_ENABLED.key
      for (useIds <- Seq(true, false)) {
        withSQLConf(vectorizedReaderEnabledKey -> "false", key -> (!useIds).toString) {
          val data = spark.read.option(key, useIds).schema(schema).parquet(path)
          checkAnswer(data, if (useIds) Row(null) else Row(0L))
        }
      }
    }
  }

  override def checkScanSchemata(df: DataFrame, expectedSchemaCatalogStrings: String*): Unit = {
    val fileSourceScanSchemata =
      collect(df.queryExecution.executedPlan) {
        case scan: BatchScanExec => scan.scan.asInstanceOf[ParquetScan].readDataSchema
      }
    assert(fileSourceScanSchemata.size === expectedSchemaCatalogStrings.size,
      s"Found ${fileSourceScanSchemata.size} file sources in dataframe, " +
        s"but expected $expectedSchemaCatalogStrings")
    fileSourceScanSchemata.zip(expectedSchemaCatalogStrings).foreach {
      case (scanSchema, expectedScanSchemaCatalogString) =>
        val expectedScanSchema = CatalystSqlParser.parseDataType(expectedScanSchemaCatalogString)
        implicit val equality = schemaEquality
        assert(scanSchema === expectedScanSchema)
    }
  }
}
