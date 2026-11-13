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

import org.apache.spark.SparkConf
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

  test("SPARK-60788: a pruned two-level LIST preserves its element type and field IDs") {
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
          |}""".stripMargin)
      val writer = ExampleParquetWriter.builder(new Path(file.toURI))
        .withConf(spark.sessionState.newHadoopConf()).withType(fileSchema).build()
      try {
        val row = new SimpleGroupFactory(fileSchema).newGroup()
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
        .add("standard", ArrayType(IntegerType))
      Seq("true", "false").foreach { vectorized =>
        withSQLConf(
          vectorizedReaderEnabledKey -> vectorized,
          SQLConf.PARQUET_FIELD_ID_READ_ENABLED.key -> "true",
          SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
          val data = spark.read.schema(readSchema).parquet(file.getCanonicalPath)
          val query = data.selectExpr("legacy[1].renamed", "standard[1]")
          checkScanSchemata(query,
            "struct<legacy:array<struct<renamed:int>>,standard:array<int>>")
          checkAnswer(query, Row(22, 33))

          // Only the missing ID is requested. The first physical field is read for levels
          // and shares its name, but has a different ID.
          checkAnswer(data.selectExpr("legacy.sibling"), Row(Seq(null, null)))
        }
      }
    }
  }

  test("SPARK-60788: prune a two-level LIST nested inside another two-level LIST") {
    withTempPath { file =>
      val fileSchema = MessageTypeParser.parseMessageType(
        """message m {
          |  optional group outer (LIST) {
          |    repeated group o {
          |      optional group inner (LIST) {
          |        repeated group i { optional int32 a; optional int32 b; }
          |      }
          |      optional int32 z;
          |    }
          |  }
          |}""".stripMargin)
      val writer = ExampleParquetWriter.builder(new Path(file.toURI))
        .withConf(spark.sessionState.newHadoopConf()).withType(fileSchema).build()
      try {
        val row = new SimpleGroupFactory(fileSchema).newGroup()
        val inner = row.addGroup("outer").addGroup("o").addGroup("inner")
        inner.addGroup("i").append("a", 1).append("b", 2)
        inner.addGroup("i").append("a", 3).append("b", 4)
        writer.write(row)
      } finally {
        writer.close()
      }
      Seq("true", "false").foreach { vectorized =>
        withSQLConf(vectorizedReaderEnabledKey -> vectorized) {
          val data = spark.read.parquet(file.getCanonicalPath)
          val query = data.selectExpr("outer[0].inner[1].a")
          checkScanSchemata(query, "struct<outer:array<struct<inner:array<struct<a:int>>>>>")
          checkAnswer(query, Row(3))
        }
      }
    }
  }

  test("SPARK-60788: a two-level LIST preserves enclosing struct nullness after pruning") {
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

  // V1 uses the session setting for both clipping and conversion. Only V2 honors the scan option.
  test("SPARK-60788: row reader uses the scan's field ID setting") {
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
