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
