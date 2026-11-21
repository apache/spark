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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.SchemaPruningSuite
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.DataSourceV2Test
import org.apache.spark.tags.ExtendedSQLTest

abstract class ParquetSchemaPruningSuite extends SchemaPruningSuite with AdaptiveSparkPlanHelper {
  override protected val dataSourceName: String = "parquet"
  override protected val vectorizedReaderEnabledKey: String =
    SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key
  override protected val vectorizedReaderNestedEnabledKey: String =
    SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key

  for {
    vectorized <- Seq(false, true)
    (projection, oldValue, newValue) <- Seq(
      ("s.b", 22, 99),
      ("arr[0].b", 22, 99),
      ("arr.b", Seq(22, null, null), Seq(99, null, null)),
      ("m['key'].b", 22, 99))
  } {
    test(s"SPARK-60790: prune existence default for $projection, vectorized: $vectorized") {
      withSQLConf(
          SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true",
          vectorizedReaderEnabledKey -> vectorized.toString,
          vectorizedReaderNestedEnabledKey -> "true") {
        withTable("nested_defaults") {
          // Nullable complex defaults are covered by the row reader. The vectorized reader
          // cannot append null elements inside a complex default yet.
          val nullElements = if (vectorized) {
            ""
          } else {
            ", CAST(NULL AS STRUCT<a:INT,b:INT>), named_struct('a',0,'b',CAST(NULL AS INT))"
          }
          val nullEntry = if (vectorized) "" else ", 'nil',CAST(NULL AS STRUCT<a:INT,b:INT>)"
          def arrayValue(first: Row): Seq[Row] =
            if (vectorized) Seq(first) else Seq(first, null, Row(0, null))
          def mapValue(first: Row): Map[String, Row] =
            if (vectorized) Map("key" -> first) else Map("key" -> first, "nil" -> null)

          sql("CREATE TABLE nested_defaults (id INT) USING parquet")
          sql("INSERT INTO nested_defaults VALUES (0)")
          sql(s"""ALTER TABLE nested_defaults ADD COLUMNS (
                |  s STRUCT<a:INT,b:INT> DEFAULT named_struct('a',11,'b',22),
                |  arr ARRAY<STRUCT<a:INT,b:INT>> DEFAULT array(
                |    named_struct('a',11,'b',22)$nullElements),
                |  m MAP<STRING,STRUCT<a:INT,b:INT>> DEFAULT map(
                |    'key',named_struct('a',11,'b',22)$nullEntry))""".stripMargin)
          sql(s"""INSERT INTO nested_defaults VALUES (
                |  1, named_struct('a',33,'b',99),
                |  array(named_struct('a',33,'b',99)$nullElements),
                |  map('key',named_struct('a',33,'b',99)$nullEntry))""".stripMargin)

          val tableSchema = spark.table("nested_defaults").schema
          val path = spark.sessionState.catalog.getTableMetadata(
            TableIdentifier("nested_defaults")).location.toString
          val data = spark.read.schema(tableSchema).parquet(path)
          checkAnswer(data, Seq(
            Row(0, Row(11, 22), arrayValue(Row(11, 22)), mapValue(Row(11, 22))),
            Row(1, Row(33, 99), arrayValue(Row(33, 99)), mapValue(Row(33, 99)))))
          val query = data.selectExpr("id", projection)
          val oldExpected = if (vectorized && projection == "arr.b") Seq(22) else oldValue
          val newExpected = if (vectorized && projection == "arr.b") Seq(99) else newValue
          checkAnswer(query, Seq(Row(0, oldExpected), Row(1, newExpected)))
          val readSchema = projection match {
            case "s.b" => "struct<id:int,s:struct<b:int>>"
            case "m['key'].b" => "struct<id:int,m:map<string,struct<b:int>>>"
            case _ => "struct<id:int,arr:array<struct<b:int>>>"
          }
          checkScanSchemata(query, readSchema)
        }
      }
    }
  }

  test("SPARK-60790: project default map keys without deduplicating them") {
    withSQLConf(
        SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true",
        vectorizedReaderEnabledKey -> "false") {
      withTable("map_defaults") {
        val default = "map(named_struct('a',1,'b',2),named_struct('c',3,'d',4)," +
          "named_struct('a',5,'b',2),named_struct('c',7,'d',6))"
        sql("CREATE TABLE map_defaults (id INT) USING parquet")
        sql("INSERT INTO map_defaults VALUES (0)")
        sql(s"""ALTER TABLE map_defaults ADD COLUMNS (
               |m MAP<STRUCT<a:INT,b:INT>,STRUCT<c:INT,d:INT>> DEFAULT $default)""".stripMargin)
        sql(s"INSERT INTO map_defaults VALUES (1, $default)")
        val schema = spark.table("map_defaults").schema
        val path = spark.sessionState.catalog.getTableMetadata(
          TableIdentifier("map_defaults")).location.toString
        val query = spark.read.schema(schema).parquet(path).selectExpr("id", "map_keys(m).b")
        checkAnswer(query, Seq(
          Row(0, Seq(2, 2)), Row(1, Seq(2, 2))))
        checkScanSchemata(query, "struct<id:int,m:map<struct<b:int>,struct<c:int,d:int>>>")
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

@DataSourceV2Test
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
