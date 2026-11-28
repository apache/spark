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

import java.util

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsOverwrite, V1Write, WriteBuilder}
import org.apache.spark.sql.functions.{coalesce, expr, lit, not}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{Filter, InsertableRelation}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class DataSourceV2BooleanReplaceWhereSuite extends QueryTest with SharedSparkSession {
  private val provider = classOf[InMemoryV1Provider].getName

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
      classOf[BooleanReplaceWhereCatalog].getName)
  }

  override def afterEach(): Unit = {
    try {
      InMemoryV1Provider.clear()
    } finally {
      super.afterEach()
    }
  }

  private object ReplaceWhereApi extends Enumeration {
    val Sql = Value("INSERT REPLACE WHERE")
    val DataFrameWriter = Value("DataFrameWriter.option(replaceWhere)")
    val DataFrameWriterV2 = Value("DataFrameWriterV2.overwrite")
  }

  for {
    writeApi <- ReplaceWhereApi.values
    flagEnabled <- Seq(false, true)
  } {
    // Off: SQL/V2 error and keep all rows; on: replace the true row. Option works in both states.
    test("Overwrite: boolean REPLACE WHERE b <=> true, " +
        s"$writeApi, flagEnabled=$flagEnabled") {
      withSQLConf(
          SQLConf.V2_EXPRESSION_BUILDER_PRESERVE_BOOLEAN_LITERALS_ENABLED.key ->
            flagEnabled.toString) {
        val table = s"$SESSION_CATALOG_NAME.default.rw_${util.UUID.randomUUID()}"
          .replace("-", "_")
        withTable(table) {
          sql(s"CREATE TABLE $table (id INT, b BOOLEAN) USING $provider")
          sql(s"INSERT INTO $table VALUES (1, true), (2, false), (3, NULL)")
          val condition = "b <=> true"
          val source = "SELECT 4 AS id, true AS b"
          val replacement = sql(source)

          def overwrite(): Unit = {
            writeApi match {
              case ReplaceWhereApi.Sql =>
                sql(s"INSERT INTO $table REPLACE WHERE $condition $source")
              case ReplaceWhereApi.DataFrameWriter =>
                replacement.write.format(provider).mode("overwrite")
                  .option("replaceWhere", condition)
                  .option("name", table.stripPrefix(s"$SESSION_CATALOG_NAME.")).save()
              case ReplaceWhereApi.DataFrameWriterV2 =>
                replacement.writeTo(table).overwrite(expr(condition))
            }
          }

          if (flagEnabled || writeApi == ReplaceWhereApi.DataFrameWriter) {
            overwrite()
            checkAnswer(spark.table(table), Seq(Row(2, false), Row(3, null), Row(4, true)))
          } else {
            val error = intercept[SparkException] {
              overwrite()
            }
            assert(error.getMessage.contains("Table does not support overwrite by expression"))
            checkAnswer(spark.table(table), Seq(Row(1, true), Row(2, false), Row(3, null)))
          }
        }
      }
    }

    // Off: SQL/V2 error and keep all rows; on: replace the true row. Option works in both states.
    test("Overwrite: boolean REPLACE WHERE true <=> b, " +
        s"$writeApi, flagEnabled=$flagEnabled") {
      withSQLConf(
          SQLConf.V2_EXPRESSION_BUILDER_PRESERVE_BOOLEAN_LITERALS_ENABLED.key ->
            flagEnabled.toString) {
        val table = s"$SESSION_CATALOG_NAME.default.rw_${util.UUID.randomUUID()}"
          .replace("-", "_")
        withTable(table) {
          sql(s"CREATE TABLE $table (id INT, b BOOLEAN) USING $provider")
          sql(s"INSERT INTO $table VALUES (1, true), (2, false), (3, NULL)")
          val condition = "true <=> b"
          val source = "SELECT 4 AS id, true AS b"
          val replacement = sql(source)

          def overwrite(): Unit = {
            writeApi match {
              case ReplaceWhereApi.Sql =>
                sql(s"INSERT INTO $table REPLACE WHERE $condition $source")
              case ReplaceWhereApi.DataFrameWriter =>
                replacement.write.format(provider).mode("overwrite")
                  .option("replaceWhere", condition)
                  .option("name", table.stripPrefix(s"$SESSION_CATALOG_NAME.")).save()
              case ReplaceWhereApi.DataFrameWriterV2 =>
                replacement.writeTo(table).overwrite(expr(condition))
            }
          }

          if (flagEnabled || writeApi == ReplaceWhereApi.DataFrameWriter) {
            overwrite()
            checkAnswer(spark.table(table), Seq(Row(2, false), Row(3, null), Row(4, true)))
          } else {
            val error = intercept[SparkException] {
              overwrite()
            }
            assert(error.getMessage.contains("Table does not support overwrite by expression"))
            checkAnswer(spark.table(table), Seq(Row(1, true), Row(2, false), Row(3, null)))
          }
        }
      }
    }

    // Off: SQL/V2 error and keep all rows; on: replace the false row. Option works in both states.
    test("Overwrite: boolean REPLACE WHERE b <=> false, " +
        s"$writeApi, flagEnabled=$flagEnabled") {
      withSQLConf(
          SQLConf.V2_EXPRESSION_BUILDER_PRESERVE_BOOLEAN_LITERALS_ENABLED.key ->
            flagEnabled.toString) {
        val table = s"$SESSION_CATALOG_NAME.default.rw_${util.UUID.randomUUID()}"
          .replace("-", "_")
        withTable(table) {
          sql(s"CREATE TABLE $table (id INT, b BOOLEAN) USING $provider")
          sql(s"INSERT INTO $table VALUES (1, true), (2, false), (3, NULL)")
          val condition = "b <=> false"
          val source = "SELECT 4 AS id, false AS b"
          val replacement = sql(source)

          def overwrite(): Unit = {
            writeApi match {
              case ReplaceWhereApi.Sql =>
                sql(s"INSERT INTO $table REPLACE WHERE $condition $source")
              case ReplaceWhereApi.DataFrameWriter =>
                replacement.write.format(provider).mode("overwrite")
                  .option("replaceWhere", condition)
                  .option("name", table.stripPrefix(s"$SESSION_CATALOG_NAME.")).save()
              case ReplaceWhereApi.DataFrameWriterV2 =>
                replacement.writeTo(table).overwrite(expr(condition))
            }
          }

          if (flagEnabled || writeApi == ReplaceWhereApi.DataFrameWriter) {
            overwrite()
            checkAnswer(spark.table(table), Seq(Row(1, true), Row(3, null), Row(4, false)))
          } else {
            val error = intercept[SparkException] {
              overwrite()
            }
            assert(error.getMessage.contains("Table does not support overwrite by expression"))
            checkAnswer(spark.table(table), Seq(Row(1, true), Row(2, false), Row(3, null)))
          }
        }
      }
    }

    // Off: SQL/V2 error and keep all rows; on: replace false and null rows.
    // Option replaces both rows in either state.
    test("Overwrite: boolean REPLACE WHERE NOT (b <=> true), " +
        s"$writeApi, flagEnabled=$flagEnabled") {
      withSQLConf(
          SQLConf.V2_EXPRESSION_BUILDER_PRESERVE_BOOLEAN_LITERALS_ENABLED.key ->
            flagEnabled.toString) {
        val table = s"$SESSION_CATALOG_NAME.default.rw_${util.UUID.randomUUID()}"
          .replace("-", "_")
        withTable(table) {
          sql(s"CREATE TABLE $table (id INT, b BOOLEAN) USING $provider")
          sql(s"INSERT INTO $table VALUES (1, true), (2, false), (3, NULL)")
          val condition = "NOT (b <=> true)"
          val source = "SELECT 4 AS id, false AS b"
          val replacement = sql(source)

          def overwrite(): Unit = {
            writeApi match {
              case ReplaceWhereApi.Sql =>
                sql(s"INSERT INTO $table REPLACE WHERE $condition $source")
              case ReplaceWhereApi.DataFrameWriter =>
                replacement.write.format(provider).mode("overwrite")
                  .option("replaceWhere", condition)
                  .option("name", table.stripPrefix(s"$SESSION_CATALOG_NAME.")).save()
              case ReplaceWhereApi.DataFrameWriterV2 =>
                replacement.writeTo(table).overwrite(expr(condition))
            }
          }

          if (flagEnabled || writeApi == ReplaceWhereApi.DataFrameWriter) {
            overwrite()
            checkAnswer(spark.table(table), Seq(Row(1, true), Row(4, false)))
          } else {
            val error = intercept[SparkException] {
              overwrite()
            }
            assert(error.getMessage.contains("Table does not support overwrite by expression"))
            checkAnswer(spark.table(table), Seq(Row(1, true), Row(2, false), Row(3, null)))
          }
        }
      }
    }

    // Off: SQL/V2 error and keep all rows; on: replace non-null rows. Option works in both states.
    test("Overwrite: boolean REPLACE WHERE b IN (true, false), " +
        s"$writeApi, flagEnabled=$flagEnabled") {
      withSQLConf(
          SQLConf.V2_EXPRESSION_BUILDER_PRESERVE_BOOLEAN_LITERALS_ENABLED.key ->
            flagEnabled.toString) {
        val table = s"$SESSION_CATALOG_NAME.default.rw_${util.UUID.randomUUID()}"
          .replace("-", "_")
        withTable(table) {
          sql(s"CREATE TABLE $table (id INT, b BOOLEAN) USING $provider")
          sql(s"INSERT INTO $table VALUES (1, true), (2, false), (3, NULL)")
          val condition = "b IN (true, false)"
          val source = "SELECT 4 AS id, true AS b"
          val replacement = sql(source)

          def overwrite(): Unit = {
            writeApi match {
              case ReplaceWhereApi.Sql =>
                sql(s"INSERT INTO $table REPLACE WHERE $condition $source")
              case ReplaceWhereApi.DataFrameWriter =>
                replacement.write.format(provider).mode("overwrite")
                  .option("replaceWhere", condition)
                  .option("name", table.stripPrefix(s"$SESSION_CATALOG_NAME.")).save()
              case ReplaceWhereApi.DataFrameWriterV2 =>
                replacement.writeTo(table).overwrite(expr(condition))
            }
          }

          if (flagEnabled || writeApi == ReplaceWhereApi.DataFrameWriter) {
            overwrite()
            checkAnswer(spark.table(table), Seq(Row(3, null), Row(4, true)))
          } else {
            val error = intercept[SparkException] {
              overwrite()
            }
            assert(error.getMessage.contains("Table does not support overwrite by expression"))
            checkAnswer(spark.table(table), Seq(Row(1, true), Row(2, false), Row(3, null)))
          }
        }
      }
    }

    // Off: SQL/V2 keep the true row; on: replace it. Option replaces it in both states.
    test("Overwrite: boolean REPLACE WHERE b <=> true OR id = 4, " +
        s"$writeApi, flagEnabled=$flagEnabled") {
      withSQLConf(
          SQLConf.V2_EXPRESSION_BUILDER_PRESERVE_BOOLEAN_LITERALS_ENABLED.key ->
            flagEnabled.toString) {
        val table = s"$SESSION_CATALOG_NAME.default.rw_${util.UUID.randomUUID()}"
          .replace("-", "_")
        withTable(table) {
          sql(s"CREATE TABLE $table (id INT, b BOOLEAN) USING $provider")
          sql(s"INSERT INTO $table VALUES (1, true), (2, false), (3, NULL)")
          val condition = "b <=> true OR id = 4"
          val source = "SELECT 4 AS id, true AS b"
          val replacement = sql(source)

          def overwrite(): Unit = {
            writeApi match {
              case ReplaceWhereApi.Sql =>
                sql(s"INSERT INTO $table REPLACE WHERE $condition $source")
              case ReplaceWhereApi.DataFrameWriter =>
                replacement.write.format(provider).mode("overwrite")
                  .option("replaceWhere", condition)
                  .option("name", table.stripPrefix(s"$SESSION_CATALOG_NAME.")).save()
              case ReplaceWhereApi.DataFrameWriterV2 =>
                replacement.writeTo(table).overwrite(expr(condition))
            }
          }

          overwrite()
          if (flagEnabled || writeApi == ReplaceWhereApi.DataFrameWriter) {
            checkAnswer(spark.table(table), Seq(Row(2, false), Row(3, null), Row(4, true)))
          } else {
            checkAnswer(
              spark.table(table),
              Seq(Row(1, true), Row(2, false), Row(3, null), Row(4, true)))
          }
        }
      }
    }

    // Off/on: all three APIs replace all rows for a constant true predicate.
    test("Overwrite: boolean REPLACE WHERE true, " +
        s"$writeApi, flagEnabled=$flagEnabled") {
      withSQLConf(
          SQLConf.V2_EXPRESSION_BUILDER_PRESERVE_BOOLEAN_LITERALS_ENABLED.key ->
            flagEnabled.toString) {
        val table = s"$SESSION_CATALOG_NAME.default.rw_${util.UUID.randomUUID()}"
          .replace("-", "_")
        withTable(table) {
          sql(s"CREATE TABLE $table (id INT, b BOOLEAN) USING $provider")
          sql(s"INSERT INTO $table VALUES (1, true), (2, false), (3, NULL)")
          val condition = "true"
          val source = "SELECT 4 AS id, true AS b"
          val replacement = sql(source)

          writeApi match {
            case ReplaceWhereApi.Sql =>
              sql(s"INSERT INTO $table REPLACE WHERE $condition $source")
            case ReplaceWhereApi.DataFrameWriter =>
              replacement.write.format(provider).mode("overwrite")
                .option("replaceWhere", condition)
                .option("name", table.stripPrefix(s"$SESSION_CATALOG_NAME.")).save()
            case ReplaceWhereApi.DataFrameWriterV2 =>
              replacement.writeTo(table).overwrite(expr(condition))
          }
          checkAnswer(spark.table(table), Seq(Row(4, true)))
        }
      }
    }
  }
}

class BooleanReplaceWhereCatalog extends V1FallbackTableCatalog {
  override def newTable(
      name: String,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): InMemoryTableWithV1Fallback = {
    val table = new BooleanReplaceWhereTable(name, schema, partitions, properties)
    InMemoryV1Provider.tables.put(name, table)
    table
  }
}

private class BooleanReplaceWhereTable(
    name: String,
    schema: StructType,
    partitions: Array[Transform],
    properties: util.Map[String, String])
  extends InMemoryTableWithV1Fallback(name, schema, partitions, properties) {

  private var rows = Seq.empty[Row]

  override def getData: Seq[Row] = rows

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new WriteBuilder with SupportsOverwrite {
      private var condition: Option[String] = None

      override def truncate(): WriteBuilder = {
        condition = Some(Option(info.options().get("replaceWhere")).getOrElse("true"))
        this
      }

      override def overwrite(filters: Array[Filter]): WriteBuilder = {
        condition = Some(filters.map(_.toV2.describe()).mkString(" AND "))
        this
      }

      override def build(): V1Write = new V1Write {
        override def toInsertableRelation: InsertableRelation = {
          (data, overwrite) => {
            assert(!overwrite, "V1 write fallbacks cannot be called with overwrite=true")
            val retained = condition.map { predicate =>
              data.sparkSession.createDataFrame(rows.asJava, schema)
                .filter(not(coalesce(expr(predicate), lit(false)))).collect().toSeq
            }.getOrElse(rows)
            rows = retained ++ data.collect().toSeq
          }
        }
      }
    }
  }
}
