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

package org.apache.spark.sql

import java.nio.file.{Files, Paths}
import java.util.Locale

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.util.Utils

/**
 * End-to-end tests to validate TPC-DS query results against collation-aware
 * modified data and queries.
 *
 * For each collation, table schemas are replicated into two databases in such way that in first
 * DB all table columns are collated with specified collation, while the second DB collates table
 * columns with case-insensitive version of the collation. Tables from first DB are then populated
 * with lowercase-converted data from tpc-ds kit and tables from second DB are populated with
 * randomized-case data.
 *
 * When running arbitrary SQL query, we convert the query to lowercase in order to move
 * all string literals contained in the query to lowercase for execution against first DB,
 * we do this to ensure results are equivalent to case-insensitive collations when queries contain
 * uppercase string literals; second DB receives original unmodified query. Results should compare
 * equal, ignoring case. We use this method to validate collations are working with arbitrary
 * standard SQL constructs.
 *
 * Additionally, we perform trims on string data to properly convert it from CharType
 * to StringType and do sanity checks to verify that results are non-empty as expected.
 *
 * To run this test suite:
 * {{{
 *   SPARK_TPCDS_DATA=<path of TPCDS SF=1 data>
 *     build/sbt "sql/testOnly *TPCDSCollationQueryTestSuite"
 * }}}
 *
 * To run a single test file upon change:
 * {{{
 *   SPARK_TPCDS_DATA=<path of TPCDS SF=1 data>
 *     build/sbt "sql/testOnly *TPCDSCollationQueryTestSuite -- -z q79"
 * }}}
 */
@ExtendedSQLTest
class TPCDSCollationQueryTestSuite extends QueryTest with TPCDSBase with SQLQueryTestHelper {

  private val tpcdsDataPath = sys.env.get("SPARK_TPCDS_DATA")

  // To make output results deterministic
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.SHUFFLE_PARTITIONS.key, "1")
    .remove("spark.hadoop.fs.file.impl")

  protected override def createSparkSession: TestSparkSession = {
    new TestSparkSession(new SparkContext("local[1]", this.getClass.getSimpleName, sparkConf))
  }

  if (tpcdsDataPath.nonEmpty) {
    val nonExistentTables = tableColumns.keys.filterNot { tableName =>
      Files.exists(Paths.get(s"${tpcdsDataPath.get}/$tableName"))
    }
    if (nonExistentTables.nonEmpty) {
      fail(s"Non-existent TPCDS table paths found in ${tpcdsDataPath.get}: " +
        nonExistentTables.mkString(", "))
    }
  }

  private def withDB[T](dbName: String)(fun: => T): T = {
    Utils.tryWithSafeFinally({
      spark.sql(s"USE `$dbName`")
      fun
    }) {
      spark.sql("USE DEFAULT")
    }
  }

  abstract class CollationCheck(
      val dbName: String,
      val collation: String,
      val columnTransform: String) {

    def queryTransform: String => String
  }

  case class CaseInsensitiveCollationCheck(
      override val dbName: String,
      override val collation: String,
      override val columnTransform: String)
    extends CollationCheck(dbName, collation, columnTransform) {

    override def queryTransform: String => String = identity
  }

  case class CaseSensitiveCollationCheck(
      override val dbName: String,
      override val collation: String,
      override val columnTransform: String)
    extends CollationCheck(dbName, collation, columnTransform) {

    override def queryTransform: String => String = _.toLowerCase(Locale.ROOT)
  }

  val randomizeCase = "RANDOMIZE_CASE"

  // List of batches of runs which should yield the same result when run on a query
  val checks: Seq[Seq[CollationCheck]] = Seq(
    Seq(
      CaseSensitiveCollationCheck("tpcds_utf8", "UTF8_BINARY", "lower"),
      CaseInsensitiveCollationCheck("tpcds_utf8_random", "UTF8_LCASE", randomizeCase)
    ),
    Seq(
      CaseSensitiveCollationCheck("tpcds_unicode", "UNICODE", "lower"),
      CaseInsensitiveCollationCheck("tpcds_unicode_random", "UNICODE_CI", randomizeCase)
    )
  )

  override def createTables(): Unit = {
    spark.udf.register(
      randomizeCase,
      functions.udf((s: String) => {
        s match {
          case null => null
          case _ =>
            val random = new scala.util.Random()
            s.map(c => if (random.nextBoolean()) c.toUpper else c.toLower)
        }
      }).asNondeterministic())
    checks.flatten.foreach(check => {
      spark.sql(s"CREATE DATABASE `${check.dbName}`")
      withDB(check.dbName) {
        tableNames.foreach(tableName => {
          val columns = tableColumns(tableName)
            .split("\n")
            .filter(_.trim.nonEmpty)
            .map { column =>
              if (column.trim.split("\\s+").length != 2) {
                throw new IllegalArgumentException(s"Invalid column definition: $column")
              }
              val Array(name, colType) = column.trim.split("\\s+")
              (name, colType.replaceAll(",$", ""))
            }

          spark.sql(
            s"""
               |CREATE TABLE `$tableName` (${collateStringColumns(columns, check.collation)})
               |USING parquet
               |""".stripMargin)

          val transformedColumns = columns.map { case (name, colType) =>
            if (isTextColumn(colType)) {
              // trim to support conversions from CharType
              s"${check.columnTransform}(trim(both from $name)) AS $name"
            } else {
              name
            }
          }.mkString(", ")

          spark.sql(
            s"""
               |INSERT INTO TABLE `$tableName`
               |SELECT $transformedColumns
               |FROM parquet.`${tpcdsDataPath.get}/$tableName`
               |""".stripMargin)
        })
      }
    })
  }

  override def dropTables(): Unit =
    checks.flatten.foreach(check => {
      withDB(check.dbName)(super.dropTables())
      spark.sql(s"DROP DATABASE `${check.dbName}`")
    })

  private def collateStringColumns(
      columns: Array[(String, String)],
      collation: String): String = {
    columns
      .map { case (name, colType) =>
        if (isTextColumn(colType)) {
          s"$name STRING COLLATE $collation"
        } else {
          s"$name $colType"
        }
      }
      .mkString(",\n")
  }

  private def isTextColumn(columnType: String): Boolean = {
    columnType.toUpperCase(Locale.ROOT).contains("CHAR")
  }

  private def runQuery(query: String, conf: Map[String, String], emptyResult: Boolean): Unit = {
    withSQLConf(conf.toSeq: _*) {
      try {
        checks.foreach(batch => {
          val res = batch.map(check =>
            withDB(check.dbName)(getQueryOutput(check.queryTransform(query)).toLowerCase()))
          if (!emptyResult) {
            res.map(queryOutput => assert(queryOutput.nonEmpty))
          }
          if (res.nonEmpty) {
            res.foreach(currRes => assertResult(currRes)(res.head))
          }
        })
      } catch {
        case e: Throwable =>
          val configs = conf.map { case (k, v) =>
            s"$k=$v"
          }
          throw new Exception(s"${e.getMessage}\nError using configs:\n${configs.mkString("\n")}")
      }
    }
  }

  private def getQueryOutput(query: String): String = {
    val (_, output) = handleExceptions(getNormalizedQueryExecutionResult(spark, query))
    output.mkString("\n").replaceAll("\\s+$", "")
  }

  // skip q91 due to use of like expression which is not supported with collations yet
  override def excludedTpcdsQueries: Set[String] = super.excludedTpcdsQueries ++ Set("q91")

  // Skip checks on queries which produce empty set of rows
  val emptyResults: Set[String] = Set("q17", "q23b", "q24a", "q24b", "q25", "q54")
  val emptyResultsV2_7_0: Set[String] = Set("q24", "q78")

  if (tpcdsDataPath.nonEmpty) {
    tpcdsQueries.foreach { name =>
      val queryString = resourceToString(
        s"tpcds/$name.sql",
        classLoader = Thread.currentThread().getContextClassLoader)
      test(name)(runQuery(queryString, Map.empty, emptyResults.contains(name)))
    }

    tpcdsQueriesV2_7_0.foreach { name =>
      val queryString = resourceToString(
        s"tpcds-v2.7.0/$name.sql",
        classLoader = Thread.currentThread().getContextClassLoader)
      test(s"$name-v2.7")(runQuery(queryString, Map.empty, emptyResultsV2_7_0.contains(name)))
    }
  } else {
    ignore("skipped because env 'SPARK_TPCDS_DATA' is not set") {}
  }
}
