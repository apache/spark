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

package org.apache.spark.sql.execution.datasources.v2.adbc

import java.sql.Date
import java.util
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Using

import org.apache.arrow.adbc.driver.jdbc.JdbcDriverFactory
import org.apache.arrow.adbc.drivermanager.AdbcDriverFactory
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, Field, FieldType, Schema}
import org.apache.logging.log4j.Level
import org.mockito.Mockito.{doThrow, mock}

import org.apache.spark.{SparkException, SparkIllegalArgumentException, SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.execution.ExtendedMode
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils

class AdbcDataSourceSuite
  extends QueryTest
  with SharedSparkSession
  with AdbcTestBase
  with AdaptiveSparkPlanHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    execJdbc(
      """CREATE TABLE "people" ("id" INTEGER NOT NULL, "name" VARCHAR(32),
        |"score" DOUBLE, "active" BOOLEAN, "balance" DECIMAL(10, 2), "born" DATE,
        |"big" BIGINT)""".stripMargin,
      """INSERT INTO "people" VALUES
        |(1, 'alice', 1.5, TRUE, 10.25, DATE '2000-01-02', 10000000000),
        |(2, 'bob', NULL, FALSE, NULL, NULL, NULL),
        |(3, NULL, 3.5, NULL, -3.50, DATE '1999-12-31', -1)""".stripMargin,
      """CREATE TABLE "odd names" ("my col" INTEGER, "a""b" VARCHAR(8))""",
      """INSERT INTO "odd names" VALUES (1, 'x'), (2, 'y')""")
  }

  private def read(options: (String, String)*): DataFrame = {
    spark.read.format("adbc")
      .option("driver", jdbcBridge)
      .option("uri", url)
      .options(options.toMap)
      .load()
  }

  private def scanOf(df: DataFrame): (BatchScanExec, AdbcScan) = {
    val scans = collect(df.queryExecution.executedPlan) { case s: BatchScanExec => s }
    assert(scans.length == 1)
    (scans.head, scans.head.scan.asInstanceOf[AdbcScan])
  }

  private def scannedQuery(df: DataFrame): String = scanOf(df)._2.query

  test("infers the schema from the driver") {
    val df = read("dbtable" -> "people")
    assert(df.schema.map(f => f.name -> f.dataType) == Seq(
      "id" -> IntegerType,
      "name" -> StringType,
      "score" -> DoubleType,
      "active" -> BooleanType,
      "balance" -> DecimalType(10, 2),
      "born" -> DateType,
      "big" -> LongType))
    assert(!df.schema("id").nullable)
    assert(df.schema("name").nullable)
  }

  test("reads all columns as columnar batches") {
    val df = read("dbtable" -> "people")
    checkAnswer(df, Seq(
      Row(1, "alice", 1.5, true, BigDecimal("10.25"), Date.valueOf("2000-01-02"), 10000000000L),
      Row(2, "bob", null, false, null, null, null),
      Row(3, null, 3.5, null, BigDecimal("-3.50"), Date.valueOf("1999-12-31"), -1L)))
    val (exec, _) = scanOf(df)
    assert(exec.supportsColumnar)
    assert(scannedQuery(df) == "SELECT * FROM \"people\"")
    // The JDBC bridge doesn't implement executePartitioned, so the query is run on an executor.
    assert(exec.inputPartitions == Seq(AdbcQueryPartition("SELECT * FROM \"people\"")))
  }

  test("an empty result") {
    val df = read("query" -> "SELECT \"id\" FROM \"people\" WHERE 1 = 0")
    checkAnswer(df, Seq.empty)
  }

  test("reads the partitions returned by executePartitioned") {
    val partitioning = classOf[PartitioningDriverFactory].getName
    val rows = Seq(
      Row(1, "alice", 1.5, true, BigDecimal("10.25"), Date.valueOf("2000-01-02"), 10000000000L),
      Row(2, "bob", null, false, null, null, null),
      Row(3, null, 3.5, null, BigDecimal("-3.50"), Date.valueOf("1999-12-31"), -1L))
    PartitioningConnection.executions.set(0)
    val df = read("driver" -> partitioning, "dbtable" -> "people")
    // Explaining the query doesn't execute it: partitions are planned when the job runs.
    Seq("true", "false").foreach { aqe =>
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe) {
        read("driver" -> partitioning, "dbtable" -> "people").queryExecution.executedPlan
      }
    }
    df.explain()
    assert(PartitioningConnection.executions.get == 0)
    assert(df.collect().length == 3)
    // Planned once, although Spark asks the scan for its partitions more than once.
    assert(PartitioningConnection.executions.get == 1)
    // Another action on the DataFrame reads the same partitions again.
    assert(df.collect().length == 3)
    assert(PartitioningConnection.executions.get == 1)
    checkAnswer(df, rows)
    val (exec, scan) = scanOf(df)
    assert(exec.inputPartitions.map(_.getClass) ==
      Seq(classOf[AdbcDescriptorPartition], classOf[AdbcDescriptorPartition]))
    assert(scan.planInputPartitions() eq scan.planInputPartitions())

    val pruned = read("driver" -> partitioning, "dbtable" -> "people",
      "identifierQuoteChar" -> "\"").select("name")
    checkAnswer(pruned, Seq(Row("alice"), Row("bob"), Row(null)))
    assert(scanOf(pruned)._1.inputPartitions.length == 2)

    PartitioningConnection.executions.set(0)
    Seq("NONE", "none").foreach { mode =>
      val unpartitioned = read(
        "driver" -> partitioning, "dbtable" -> "people", "driverPartitioning" -> mode)
      checkAnswer(unpartitioned, rows)
      assert(scanOf(unpartitioned)._1.inputPartitions ==
        Seq(AdbcQueryPartition("SELECT * FROM \"people\"")))
    }
    assert(PartitioningConnection.executions.get == 0)
    val auto = read(
      "driver" -> partitioning, "dbtable" -> "people", "driverPartitioning" -> "AUTO")
    checkAnswer(auto, rows)
    assert(scanOf(auto)._1.inputPartitions.length == 2)
  }

  test("a user-specified schema is not supported") {
    val e = intercept[SparkUnsupportedOperationException] {
      spark.read.format("adbc").schema(new StructType().add("id", IntegerType))
        .option("driver", jdbcBridge)
        .option("uri", url)
        .option("dbtable", "people")
        .load()
    }
    assert(e.getMessage.contains("does not support user-specified schema"))
  }

  test("the driver must name an ADBC driver factory") {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        read("driver" -> "org.example.NoSuchDriverFactory", "dbtable" -> "people")
      },
      condition = "INVALID_ADBC_OPTIONS.DRIVER_CLASS_NOT_FOUND",
      parameters = Map("driver" -> "'org.example.NoSuchDriverFactory'"))
    Seq(classOf[String], classOf[NoDefaultConstructorDriverFactory]).foreach { cls =>
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          read("driver" -> cls.getName, "dbtable" -> "people")
        },
        condition = "INVALID_ADBC_OPTIONS.INVALID_DRIVER_CLASS",
        parameters = Map(
          "driver" -> s"'${cls.getName}'",
          "factoryClass" -> classOf[AdbcDriverFactory].getName))
    }
  }

  test("query option is sent unchanged") {
    val query = "SELECT \"id\", \"name\" FROM \"people\" WHERE \"id\" > 1"
    val df = read("query" -> s"$query;")
    assert(df.schema.fieldNames.toSeq == Seq("id", "name"))
    checkAnswer(df, Seq(Row(2, "bob"), Row(3, null)))
    assert(scannedQuery(df) == query)
  }

  test("column pruning with identifierQuoteChar") {
    val df = read("dbtable" -> "people", "identifierQuoteChar" -> "\"")
      .select("name", "id")
    checkAnswer(df, Seq(Row("alice", 1), Row("bob", 2), Row(null, 3)))
    assert(scannedQuery(df) == "SELECT \"id\", \"name\" FROM \"people\"")
    assert(scanOf(df)._2.readSchema.fieldNames.toSeq == Seq("id", "name"))
  }

  test("column pruning of a query wraps it as a subquery") {
    val query = "SELECT \"id\", \"name\" FROM \"people\""
    val df = read("query" -> query, "identifierQuoteChar" -> "\"").select("name")
    checkAnswer(df, Seq(Row("alice"), Row("bob"), Row(null)))
    assert(scannedQuery(df) == s"""SELECT "name" FROM ($query) SPARK_GEN_SUBQ""")
  }

  test("column pruning quotes and escapes identifiers") {
    val df = read("dbtable" -> "odd names", "identifierQuoteChar" -> "\"").select("a\"b")
    checkAnswer(df, Seq(Row("x"), Row("y")))
    assert(scannedQuery(df) == "SELECT \"a\"\"b\" FROM \"odd names\"")
  }

  test("column pruning with the quote string reported by the driver") {
    val df = read(
      "driver" -> classOf[QuoteReportingDriverFactory].getName,
      "dbtable" -> "people").select("score")
    checkAnswer(df, Seq(Row(1.5), Row(null), Row(3.5)))
    assert(scannedQuery(df) == "SELECT \"score\" FROM \"people\"")
  }

  test("the quote string reported by the driver is looked up once per table") {
    QuoteReportingConnection.quoteLookups.set(0)
    val df = read(
      "driver" -> classOf[QuoteReportingDriverFactory].getName,
      "dbtable" -> "people")
    // Once to infer the schema.
    assert(QuoteReportingConnection.quoteLookups.get == 1)
    checkAnswer(df, Seq(Row(1, "alice", 1.5, true, BigDecimal("10.25"),
      Date.valueOf("2000-01-02"), 10000000000L), Row(2, "bob", null, false, null, null, null),
      Row(3, null, 3.5, null, BigDecimal("-3.50"), Date.valueOf("1999-12-31"), -1L)))
    val pruned = df.select("score")
    checkAnswer(pruned, Seq(Row(1.5), Row(null), Row(3.5)))
    checkAnswer(pruned, Seq(Row(1.5), Row(null), Row(3.5)))
    assert(QuoteReportingConnection.quoteLookups.get == 2)
  }

  test("identifierQuoteChar overrides the quote string reported by the driver") {
    val query = "SELECT * FROM \"people\""
    val df = read(
      "driver" -> classOf[QuoteReportingDriverFactory].getName,
      "query" -> query,
      "identifierQuoteChar" -> "`").select("score")
    assert(scannedQuery(df) == s"SELECT `score` FROM ($query) SPARK_GEN_SUBQ")
    // H2 doesn't accept backticks, so the wrong quote string fails the query.
    val e = intercept[SparkException](df.collect())
    assert(e.getMessage.contains("`score`"))
    intercept[Exception] {
      read("dbtable" -> "people", "identifierQuoteChar" -> "`")
    }
  }

  test("tables are quoted with double quotes when the quote string is unknown") {
    // The JDBC bridge driver doesn't report SQL_IDENTIFIER_QUOTE_CHAR.
    val bridge = new JdbcDriverFactory().getDriver(ArrowUtils.rootAllocator)
    Using.resource(bridge.open(util.Map.of("uri", url))) { db =>
      Using.resource(db.connect()) { conn =>
        assert(AdbcUtils.getIdentifierQuoteChar(conn).isEmpty)
      }
    }
    val df = read("dbtable" -> "people").select("name")
    checkAnswer(df, Seq(Row("alice"), Row("bob"), Row(null)))
    assert(scannedQuery(df) == "SELECT \"name\" FROM \"people\"")
    // The columns of a query aren't quoted, so they're not pruned.
    val query = "SELECT * FROM \"people\""
    val fromQuery = read("query" -> query).select("name")
    checkAnswer(fromQuery, Seq(Row("alice"), Row("bob"), Row(null)))
    assert(scannedQuery(fromQuery) == query)
    assert(scanOf(fromQuery)._2.readSchema.length == 7)
  }

  test("a table in another schema, with a name that needs quoting") {
    execJdbc(
      """CREATE SCHEMA "other"""",
      """CREATE TABLE "other"."Mixed.Case" ("id" INTEGER)""",
      """INSERT INTO "other"."Mixed.Case" VALUES (7)""")
    try {
      val df = read("dbschema" -> "other", "dbtable" -> "Mixed.Case")
      checkAnswer(df, Row(7))
      assert(scannedQuery(df) == "SELECT * FROM \"other\".\"Mixed.Case\"")
      // The table name is a single identifier.
      intercept[Exception](read("dbtable" -> "other.Mixed.Case"))
    } finally {
      execJdbc("""DROP SCHEMA "other" CASCADE""")
    }
  }

  test("types that Spark can't read are rejected with the schema") {
    val driver = classOf[UnreadableSchemaDriverFactory].getName
    val decimal256 = new ArrowType.Decimal(38, 10, 256)
    UnreadableSchemaDriverFactory.schema = new Schema(util.List.of(
      Field.nullable("d", decimal256)))
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        read("driver" -> driver, "dbtable" -> "people")
      },
      condition = "UNSUPPORTED_ARROWTYPE",
      parameters = Map("typeName" -> decimal256.toString))
    val dictionary = new DictionaryEncoding(0, false, new ArrowType.Int(32, true))
    UnreadableSchemaDriverFactory.schema = new Schema(util.List.of(new Field("s",
      new FieldType(true, ArrowType.Utf8.INSTANCE, dictionary), util.List.of())))
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        read("driver" -> driver, "dbtable" -> "people")
      },
      condition = "UNSUPPORTED_ARROWTYPE",
      parameters = Map("typeName" -> "dictionary-encoded Utf8"))
  }

  test("reading no columns selects a constant") {
    val df = read("dbtable" -> "people")
    assert(df.count() == 3)
    val counted = df.groupBy().count()
    checkAnswer(counted, Row(3))
    assert(scannedQuery(counted) == "SELECT 1 FROM \"people\"")
    assert(scanOf(counted)._2.readSchema.isEmpty)
  }

  test("a result that doesn't match the schema of the scan is rejected") {
    def readWith(readSchema: StructType, query: String): Unit = {
      val options = new AdbcOptions(Map(
        "driver" -> jdbcBridge, "uri" -> url, "dbtable" -> "people"))
      Using.resource(new AdbcPartitionReader(readSchema, options, AdbcQueryPartition(query))) {
        _.next()
      }
    }
    val query = """SELECT "name" FROM "people""""
    readWith(new StructType().add("renamed", StringType), query)
    checkError(
      exception = intercept[SparkRuntimeException] {
        readWith(new StructType().add("name", IntegerType), query)
      },
      condition = "ADBC_SCHEMA_MISMATCH",
      parameters = Map(
        "expected" -> "\"STRUCT<name: INT>\"",
        "actual" -> "\"STRUCT<name: STRING>\""))
    checkError(
      exception = intercept[SparkRuntimeException] {
        readWith(new StructType().add("name", StringType).add("id", IntegerType), query)
      },
      condition = "ADBC_SCHEMA_MISMATCH",
      parameters = Map(
        "expected" -> "\"STRUCT<name: STRING, id: INT>\"",
        "actual" -> "\"STRUCT<name: STRING>\""))
  }

  test("memory leaked by the driver is logged rather than failing the query") {
    val allocator = mock(classOf[BufferAllocator])
    doThrow(new IllegalStateException("Memory was leaked")).when(allocator).close()
    val appender = new LogAppender("ADBC allocator leak")
    withLogAppender(appender, level = Some(Level.WARN)) {
      AdbcUtils.closeAllocator(allocator, jdbcBridge)
    }
    val logs = appender.loggingEvents.map(_.getMessage.getFormattedMessage)
    assert(logs.exists(_.contains(s"not released after using the ADBC driver $jdbcBridge")))
  }

  test("options are validated") {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        read("dbtable" -> "people", "query" -> "SELECT 1")
      },
      condition = "INVALID_ADBC_OPTIONS.BOTH_TABLE_AND_QUERY",
      parameters = Map("tableOption" -> "\"dbtable\"", "queryOption" -> "\"query\""))
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        read()
      },
      condition = "INVALID_ADBC_OPTIONS.MISSING_TABLE_OR_QUERY",
      parameters = Map("tableOption" -> "\"dbtable\"", "queryOption" -> "\"query\""))
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        read("dbtable" -> "people", "identifierQuoteChar" -> "")
      },
      condition = "INVALID_ADBC_OPTIONS.EMPTY_OPTION",
      parameters = Map("option" -> "\"identifierQuoteChar\""))
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        spark.read.format("adbc").option("dbtable", "people").load()
      },
      condition = "INVALID_ADBC_OPTIONS.MISSING_OPTION",
      parameters = Map("option" -> "\"driver\""))
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        read("driver" -> "", "dbtable" -> "people")
      },
      condition = "INVALID_ADBC_OPTIONS.EMPTY_OPTION",
      parameters = Map("option" -> "\"driver\""))
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        new AdbcOptions(Map("driver" -> jdbcBridge, "dbtable" -> "t",
          "driverPartitioning" -> "always"))
      },
      condition = "INVALID_ADBC_OPTIONS.INVALID_OPTION_VALUE",
      parameters = Map(
        "option" -> "\"driverPartitioning\"",
        "value" -> "'always'",
        "validValues" -> "'auto', 'none'"))
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        new AdbcOptions(Map("driver" -> jdbcBridge, "query" -> "SELECT 1", "dbschema" -> "s"))
      },
      condition = "INVALID_ADBC_OPTIONS.SCHEMA_WITH_QUERY",
      parameters = Map("schemaOption" -> "\"dbschema\"", "tableOption" -> "\"dbtable\"",
        "queryOption" -> "\"query\""))
    Seq(
      ("dbschema", Map("driver" -> jdbcBridge, "dbtable" -> "t", "dbschema" -> " ")),
      ("driver", Map("driver" -> "  ", "dbtable" -> "t")),
      ("dbtable", Map("driver" -> jdbcBridge, "dbtable" -> "  ")),
      ("query", Map("driver" -> jdbcBridge, "query" -> " ; ;"))).foreach { case (option, opts) =>
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          new AdbcOptions(opts)
        },
        condition = "INVALID_ADBC_OPTIONS.EMPTY_OPTION",
        parameters = Map("option" -> s""""$option""""))
    }
  }

  test("only the supported database parameters are passed to the driver") {
    val options = new AdbcOptions(Map(
      "driver" -> jdbcBridge, "dbtable" -> "t", "identifierQuoteChar" -> "\"",
      "URI" -> url, "Username" -> "u", "PASSWORD" -> "p", "JNI.Driver" -> "adbc_driver_sqlite",
      "adbc.sql.quirks" -> "x"))
    assert(options.databaseParameters == Map("uri" -> url, "username" -> "u",
      "password" -> "p", "jni.driver" -> "adbc_driver_sqlite"))
  }

  test("null option values are rejected") {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        new AdbcOptions(Map("driver" -> jdbcBridge, "dbtable" -> "t", "uri" -> null))
      },
      condition = "NULL_DATA_SOURCE_OPTION",
      parameters = Map("option" -> "uri"))
  }

  test("the schema is taken from a query result if the driver can't report it") {
    NoExecuteSchemaDriverFactory.queries.clear()
    val df = read(
      "driver" -> classOf[NoExecuteSchemaDriverFactory].getName, "dbtable" -> "people")
    assert(df.schema == read("dbtable" -> "people").schema)
    assert(NoExecuteSchemaDriverFactory.queries.asScala.toSeq ==
      Seq("SELECT * FROM \"people\"", "SELECT * FROM \"people\" WHERE 1 = 0"))
    checkAnswer(df.select("id"), Seq(Row(1), Row(2), Row(3)))
  }

  test("a failure to get the quote string is logged, and all columns of a query are read") {
    val appender = new LogAppender("ADBC getInfo failure")
    val query = "SELECT * FROM \"people\""
    val df = read("driver" -> classOf[FailingInfoDriverFactory].getName, "query" -> query)
      .select("id")
    withLogAppender(appender, level = Some(Level.WARN)) {
      checkAnswer(df, Seq(Row(1), Row(2), Row(3)))
    }
    assert(scannedQuery(df) == query)
    val logs = appender.loggingEvents.map(_.getMessage.getFormattedMessage)
    assert(logs.exists(_.contains("Failed to get the identifier quote string")))
  }

  Seq("none", "auto").foreach { driverPartitioning =>
    test(s"killing a task cancels its query, with driverPartitioning $driverPartitioning") {
      BlockingDriverFactory.reset()
      // With driver partitioning, the read of the partition blocks rather than the query.
      val df = read("driver" -> classOf[BlockingDriverFactory].getName,
        "query" -> "SELECT \"id\" FROM \"people\"", "driverPartitioning" -> driverPartitioning)
      val sc = spark.sparkContext
      val job = new Thread(() => {
        // Don't interrupt the task thread: only cancelling the query unblocks it.
        sc.setJobGroup("adbc-cancel", "cancel an ADBC query", interruptOnCancel = false)
        try df.collect() catch { case _: SparkException => }
      })
      job.start()
      try {
        assert(BlockingDriverFactory.executing.await(1, TimeUnit.MINUTES))
        sc.cancelJobGroup("adbc-cancel")
        assert(BlockingDriverFactory.cancelled.await(1, TimeUnit.MINUTES))
      } finally {
        BlockingDriverFactory.cancelled.countDown()
        job.join(TimeUnit.MINUTES.toMillis(1))
      }
      // Wait for the killed task to close its reader before the memory check.
      eventually(timeout(1.minute)) {
        assert(sc.statusTracker.getActiveStageIds().isEmpty)
        assertArrowMemoryReleased()
      }
    }
  }

  test("a query that fails on the executor releases its resources") {
    execJdbc("""CREATE TABLE "dropped" ("id" INTEGER)""")
    val df = read("dbtable" -> "dropped")
    execJdbc("""DROP TABLE "dropped"""")
    val e = intercept[SparkException](df.collect())
    assert(e.getMessage.contains("dropped"))
  }

  test("username and password are passed to the driver") {
    val noCredentials = url.substring(0, url.indexOf(';'))
    def readWith(credentials: (String, String)*): DataFrame = spark.read.format("adbc")
      .option("driver", jdbcBridge)
      .option("uri", noCredentials)
      .option("dbtable", "people")
      .options(credentials.toMap)
      .load()
    checkAnswer(readWith("username" -> "testUser", "password" -> "testPass").select("id"),
      Seq(Row(1), Row(2), Row(3)))
    val e = intercept[Exception] {
      readWith("username" -> "testUser", "password" -> "wrong")
    }
    assert(e.getMessage.contains("Wrong user name or password"))
  }

  test("the password isn't shown in the plan") {
    val noCredentials = url.substring(0, url.indexOf(';'))
    val df = spark.read.format("adbc")
      .option("driver", jdbcBridge)
      .option("uri", noCredentials)
      .option("dbtable", "people")
      .option("username", "testUser")
      .option("password", "testPass")
      .load()
    val plans = df.queryExecution.toString + df.queryExecution.explainString(ExtendedMode)
    assert(!plans.contains("testPass"))
    // A URL with the password in it is redacted too.
    val withUrl = read("dbtable" -> "people")
    assert(!withUrl.queryExecution.explainString(ExtendedMode).contains("testPass"))
  }

  test("timestamps in milliseconds are not supported") {
    // The JDBC bridge driver returns TIMESTAMP columns in milliseconds.
    execJdbc("""CREATE TABLE "times" ("id" INTEGER, "ts" TIMESTAMP)""")
    try {
      checkError(
        exception = intercept[SparkUnsupportedOperationException] {
          read("dbtable" -> "times")
        },
        condition = "UNSUPPORTED_ARROWTYPE",
        parameters = Map("typeName" -> "Timestamp(MILLISECOND, UTC)"))
      // The table can be read without the column.
      checkAnswer(read("query" -> "SELECT \"id\" FROM \"times\""), Seq.empty)
    } finally {
      execJdbc("""DROP TABLE "times"""")
    }
  }

  test("range partitioning") {
    // "big" is 10000000000, NULL and -1.
    val df = read("dbtable" -> "people", "identifierQuoteChar" -> "\"",
      "partitionColumn" -> "big", "lowerBound" -> "0", "upperBound" -> "10",
      "numPartitions" -> "3")
    checkAnswer(df.select("id"), Seq(Row(1), Row(2), Row(3)))
    val (exec, scan) = scanOf(df.select("id"))
    assert(scan.rangePredicates == Seq(
      "\"big\" < 3 OR \"big\" IS NULL",
      "\"big\" >= 3 AND \"big\" < 6",
      "\"big\" >= 6"))
    assert(exec.inputPartitions == scan.rangePredicates.map { p =>
      AdbcQueryPartition(s"""SELECT "id" FROM "people" WHERE $p""")
    })
    assert(df.select("id").rdd.mapPartitions(it => Iterator(it.size)).collect().toSeq ==
      Seq(2, 0, 1))

    // A query is read as a subquery, and the driver doesn't split the ranges. The column is
    // resolved case-insensitively.
    val query = read("query" -> "SELECT \"id\", \"big\" FROM \"people\"",
      "driver" -> classOf[PartitioningDriverFactory].getName, "identifierQuoteChar" -> "\"",
      "partitionColumn" -> "ID", "lowerBound" -> "1", "upperBound" -> "3",
      "numPartitions" -> "10")
    checkAnswer(query, Seq(Row(1, 10000000000L), Row(2, null), Row(3, -1L)))
    assert(scanOf(query)._1.inputPartitions == Seq(
      AdbcQueryPartition("""SELECT * FROM (SELECT "id", "big" FROM "people") SPARK_GEN_SUBQ """ +
        """WHERE "id" < 2 OR "id" IS NULL"""),
      AdbcQueryPartition("""SELECT * FROM (SELECT "id", "big" FROM "people") SPARK_GEN_SUBQ """ +
        """WHERE "id" >= 2""")))

    // Equal bounds read a single partition, which the driver doesn't split either.
    val single = read("dbtable" -> "people", "partitionColumn" -> "id",
      "driver" -> classOf[PartitioningDriverFactory].getName,
      "lowerBound" -> "5", "upperBound" -> "5", "numPartitions" -> "4")
    assert(scanOf(single)._2.rangePredicates.isEmpty)
    assert(scanOf(single)._1.inputPartitions == Seq(
      AdbcQueryPartition("SELECT * FROM \"people\"")))
    checkAnswer(single.select("id"), Seq(Row(1), Row(2), Row(3)))
  }

  test("range partitioning options are validated") {
    val base = Map("driver" -> jdbcBridge, "uri" -> url, "dbtable" -> "people")
    def options(extra: (String, String)*): Unit = new AdbcOptions(base ++ extra)
    val all = Seq("partitionColumn" -> "id", "lowerBound" -> "0", "upperBound" -> "10",
      "numPartitions" -> "2")
    checkError(
      exception = intercept[SparkIllegalArgumentException](options(all.take(2): _*)),
      condition = "INVALID_ADBC_OPTIONS.INCOMPLETE_PARTITION_OPTIONS",
      parameters = Map("options" ->
        "\"partitionColumn\", \"lowerBound\", \"upperBound\", \"numPartitions\""))
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        options(all :+ ("lowerBound" -> "11"): _*)
      },
      condition = "INVALID_ADBC_OPTIONS.INVALID_PARTITION_BOUNDS",
      parameters = Map("lowerBoundOption" -> "\"lowerBound\"", "lowerBound" -> "11",
        "upperBoundOption" -> "\"upperBound\"", "upperBound" -> "10"))
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        options(all :+ ("upperBound" -> "1e3"): _*)
      },
      condition = "INVALID_ADBC_OPTIONS.NOT_AN_INTEGER",
      parameters = Map("option" -> "\"upperBound\"", "value" -> "'1e3'"))
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        options(all :+ ("numPartitions" -> "0"): _*)
      },
      condition = "INVALID_ADBC_OPTIONS.NOT_A_POSITIVE_INTEGER",
      parameters = Map("option" -> "\"numPartitions\"", "value" -> "'0'"))
    Seq("name", "missing").foreach { column =>
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          read(("dbtable" -> "people") +: all :+ ("partitionColumn" -> column): _*)
        },
        condition = "INVALID_ADBC_OPTIONS.INVALID_PARTITION_COLUMN",
        parameters = Map("column" -> s"`$column`"))
    }
  }

  test("a temporary view in SQL") {
    withTempView("people_view") {
      sql(
        s"""CREATE TEMPORARY VIEW people_view
           |USING adbc
           |OPTIONS (driver '$jdbcBridge', uri '$url', dbtable 'people')""".stripMargin)
      checkAnswer(sql("SELECT name FROM people_view WHERE id > 1"), Seq(Row("bob"), Row(null)))
    }
  }
}
