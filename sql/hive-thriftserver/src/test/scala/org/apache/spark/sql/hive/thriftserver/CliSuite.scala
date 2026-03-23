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

package org.apache.spark.sql.hive.thriftserver

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.CountDownLatch

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Promise
import scala.concurrent.duration._

import org.apache.hadoop.hive.cli.CliSessionState
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.{ErrorMessageFormat, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.ProcessTestUtils.ProcessOutputCapturer
import org.apache.spark.deploy.{RedirectConsolePlugin, SparkHadoopUtil}
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.hive.HiveUtils._
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.hive.test.HiveTestJars
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * A test suite for the `spark-sql` CLI tool.
 */
class CliSuite extends SparkFunSuite {
  val warehousePath = Utils.createTempDir()
  val metastorePath = Utils.createTempDir()
  val scratchDirPath = Utils.createTempDir()
  val sparkWareHouseDir = Utils.createTempDir()

  override def beforeAll(): Unit = {
    super.beforeAll()
    warehousePath.delete()
    metastorePath.delete()
    scratchDirPath.delete()
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(warehousePath)
      Utils.deleteRecursively(metastorePath)
      Utils.deleteRecursively(scratchDirPath)
    } finally {
      super.afterAll()
    }
  }

  /**
   * Run a CLI operation and expect all the queries and expected answers to be returned.
   *
   * @param timeout maximum time for the commands to complete
   * @param extraArgs any extra arguments
   * @param errorResponses a sequence of strings whose presence in the stdout of the forked process
   *                       is taken as an immediate error condition. That is: if a line containing
   *                       with one of these strings is found, fail the test immediately.
   *                       The default value is `Seq("Error:")`
   * @param maybeWarehouse an option for warehouse path, which will be set via
   *                       `hive.metastore.warehouse.dir`.
   * @param useExternalHiveFile whether to load the hive-site.xml from `src/test/noclasspath` or
   *                            not, disabled by default
   * @param metastore which path the embedded derby database for metastore locates. Use the
   *                  global `metastorePath` by default
   * @param queriesAndExpectedAnswers one or more tuples of query + answer
   */
  def runCliWithin(
      timeout: FiniteDuration,
      extraArgs: Seq[String] = Seq.empty,
      errorResponses: Seq[String] = Seq("Error:"),
      maybeWarehouse: Option[File] = Some(warehousePath),
      useExternalHiveFile: Boolean = false,
      metastore: File = metastorePath,
      prompt: String = "spark-sql>")(
      queriesAndExpectedAnswers: (String, String)*): Unit = {

    // Explicitly adds ENTER for each statement to make sure they are actually entered into the CLI.
    val queriesString = queriesAndExpectedAnswers.map(_._1 + "\n").mkString
    // spark-sql echoes the queries on STDOUT, expect first an echo of the query, then the answer.
    val expectedAnswers = queriesAndExpectedAnswers.flatMap {
      case (query, answer) =>
        if (query == "") {
          // empty query means a command launched with -e
          Seq(answer)
        } else {
          // spark-sql echoes the submitted queries
          val xs = query.split("\n").toList
          val queryEcho = s"$prompt ${xs.head}" :: xs.tail.map(l => s"         > $l")
          // longer lines sometimes get split in the output,
          // match the first 60 characters of each query line
          queryEcho.map(_.take(60)) :+ answer
        }
    }

    val extraHive = if (useExternalHiveFile) {
      s"--driver-class-path ${System.getProperty("user.dir")}/src/test/noclasspath"
    } else {
      ""
    }
    val warehouseConf =
      maybeWarehouse.map(dir => s"--hiveconf hive.metastore.warehouse.dir=$dir").getOrElse("")
    val command = {
      val cliScript = "../../bin/spark-sql".split("/").mkString(File.separator)
      val jdbcUrl = s"jdbc:derby:;databaseName=$metastore;create=true"
      s"""$cliScript
         |  --master local
         |  --driver-java-options -Dderby.system.durability=test
         |  $extraHive
         |  --conf spark.ui.enabled=false
         |  --conf ${SQLConf.LEGACY_EMPTY_CURRENT_DB_IN_CLI.key}=true
         |  --hiveconf javax.jdo.option.ConnectionURL=$jdbcUrl
         |  --hiveconf hive.exec.scratchdir=$scratchDirPath
         |  --hiveconf conf1=conftest
         |  --hiveconf conf2=1
         |  $warehouseConf
       """.stripMargin.split("\\s+").toSeq ++ extraArgs
    }

    var next = 0
    val foundMasterAndApplicationIdMessage = Promise.apply[Unit]()
    val foundAllExpectedAnswers = Promise.apply[Unit]()
    val buffer = new ArrayBuffer[String]()
    val lock = new Object

    def captureOutput(source: String)(line: String): Unit = lock.synchronized {
      logInfo(s"$source> $line")
      buffer += line

      if (line.startsWith("Spark master: ") && line.contains("Application Id: ")) {
        foundMasterAndApplicationIdMessage.trySuccess(())
      }

      // If we haven't found all expected answers and another expected answer comes up...
      if (next < expectedAnswers.size && line.contains(expectedAnswers(next))) {
        log.info(s"$source> found expected output line $next: '${expectedAnswers(next)}'")
        next += 1
        // If all expected answers have been found...
        if (next == expectedAnswers.size) {
          foundAllExpectedAnswers.trySuccess(())
        }
      } else {
        errorResponses.foreach { r =>
          if (line.contains(r) && !line.contains("IntentionallyFaultyConnectionProvider")) {
            foundAllExpectedAnswers.tryFailure(
              new RuntimeException(s"Failed with error line '$line'"))
          }
        }
      }
    }

    val process = new ProcessBuilder(command: _*).start()

    val stdinWriter = new OutputStreamWriter(process.getOutputStream, StandardCharsets.UTF_8)
    stdinWriter.write(queriesString)
    stdinWriter.flush()
    stdinWriter.close()

    new ProcessOutputCapturer(process.getInputStream, captureOutput("stdout")).start()
    new ProcessOutputCapturer(process.getErrorStream, captureOutput("stderr")).start()

    try {
      val timeoutForQuery = if (!extraArgs.contains("-e")) {
        // Wait for for cli driver to boot, up to two minutes
        ThreadUtils.awaitResult(foundMasterAndApplicationIdMessage.future, 2.minutes)
        log.info("Cli driver is booted. Waiting for expected answers.")
        // Given timeout is applied after the cli driver is ready
        timeout
      } else {
        // There's no boot message if -e option is provided, just extend timeout long enough
        // so that the bootup duration is counted on the timeout
        2.minutes + timeout
      }
      ThreadUtils.awaitResult(foundAllExpectedAnswers.future, timeoutForQuery)
      log.info("Found all expected output.")
    } catch { case cause: Throwable =>
      val message = lock.synchronized {
        s"""
           |=======================
           |CliSuite failure output
           |=======================
           |Spark SQL CLI command line: ${command.mkString(" ")}
           |Exception: $cause
           |Failed to capture next expected output "${expectedAnswers(next)}" within $timeout.
           |
           |${buffer.mkString("\n")}
           |===========================
           |End CliSuite failure output
           |===========================
         """.stripMargin
      }
      logError(message, cause)
      fail(message, cause)
    } finally {
      if (!process.waitFor(1, MINUTES)) {
        try {
          log.warn("spark-sql did not exit gracefully.")
        } finally {
          process.destroy()
        }
      }
    }
  }

  test("load warehouse dir from hive-site.xml") {
    val metastore = Utils.createTempDir()
    metastore.delete()
    try {
      runCliWithin(1.minute,
        maybeWarehouse = None,
        useExternalHiveFile = true,
        metastore = metastore)(
        "desc database default;" -> "hive_one",
        "set spark.sql.warehouse.dir;" -> "hive_one")
    } finally {
      Utils.deleteRecursively(metastore)
    }
  }

  test("load warehouse dir from --hiveconf") {
    // --hiveconf will overrides hive-site.xml
    runCliWithin(2.minute, useExternalHiveFile = true)(
      "desc database default;" -> warehousePath.getAbsolutePath,
      "create database cliTestDb;" -> "",
      "desc database cliTestDb;" -> warehousePath.getAbsolutePath,
      "set spark.sql.warehouse.dir;" -> warehousePath.getAbsolutePath)
  }

  test("load warehouse dir from --conf spark(.hadoop).hive.*") {
    // override conf from hive-site.xml
    val metastore = Utils.createTempDir()
    metastore.delete()
    try {
      runCliWithin(2.minute,
        extraArgs =
          Seq("--conf", s"spark.hadoop.hive.metastore.warehouse.dir=$sparkWareHouseDir"),
        maybeWarehouse = None,
        useExternalHiveFile = true,
        metastore = metastore)(
        "desc database default;" -> sparkWareHouseDir.getAbsolutePath,
        "create database cliTestDb;" -> "",
        "desc database cliTestDb;" -> sparkWareHouseDir.getAbsolutePath,
        "set spark.sql.warehouse.dir;" -> sparkWareHouseDir.getAbsolutePath)

      // override conf from --hiveconf too
      runCliWithin(2.minute,
        extraArgs = Seq("--conf", s"spark.hive.metastore.warehouse.dir=$sparkWareHouseDir"),
        metastore = metastore)(
        "desc database default;" -> sparkWareHouseDir.getAbsolutePath,
        "create database cliTestDb;" -> "",
        "desc database cliTestDb;" -> sparkWareHouseDir.getAbsolutePath,
        "set spark.sql.warehouse.dir;" -> sparkWareHouseDir.getAbsolutePath)
    } finally {
      Utils.deleteRecursively(metastore)
    }
  }

  test("load warehouse dir from spark.sql.warehouse.dir") {
    // spark.sql.warehouse.dir overrides all hive ones
    val metastore = Utils.createTempDir()
    metastore.delete()
    try {
      runCliWithin(2.minute,
        extraArgs = Seq(
            "--conf", s"${StaticSQLConf.WAREHOUSE_PATH.key}=${sparkWareHouseDir}1",
            "--conf", s"spark.hadoop.hive.metastore.warehouse.dir=${sparkWareHouseDir}2"),
        metastore = metastore)(
        "desc database default;" -> sparkWareHouseDir.getAbsolutePath.concat("1"))
    } finally {
      Utils.deleteRecursively(metastore)
    }
  }

  test("Simple commands") {
    val dataFilePath =
      Thread.currentThread().getContextClassLoader.getResource("data/files/small_kv.txt")

    runCliWithin(3.minute)(
      "CREATE TABLE hive_test(key INT, val STRING) USING hive;"
        -> "",
      "SHOW TABLES;"
        -> "hive_test",
      s"""LOAD DATA LOCAL INPATH '$dataFilePath'
         |OVERWRITE INTO TABLE hive_test;""".stripMargin
        -> "",
      "CACHE TABLE hive_test;"
        -> "",
      "SELECT COUNT(*) FROM hive_test;"
        -> "5",
      "DROP TABLE hive_test;"
        -> ""
    )
  }

  test("Single command with -e") {
    runCliWithin(2.minute, Seq("-e", "SHOW DATABASES;"))("" -> "")
  }

  test("Single command with --database") {
    runCliWithin(2.minute)(
      "CREATE DATABASE hive_db_test;"
        -> "",
      "USE hive_db_test;"
        -> "",
      "CREATE TABLE hive_table_test(key INT, val STRING);"
        -> "",
      "SHOW TABLES;"
        -> "hive_table_test"
    )

    runCliWithin(2.minute, Seq("--database", "hive_db_test", "-e", "SHOW TABLES;"))(
      "" -> "hive_table_test"
    )
  }

  test("Commands using SerDe provided in --jars") {
    val jarFile = HiveTestJars.getHiveHcatalogCoreJar().getCanonicalPath

    val dataFilePath =
      Thread.currentThread().getContextClassLoader.getResource("data/files/small_kv.txt")

    runCliWithin(3.minute, Seq("--jars", s"$jarFile"))(
      """CREATE TABLE t1(key string, val string)
        |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe';""".stripMargin
        -> "",
      "CREATE TABLE sourceTable (key INT, val STRING) USING hive;"
        -> "",
      s"""LOAD DATA LOCAL INPATH '$dataFilePath'
         |OVERWRITE INTO TABLE sourceTable;""".stripMargin
        -> "",
      "INSERT INTO TABLE t1 SELECT key, val FROM sourceTable;"
        -> "",
      "SELECT collect_list(array(val)) FROM t1;"
        -> """[["val_238"],["val_86"],["val_311"],["val_27"],["val_165"]]""",
      "DROP TABLE t1;"
        -> "",
      "DROP TABLE sourceTable;"
        -> ""
    )
  }

  test("SPARK-29022: Commands using SerDe provided in --hive.aux.jars.path") {
    val dataFilePath =
      Thread.currentThread().getContextClassLoader.getResource("data/files/small_kv.txt")
    val hiveContribJar = HiveTestJars.getHiveHcatalogCoreJar().getCanonicalPath
    runCliWithin(
      3.minute,
      Seq("--conf", s"spark.hadoop.hive.aux.jars.path=$hiveContribJar"))(
      """CREATE TABLE addJarWithHiveAux(key string, val string)
        |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe';""".stripMargin
        -> "",
      "CREATE TABLE sourceTableForWithHiveAux (key INT, val STRING) USING hive;"
        -> "",
      s"""LOAD DATA LOCAL INPATH '$dataFilePath'
         |OVERWRITE INTO TABLE sourceTableForWithHiveAux;""".stripMargin
        -> "",
      "INSERT INTO TABLE addJarWithHiveAux SELECT key, val FROM sourceTableForWithHiveAux;"
        -> "",
      "SELECT collect_list(array(val)) FROM addJarWithHiveAux;"
        -> """[["val_238"],["val_86"],["val_311"],["val_27"],["val_165"]]""",
      "DROP TABLE addJarWithHiveAux;"
        -> "",
      "DROP TABLE sourceTableForWithHiveAux;"
        -> ""
    )
  }

  testRetry("SPARK-11188 Analysis error reporting") {
    runCliWithin(timeout = 2.minute,
      errorResponses = Seq("AnalysisException"))(
      "select * from nonexistent_table;" -> "nonexistent_table"
    )
  }

  test("SPARK-11624 Spark SQL CLI should set sessionState only once") {
    runCliWithin(2.minute, Seq("-e", "!echo \"This is a test for Spark-11624\";"))(
      "" -> "This is a test for Spark-11624")
  }

  test("list jars") {
    val jarFile = Thread.currentThread().getContextClassLoader.getResource("TestUDTF.jar")
    assume(jarFile != null)
    runCliWithin(2.minute)(
      s"ADD JAR $jarFile;" -> "",
      s"LIST JARS;" -> "TestUDTF.jar"
    )
  }

  test("list jar <jarfile>") {
    val jarFile = Thread.currentThread().getContextClassLoader.getResource("TestUDTF.jar")
    assume(jarFile != null)
    runCliWithin(2.minute)(
      s"ADD JAR $jarFile;" -> "",
      s"List JAR $jarFile;" -> "TestUDTF.jar"
    )
  }

  test("list files") {
    val dataFilePath = Thread.currentThread().
      getContextClassLoader.getResource("data/files/small_kv.txt")
    runCliWithin(2.minute)(
      s"ADD FILE $dataFilePath;" -> "",
      s"LIST FILES;" -> "small_kv.txt"
    )
  }

  test("list file <filepath>") {
    val dataFilePath = Thread.currentThread().
      getContextClassLoader.getResource("data/files/small_kv.txt")
    runCliWithin(2.minute)(
      s"ADD FILE $dataFilePath;" -> "",
      s"LIST FILE $dataFilePath;" -> "small_kv.txt"
    )
  }

  test("apply hiveconf from cli command") {
    runCliWithin(2.minute)(
      "SET conf1;" -> "conftest",
      "SET conf2;" -> "1",
      "SET conf3=${hiveconf:conf1};" -> "conftest",
      "SET conf3;" -> "conftest"
    )
  }

  test("Support hive.aux.jars.path") {
    val hiveContribJar = HiveTestJars.getHiveContribJar().getCanonicalPath
    runCliWithin(
      1.minute,
      Seq("--conf", s"spark.hadoop.hive.aux.jars.path=$hiveContribJar"))(
      "CREATE TEMPORARY FUNCTION example_format AS " +
        "'org.apache.hadoop.hive.contrib.udf.example.UDFExampleFormat';" -> "",
      "SELECT example_format('%o', 93);" -> "135"
    )
  }

  test("SPARK-28840 test --jars command") {
    val jarFile = new File("../../sql/hive/src/test/resources/SPARK-21101-1.0.jar")
    assume(jarFile.exists)
    runCliWithin(
      1.minute,
      Seq("--jars", s"${jarFile.getCanonicalPath}"))(
      "CREATE TEMPORARY FUNCTION testjar AS" +
        " 'org.apache.spark.sql.hive.execution.UDTFStack';" -> "",
      "SELECT testjar(1,'TEST-SPARK-TEST-jar', 28840);" -> "TEST-SPARK-TEST-jar\t28840"
    )
  }

  test("SPARK-28840 test --jars and hive.aux.jars.path command") {
    val jarFile = new File("../../sql/hive/src/test/resources/SPARK-21101-1.0.jar")
    assume(jarFile.exists)
    val hiveContribJar = HiveTestJars.getHiveContribJar().getCanonicalPath
    runCliWithin(
      2.minutes,
      Seq("--jars", s"${jarFile.getCanonicalPath}", "--conf",
        s"spark.hadoop.hive.aux.jars.path=$hiveContribJar"))(
      "CREATE TEMPORARY FUNCTION testjar AS" +
        " 'org.apache.spark.sql.hive.execution.UDTFStack';" -> "",
      "SELECT testjar(1,'TEST-SPARK-TEST-jar', 28840);" -> "TEST-SPARK-TEST-jar\t28840",
      "CREATE TEMPORARY FUNCTION example_max AS " +
        "'org.apache.hadoop.hive.contrib.udaf.example.UDAFExampleMax';" -> "",
      "SELECT concat_ws(',', 'First', example_max(1234321), 'Third');" -> "First,1234321,Third"
    )
  }

  test("SPARK-29022 Commands using SerDe provided in ADD JAR sql") {
    val dataFilePath =
      Thread.currentThread().getContextClassLoader.getResource("data/files/small_kv.txt")
    val hiveContribJar = HiveTestJars.getHiveHcatalogCoreJar().getCanonicalPath
    runCliWithin(
      3.minute)(
      s"ADD JAR ${hiveContribJar};" -> "",
      """CREATE TABLE addJarWithSQL(key string, val string)
        |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe';""".stripMargin
        -> "",
      "CREATE TABLE sourceTableForWithSQL(key INT, val STRING) USING hive;"
        -> "",
      s"""LOAD DATA LOCAL INPATH '$dataFilePath'
         |OVERWRITE INTO TABLE sourceTableForWithSQL;""".stripMargin
        -> "",
      "INSERT INTO TABLE addJarWithSQL SELECT key, val FROM sourceTableForWithSQL;"
        -> "",
      "SELECT collect_list(array(val)) FROM addJarWithSQL;"
        -> """[["val_238"],["val_86"],["val_311"],["val_27"],["val_165"]]""",
      "DROP TABLE addJarWithSQL;"
        -> "",
      "DROP TABLE sourceTableForWithSQL;"
        -> ""
    )
  }

  test("SPARK-26321 Should not split semicolon within quoted string literals") {
    runCliWithin(3.minute)(
      """select 'Test1', "^;^";""" -> "Test1\t^;^",
      """select 'Test2', "\";";""" -> "Test2\t\";",
      """select 'Test3', "\';";""" -> "Test3\t';",
      "select concat('Test4', ';');" -> "Test4;"
    )
  }

  test("Pad Decimal numbers with trailing zeros to the scale of the column") {
    runCliWithin(1.minute)(
      "SELECT CAST(1 AS DECIMAL(38, 18));"
        -> "1.000000000000000000"
    )
  }

  test("SPARK-30049 Should not complain for quotes in commented lines") {
    runCliWithin(1.minute)(
      """SELECT concat('test', 'comment') -- someone's comment here
        |;""".stripMargin -> "testcomment"
    )
  }

  test("SPARK-31102 spark-sql fails to parse when contains comment") {
    runCliWithin(1.minute)(
      """SELECT concat('test', 'comment'),
        |    -- someone's comment here
        | 2;""".stripMargin -> "testcomment"
    )
  }

  test("SPARK-30049 Should not complain for quotes in commented with multi-lines") {
    runCliWithin(1.minute)(
      """SELECT concat('test', 'comment') -- someone's comment here \
        | comment continues here with single ' quote \
        | extra ' \
        |;""".stripMargin -> "testcomment"
    )
  }

  test("SPARK-31595 Should allow unescaped quote mark in quoted string") {
    runCliWithin(1.minute)(
      "SELECT '\"legal string a';select 1 + 234;".stripMargin -> "235"
    )
    runCliWithin(1.minute)(
      "SELECT \"legal 'string b\";select 22222 + 1;".stripMargin -> "22223"
    )
  }

  testRetry("DEBUG format prints stack traces for all errors") {
    // In DEBUG format with silent=false, stack traces are always printed
    runCliWithin(
      1.minute,
      extraArgs = Seq(
        "--hiveconf", "hive.session.silent=false",
        "--conf", s"${SQLConf.ERROR_MESSAGE_FORMAT.key}=${ErrorMessageFormat.DEBUG}",
        "-e", "select from_json('a', 'a INT', map('mode', 'FAILFAST'));"),
      errorResponses = Seq("JsonParseException"))(
      ("", "[MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION]"),
      ("", "JsonParseException: Unrecognized token 'a'"))
    // In DEBUG format with silent=true, stack traces are suppressed
    runCliWithin(
      1.minute,
      extraArgs = Seq(
        "--conf", "spark.hive.session.silent=true",
        "--conf", s"${SQLConf.ERROR_MESSAGE_FORMAT.key}=${ErrorMessageFormat.DEBUG}",
        "-e", "select from_json('a', 'a INT', map('mode', 'FAILFAST'));"),
      errorResponses = Seq("MALFORMED_RECORD_IN_PARSING"))(
      ("", "[MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION]"))
  }

  test("SPARK-30808: use Java 8 time API in Thrift SQL CLI by default") {
    // If Java 8 time API is enabled via the SQL config `spark.sql.datetime.java8API.enabled`,
    // the date formatter for `java.sql.LocalDate` must output negative years with sign.
    runCliWithin(1.minute)("SELECT MAKE_DATE(-44, 3, 15);" -> "-0044-03-15")
  }

  testRetry("SPARK-33100: Ignore a semicolon inside a bracketed comment in spark-sql") {
    runCliWithin(1.minute)(
      "/* SELECT 'test';*/ SELECT 'test';" -> "test",
      ";;/* SELECT 'test';*/ SELECT 'test';" -> "test",
      "/* SELECT 'test';*/;; SELECT 'test';" -> "test",
      "SELECT 'test'; -- SELECT 'test';" -> "test",
      "SELECT 'test'; /* SELECT 'test';*/;" -> "test",
      "/*$meta chars{^\\;}*/ SELECT 'test';" -> "test",
      "/*\nmulti-line\n*/ SELECT 'test';" -> "test",
      "/*/* multi-level bracketed*/ SELECT 'test';" -> "test"
    )
  }

  test("SPARK-33100: test sql statements with hint in bracketed comment") {
    runCliWithin(2.minute)(
      "CREATE TEMPORARY VIEW t1 AS SELECT * FROM VALUES(1, 2) AS t1(k, v);" -> "",
      "CREATE TEMPORARY VIEW t2 AS SELECT * FROM VALUES(2, 1) AS t2(k, v);" -> "",
      "EXPLAIN SELECT /*+ MERGEJOIN(t1) */ t1.* FROM t1 JOIN t2 ON t1.k = t2.v;" -> "SortMergeJoin",
      "EXPLAIN SELECT /* + MERGEJOIN(t1) */ t1.* FROM t1 JOIN t2 ON t1.k = t2.v;"
        -> "BroadcastHashJoin"
    )
  }

  test("SPARK-35086: --verbose should be passed to Spark SQL CLI") {
    runCliWithin(2.minute, Seq("--verbose"))(
      "SELECT 'SPARK-35086' AS c1, '--verbose' AS c2;" ->
        "SELECT 'SPARK-35086' AS c1, '--verbose' AS c2"
    )
  }

  test("SPARK-35102: Make spark.sql.hive.version meaningful and not deprecated") {
    runCliWithin(1.minute,
      Seq("--conf", "spark.sql.hive.version=0.1"),
      Seq(s"please use ${HIVE_METASTORE_VERSION.key}"))("" -> "")
    runCliWithin(2.minute,
      Seq("--conf", s"${BUILTIN_HIVE_VERSION.key}=$builtinHiveVersion"))(
      s"set ${BUILTIN_HIVE_VERSION.key};" -> builtinHiveVersion, "SET -v;" -> builtinHiveVersion)
  }

  test("SPARK-37471: spark-sql support nested bracketed comment ") {
    runCliWithin(1.minute)(
      """
        |/* SELECT /*+ HINT() */ 4; */
        |SELECT 1;
        |""".stripMargin -> "SELECT 1"
    )
  }

  testRetry("SPARK-37555: spark-sql should pass last unclosed comment to backend") {
    runCliWithin(1.minute)(
      // Only unclosed comment.
      "/* SELECT /*+ HINT() 4; */;".stripMargin -> "Syntax error at or near ';'",
      // Unclosed nested bracketed comment.
      "/* SELECT /*+ HINT() 4; */ SELECT 1;".stripMargin -> "1",
      // Unclosed comment with query.
      "/* Here is a unclosed bracketed comment SELECT 1;"->
        "Found an unclosed bracketed comment. Please, append */ at the end of the comment.",
      // Whole comment.
      "/* SELECT /*+ HINT() */ 4; */;".stripMargin -> ""
    )
  }

  testRetry("SPARK-37694: delete [jar|file|archive] shall use spark sql processor") {
    runCliWithin(2.minute, errorResponses = Seq("ParseException"))(
      "delete jar dummy.jar;" ->
        "Syntax error at or near 'jar': missing 'FROM'. SQLSTATE: 42601 (line 1, pos 7)")
  }

  test("SPARK-37906: Spark SQL CLI should not pass final comment") {
    val sparkConf = new SparkConf(loadDefaults = true)
      .setMaster("local-cluster[1,1,1024]")
      .setAppName("SPARK-37906")
    val sparkContext = new SparkContext(sparkConf)
    SparkSQLEnv.sparkContext = sparkContext
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
    val cliConf = HiveClientImpl.newHiveConf(sparkConf, hadoopConf)
    val sessionState = new CliSessionState(cliConf)
    SessionState.setCurrentSessionState(sessionState)
    val cli = new SparkSQLCLIDriver
    Seq("SELECT 1; --comment" -> Seq("SELECT 1"),
      "SELECT 1; /* comment */" -> Seq("SELECT 1"),
      "SELECT 1; /* comment" -> Seq("SELECT 1", " /* comment"),
      "SELECT 1; /* comment select 1;" -> Seq("SELECT 1", " /* comment select 1;"),
      "/* This is a comment without end symbol SELECT 1;" ->
        Seq("/* This is a comment without end symbol SELECT 1;"),
      "SELECT 1; --comment\n" -> Seq("SELECT 1"),
      "SELECT 1; /* comment */\n" -> Seq("SELECT 1"),
      "SELECT 1; /* comment\n" -> Seq("SELECT 1", " /* comment\n"),
      "SELECT 1; /* comment select 1;\n" -> Seq("SELECT 1", " /* comment select 1;\n"),
      "/* This is a comment without end symbol SELECT 1;\n" ->
        Seq("/* This is a comment without end symbol SELECT 1;\n"),
      "/* comment */ SELECT 1;" -> Seq("/* comment */ SELECT 1"),
      "SELECT /* comment */  1;" -> Seq("SELECT /* comment */  1"),
      "-- comment " -> Seq(),
      "-- comment \nSELECT 1" -> Seq("-- comment \nSELECT 1"),
      "/*  comment */  " -> Seq()
    ).foreach { case (query, ret) =>
      assert(cli.splitSemiColon(query) === ret)
    }
    sessionState.close()
    SparkSQLEnv.stop()
  }

  test("SQL Scripting: splitSemiColon should not split inside compound blocks") {
    val sparkConf = new SparkConf(loadDefaults = true)
      .setMaster("local-cluster[1,1,1024]")
      .setAppName("sql-scripting-split")
    val sparkContext = new SparkContext(sparkConf)
    SparkSQLEnv.sparkContext = sparkContext
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
    val cliConf = HiveClientImpl.newHiveConf(sparkConf, hadoopConf)
    val sessionState = new CliSessionState(cliConf)
    SessionState.setCurrentSessionState(sessionState)
    val cli = new SparkSQLCLIDriver

    // Simple BEGIN...END
    assert(cli.splitSemiColon(
      """BEGIN
        |  SELECT 1;
        |END""".stripMargin) ===
      Seq(
        """BEGIN
          |  SELECT 1;
          |END""".stripMargin))

    // BEGIN...END with trailing semicolon
    assert(cli.splitSemiColon(
      """BEGIN
        |  SELECT 1;
        |END;""".stripMargin) ===
      Seq(
        """BEGIN
          |  SELECT 1;
          |END""".stripMargin))

    // Multiple statements inside the block
    assert(cli.splitSemiColon(
      """BEGIN
        |  SELECT 1;
        |  SELECT 2;
        |END;""".stripMargin) ===
      Seq(
        """BEGIN
          |  SELECT 1;
          |  SELECT 2;
          |END""".stripMargin))

    // Regular statements before and after a scripting block should still be split
    assert(cli.splitSemiColon(
      """SELECT 0;
        |BEGIN
        |  SELECT 1;
        |  SELECT 2;
        |END;
        |SELECT 3;""".stripMargin) ===
      Seq(
        "SELECT 0",
        """
          |BEGIN
          |  SELECT 1;
          |  SELECT 2;
          |END""".stripMargin,
        """
          |SELECT 3""".stripMargin))

    // IF...END IF inside a block
    assert(cli.splitSemiColon(
      """BEGIN
        |  IF x = 1 THEN
        |    SELECT 1;
        |  END IF;
        |END;""".stripMargin) ===
      Seq(
        """BEGIN
          |  IF x = 1 THEN
          |    SELECT 1;
          |  END IF;
          |END""".stripMargin))

    // WHILE...DO...END WHILE inside a block
    assert(cli.splitSemiColon(
      """BEGIN
        |  WHILE x > 0 DO
        |    SET x = x - 1;
        |  END WHILE;
        |END;""".stripMargin) ===
      Seq(
        """BEGIN
          |  WHILE x > 0 DO
          |    SET x = x - 1;
          |  END WHILE;
          |END""".stripMargin))

    // FOR...DO...END FOR inside a block
    assert(cli.splitSemiColon(
      """BEGIN
        |  FOR r AS SELECT * FROM t DO
        |    SELECT r.id;
        |  END FOR;
        |END;""".stripMargin) ===
      Seq(
        """BEGIN
          |  FOR r AS SELECT * FROM t DO
          |    SELECT r.id;
          |  END FOR;
          |END""".stripMargin))

    // LOOP...END LOOP inside a block
    assert(cli.splitSemiColon(
      """BEGIN
        |  LOOP
        |    SELECT 1;
        |  END LOOP;
        |END;""".stripMargin) ===
      Seq(
        """BEGIN
          |  LOOP
          |    SELECT 1;
          |  END LOOP;
          |END""".stripMargin))

    // REPEAT...END REPEAT inside a block
    assert(cli.splitSemiColon(
      """BEGIN
        |  REPEAT
        |    SELECT 1;
        |  UNTIL x > 0 END REPEAT;
        |END;""".stripMargin) ===
      Seq(
        """BEGIN
          |  REPEAT
          |    SELECT 1;
          |  UNTIL x > 0 END REPEAT;
          |END""".stripMargin))

    // CASE statement inside a block
    assert(cli.splitSemiColon(
      """BEGIN
        |  CASE x
        |    WHEN 1 THEN SELECT 'one';
        |  END CASE;
        |END;""".stripMargin) ===
      Seq(
        """BEGIN
          |  CASE x
          |    WHEN 1 THEN SELECT 'one';
          |  END CASE;
          |END""".stripMargin))

    // CASE expression (not a scripting block) inside a scripting block
    assert(cli.splitSemiColon(
      """BEGIN
        |  SELECT CASE WHEN x=1 THEN 'a' ELSE 'b' END;
        |END;""".stripMargin) ===
      Seq(
        """BEGIN
          |  SELECT CASE WHEN x=1 THEN 'a' ELSE 'b' END;
          |END""".stripMargin))

    // Nested BEGIN...END
    assert(cli.splitSemiColon(
      """BEGIN
        |  BEGIN
        |    SELECT 1;
        |  END;
        |  SELECT 2;
        |END;""".stripMargin) ===
      Seq(
        """BEGIN
          |  BEGIN
          |    SELECT 1;
          |  END;
          |  SELECT 2;
          |END""".stripMargin))

    // `IF(` is the Spark SQL built-in function, not a scripting IF -- the block must
    // not be kept unsplit merely because IF appears inside it.
    assert(cli.splitSemiColon(
      """BEGIN
        |  SELECT IF(x > 0, 'pos', 'non-pos');
        |END;""".stripMargin) ===
      Seq(
        """BEGIN
          |  SELECT IF(x > 0, 'pos', 'non-pos');
          |END""".stripMargin))

    // Labeled block: label: BEGIN ... END label
    assert(cli.splitSemiColon(
      """outer: BEGIN
        |  SELECT 1;
        |END outer;""".stripMargin) ===
      Seq(
        """outer: BEGIN
          |  SELECT 1;
          |END outer""".stripMargin))

    sessionState.close()
    SparkSQLEnv.stop()
  }

  test("SQL Scripting: sqlScriptingBlockDepth correctly tracks nesting") {
    import SparkSQLCLIDriver.sqlScriptingBlockDepth

    // Not a script
    assert(sqlScriptingBlockDepth("SELECT 1") === 0)
    // Simple open block
    assert(sqlScriptingBlockDepth(
      """BEGIN
        |  SELECT 1;""".stripMargin) === 1)
    // Closed block
    assert(sqlScriptingBlockDepth(
      """BEGIN
        |  SELECT 1;
        |END""".stripMargin) === 0)
    // Trailing semicolon after END still closes
    assert(sqlScriptingBlockDepth("BEGIN SELECT 1; END;") === 0)
    // Nested block open
    assert(sqlScriptingBlockDepth(
      """BEGIN
        |  BEGIN
        |    SELECT 1;""".stripMargin) === 2)
    // IF still open
    assert(sqlScriptingBlockDepth(
      """BEGIN
        |  IF x=1 THEN
        |    SELECT 1;""".stripMargin) === 2)
    // IF closed
    assert(sqlScriptingBlockDepth(
      """BEGIN
        |  IF x=1 THEN
        |    SELECT 1;
        |  END IF;""".stripMargin) === 1)
    // WHILE/DO still open
    assert(sqlScriptingBlockDepth(
      """BEGIN
        |  WHILE x>0 DO
        |    SET x=x-1;""".stripMargin) === 2)
    // WHILE closed
    assert(sqlScriptingBlockDepth(
      """BEGIN
        |  WHILE x>0 DO
        |    SET x=x-1;
        |  END WHILE;""".stripMargin) === 1)
    // CASE expression: CASE increments, END decrements -- net zero inside the block
    assert(sqlScriptingBlockDepth(
      """BEGIN
        |  SELECT CASE WHEN 1=1 THEN 'a' END;""".stripMargin) === 1)
    // IF( is a Spark SQL function call, not a scripting IF -- must not increment
    assert(sqlScriptingBlockDepth(
      """BEGIN
        |  SELECT IF(1=1, 'a', 'b');""".stripMargin) === 1)
    // Keywords inside string literals must be ignored
    assert(sqlScriptingBlockDepth(
      """BEGIN
        |  SELECT 'END';""".stripMargin) === 1)
    // Keywords inside line comments must be ignored
    assert(sqlScriptingBlockDepth(
      """BEGIN
        |  -- END
        |  SELECT 1;""".stripMargin) === 1)
    // Keywords inside bracketed comments must be ignored
    assert(sqlScriptingBlockDepth("BEGIN /* END */ SELECT 1;") === 1)
  }

  testRetry("SPARK-39068: support in-memory catalog and running concurrently") {
    val extraConf = Seq("-c", s"${StaticSQLConf.CATALOG_IMPLEMENTATION.key}=in-memory")
    val cd = new CountDownLatch(2)
    def t: Thread = new Thread {
      override def run(): Unit = {
        // catalog is in-memory and isolated, so that we can create table with duplicated
        // names.
        runCliWithin(1.minute, extraArgs = extraConf)(
          "create table src(key int) using hive;" ->
            "NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT",
          "create table src(key int) using parquet;" -> "")
        cd.countDown()
      }
    }
    t.start()
    t.start()
    cd.await()
  }

  // scalastyle:off line.size.limit
  testRetry("formats of error messages") {
    def check(format: ErrorMessageFormat.Value, errorMessage: String, silent: Boolean): Unit = {
      val expected = errorMessage.split(System.lineSeparator()).map("" -> _)
      import org.apache.spark.util.ArrayImplicits._
      runCliWithin(
        1.minute,
        extraArgs = Seq(
          "--conf", s"spark.hive.session.silent=$silent",
          "--conf", s"${SQLConf.ERROR_MESSAGE_FORMAT.key}=$format",
          "--conf", s"${SQLConf.ANSI_ENABLED.key}=true",
          "-e", "select 1 / 0"),
        errorResponses = Seq("DIVIDE_BY_ZERO"))(expected.toImmutableArraySeq: _*)
    }
    // DIVIDE_BY_ZERO has SQLSTATE 22012 (not XX***), so it's a user error
    // It also has no cause, so stack traces should NOT be shown in PRETTY mode
    check(
      format = ErrorMessageFormat.PRETTY,
      errorMessage =
        """[DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead. If necessary set "spark.sql.ansi.enabled" to "false" to bypass this error.
          |== SQL (line 1, position 8) ==
          |select 1 / 0
          |       ^^^^^
          |""".stripMargin,
      silent = true)
    check(
      format = ErrorMessageFormat.PRETTY,
      errorMessage =
        """[DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead. If necessary set "spark.sql.ansi.enabled" to "false" to bypass this error.
          |== SQL (line 1, position 8) ==
          |select 1 / 0
          |       ^^^^^
          |""".stripMargin,
      silent = false)
    // DEBUG format should always show stack traces, even for user errors
    // Silent mode still suppresses stack traces
    check(
      format = ErrorMessageFormat.DEBUG,
      errorMessage =
        """[DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead. If necessary set "spark.sql.ansi.enabled" to "false" to bypass this error.
          |== SQL (line 1, position 8) ==
          |select 1 / 0
          |       ^^^^^
          |""".stripMargin,
      silent = true)
    check(
      format = ErrorMessageFormat.DEBUG,
      errorMessage =
        """[DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead. If necessary set "spark.sql.ansi.enabled" to "false" to bypass this error.
          |== SQL (line 1, position 8) ==
          |select 1 / 0
          |       ^^^^^
          |
          |org.apache.spark.SparkArithmeticException: [DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead. If necessary set "spark.sql.ansi.enabled" to "false" to bypass this error.
          |""".stripMargin,
      silent = false)
    Seq(true, false).foreach { silent =>
      check(
        format = ErrorMessageFormat.MINIMAL,
        errorMessage =
          """{
            |  "errorClass" : "DIVIDE_BY_ZERO",
            |  "sqlState" : "22012",
            |  "messageParameters" : {
            |    "config" : "\"spark.sql.ansi.enabled\""
            |  },
            |  "queryContext" : [ {
            |    "objectType" : "",
            |    "objectName" : "",
            |    "startIndex" : 8,
            |    "stopIndex" : 12,
            |    "fragment" : "1 / 0"
            |  } ]
            |}""".stripMargin,
        silent)
      check(
        format = ErrorMessageFormat.STANDARD,
        errorMessage =
          """{
            |  "errorClass" : "DIVIDE_BY_ZERO",
            |  "messageTemplate" : "Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead. If necessary set <config> to \"false\" to bypass this error.",
            |  "sqlState" : "22012",
            |  "messageParameters" : {
            |    "config" : "\"spark.sql.ansi.enabled\""
            |  },
            |  "queryContext" : [ {
            |    "objectType" : "",
            |    "objectName" : "",
            |    "startIndex" : 8,
            |    "stopIndex" : 12,
            |    "fragment" : "1 / 0"
            |  } ]
            |}""".stripMargin,
        silent)
    }
  }
  // scalastyle:on line.size.limit

  test("SPARK-35242: Support change catalog default database for spark") {
    // Create db and table first
    runCliWithin(2.minute,
      Seq("--conf", s"${StaticSQLConf.WAREHOUSE_PATH.key}=${sparkWareHouseDir}"))(
      "create database spark_35242;" -> "",
      "use spark_35242;" -> "",
      "CREATE TABLE spark_test(key INT, val STRING);" -> "")

    // Set default db
    runCliWithin(2.minute,
      Seq("--conf", s"${StaticSQLConf.WAREHOUSE_PATH.key}=${sparkWareHouseDir}",
          "--conf", s"${StaticSQLConf.CATALOG_DEFAULT_DATABASE.key}=spark_35242"))(
      "show tables;" -> "spark_test")
  }

  test("SPARK-42448: Print correct database in prompt") {
    runCliWithin(
      2.minute,
      Seq("--conf", s"${SQLConf.LEGACY_EMPTY_CURRENT_DB_IN_CLI.key}=false"),
      prompt = "spark-sql (default)>")(
      "set abc;" -> "abc\t<undefined>",
      "create database spark_42448;" -> "")

    runCliWithin(
      2.minute,
      Seq("--conf", s"${SQLConf.LEGACY_EMPTY_CURRENT_DB_IN_CLI.key}=false", "--database",
        "spark_42448"),
      prompt = "spark-sql (spark_42448)>")(
      "select current_database();" -> "spark_42448")
  }

  test("SPARK-42823: multipart identifier support for specify database by --database option") {
    val catalogName = "testcat"
    val catalogImpl = s"spark.sql.catalog.$catalogName=${classOf[JDBCTableCatalog].getName}"
    val catalogUrl =
      s"spark.sql.catalog.$catalogName.url=jdbc:derby:memory:$catalogName;create=true"
    val catalogDriver =
      s"spark.sql.catalog.$catalogName.driver=org.apache.derby.iapi.jdbc.AutoloadedDriver"
    val catalogConfigs =
      Seq(catalogImpl, catalogDriver, catalogUrl, "spark.sql.catalogImplementation=in-memory")
        .flatMap(Seq("--conf", _))
    runCliWithin(
      2.minute,
      catalogConfigs ++ Seq("--database", s"$catalogName.SYS"))(
      "SELECT CURRENT_CATALOG();" -> catalogName,
      "SELECT CURRENT_SCHEMA();" -> "SYS")

    runCliWithin(
      2.minute,
      catalogConfigs ++
        Seq("--conf", s"spark.sql.defaultCatalog=$catalogName", "--database", "SYS"))(
      "SELECT CURRENT_CATALOG();" -> catalogName,
      "SELECT CURRENT_SCHEMA();" -> "SYS")
  }

  test("SPARK-52426: do not redirect stderr and stdout in spark-sql") {
    runCliWithin(
      2.minute,
      extraArgs = "--conf" :: s"spark.plugins=${classOf[RedirectConsolePlugin].getName}" :: Nil)(
      "SELECT 1;" -> "1")
  }

  test("unbound parameter markers in CLI are detected and reported") {
    // Test that parameter markers without parameters are properly detected in spark-sql CLI
    // and throw UNBOUND_SQL_PARAMETER error instead of internal errors.
    // This guards against regression where SparkSQLDriver wasn't using pre-parser.
    runCliWithin(
      2.minute,
      errorResponses = Seq("UNBOUND_SQL_PARAMETER"))(
      "SELECT :param;" -> "param",
      "SELECT 'hello' :parm;" -> "parm",
      "SELECT ?;" -> ""
    )
  }

  test("SPARK-55198: spark-sql should skip comment line with leading whitespaces") {
    val sql = """SET x=
                | -- comment
                |1;
                |""".stripMargin
    runCliWithin(2.minutes)(sql -> "x\t1")

    withTempDir { tmpDir =>
      val sqlFilePath = tmpDir.toPath.resolve("test.sql").toAbsolutePath
      Files.writeString(sqlFilePath, sql)
      runCliWithin(2.minutes, extraArgs = Seq("-f", sqlFilePath.toString))("" -> "x\t1")
    }
  }
}
