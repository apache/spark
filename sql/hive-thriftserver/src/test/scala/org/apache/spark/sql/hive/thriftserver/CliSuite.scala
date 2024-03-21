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
import java.util.concurrent.CountDownLatch

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.apache.hadoop.hive.cli.CliSessionState
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.{ErrorMessageFormat, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.ProcessTestUtils.ProcessOutputCapturer
import org.apache.spark.deploy.SparkHadoopUtil
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
      maybeWarehouse.map(dir => s"--hiveconf ${ConfVars.METASTOREWAREHOUSE}=$dir").getOrElse("")
    val command = {
      val cliScript = "../../bin/spark-sql".split("/").mkString(File.separator)
      val jdbcUrl = s"jdbc:derby:;databaseName=$metastore;create=true"
      s"""$cliScript
         |  --master local
         |  --driver-java-options -Dderby.system.durability=test
         |  $extraHive
         |  --conf spark.ui.enabled=false
         |  --conf ${SQLConf.LEGACY_EMPTY_CURRENT_DB_IN_CLI.key}=true
         |  --hiveconf ${ConfVars.METASTORECONNECTURLKEY}=$jdbcUrl
         |  --hiveconf ${ConfVars.SCRATCHDIR}=$scratchDirPath
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
          Seq("--conf", s"spark.hadoop.${ConfVars.METASTOREWAREHOUSE}=$sparkWareHouseDir"),
        maybeWarehouse = None,
        useExternalHiveFile = true,
        metastore = metastore)(
        "desc database default;" -> sparkWareHouseDir.getAbsolutePath,
        "create database cliTestDb;" -> "",
        "desc database cliTestDb;" -> sparkWareHouseDir.getAbsolutePath,
        "set spark.sql.warehouse.dir;" -> sparkWareHouseDir.getAbsolutePath)

      // override conf from --hiveconf too
      runCliWithin(2.minute,
        extraArgs = Seq("--conf", s"spark.${ConfVars.METASTOREWAREHOUSE}=$sparkWareHouseDir"),
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
            "--conf", s"spark.hadoop.${ConfVars.METASTOREWAREHOUSE}=${sparkWareHouseDir}2"),
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
      Seq("--conf", s"spark.hadoop.${ConfVars.HIVEAUXJARS}=$hiveContribJar"))(
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
    runCliWithin(2.minute)(
      s"ADD JAR $jarFile;" -> "",
      s"LIST JARS;" -> "TestUDTF.jar"
    )
  }

  test("list jar <jarfile>") {
    val jarFile = Thread.currentThread().getContextClassLoader.getResource("TestUDTF.jar")
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
      Seq("--conf", s"spark.hadoop.${ConfVars.HIVEAUXJARS}=$hiveContribJar"))(
      "CREATE TEMPORARY FUNCTION example_format AS " +
        "'org.apache.hadoop.hive.contrib.udf.example.UDFExampleFormat';" -> "",
      "SELECT example_format('%o', 93);" -> "135"
    )
  }

  test("SPARK-28840 test --jars command") {
    val jarFile = new File("../../sql/hive/src/test/resources/SPARK-21101-1.0.jar").getCanonicalPath
    runCliWithin(
      1.minute,
      Seq("--jars", s"$jarFile"))(
      "CREATE TEMPORARY FUNCTION testjar AS" +
        " 'org.apache.spark.sql.hive.execution.UDTFStack';" -> "",
      "SELECT testjar(1,'TEST-SPARK-TEST-jar', 28840);" -> "TEST-SPARK-TEST-jar\t28840"
    )
  }

  test("SPARK-28840 test --jars and hive.aux.jars.path command") {
    val jarFile = new File("../../sql/hive/src/test/resources/SPARK-21101-1.0.jar").getCanonicalPath
    val hiveContribJar = HiveTestJars.getHiveContribJar().getCanonicalPath
    runCliWithin(
      2.minutes,
      Seq("--jars", s"$jarFile", "--conf",
        s"spark.hadoop.${ConfVars.HIVEAUXJARS}=$hiveContribJar"))(
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

  testRetry("SparkException with root cause will be printStacktrace") {
    // If it is not in silent mode, will print the stacktrace
    runCliWithin(
      1.minute,
      extraArgs = Seq("--hiveconf", "hive.session.silent=false",
        "-e", "select from_json('a', 'a INT', map('mode', 'FAILFAST'));"),
      errorResponses = Seq("JsonParseException"))(
      ("", "SparkException: [MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION]"),
      ("", "JsonParseException: Unrecognized token 'a'"))
    // If it is in silent mode, will print the error message only
    runCliWithin(
      1.minute,
      extraArgs = Seq("--conf", "spark.hive.session.silent=true",
        "-e", "select from_json('a', 'a INT', map('mode', 'FAILFAST'));"),
      errorResponses = Seq("SparkException"))(
      ("", "SparkException: [MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION]"))
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
      assert(cli.splitSemiColon(query).asScala === ret)
    }
    sessionState.close()
    SparkSQLEnv.stop()
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
}
