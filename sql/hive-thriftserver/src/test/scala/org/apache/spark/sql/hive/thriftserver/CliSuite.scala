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
import java.sql.Timestamp
import java.util.Date

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Promise
import scala.concurrent.duration._

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.test.HiveTestJars
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.test.ProcessTestUtils.ProcessOutputCapturer
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * A test suite for the `spark-sql` CLI tool.
 */
class CliSuite extends SparkFunSuite with BeforeAndAfterAll with BeforeAndAfterEach with Logging {
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

  override def afterEach(): Unit = {
    // Only running `runCliWithin` in a single test case will share the same temporary
    // Hive metastore
    Utils.deleteRecursively(metastorePath)
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
   * @param queriesAndExpectedAnswers one or more tuples of query + answer
   */
  def runCliWithin(
      timeout: FiniteDuration,
      extraArgs: Seq[String] = Seq.empty,
      errorResponses: Seq[String] = Seq("Error:"),
      maybeWarehouse: Option[File] = Some(warehousePath),
      useExternalHiveFile: Boolean = false)(
      queriesAndExpectedAnswers: (String, String)*): Unit = {

    val (queries, expectedAnswers) = queriesAndExpectedAnswers.unzip
    // Explicitly adds ENTER for each statement to make sure they are actually entered into the CLI.
    val queriesString = queries.map(_ + "\n").mkString

    val extraHive = if (useExternalHiveFile) {
      s"--driver-class-path ${System.getProperty("user.dir")}/src/test/noclasspath"
    } else {
      ""
    }
    val warehouseConf =
      maybeWarehouse.map(dir => s"--hiveconf ${ConfVars.METASTOREWAREHOUSE}=$dir").getOrElse("")
    val command = {
      val cliScript = "../../bin/spark-sql".split("/").mkString(File.separator)
      val jdbcUrl = s"jdbc:derby:;databaseName=$metastorePath;create=true"
      s"""$cliScript
         |  --master local
         |  --driver-java-options -Dderby.system.durability=test
         |  $extraHive
         |  --conf spark.ui.enabled=false
         |  --hiveconf ${ConfVars.METASTORECONNECTURLKEY}=$jdbcUrl
         |  --hiveconf ${ConfVars.SCRATCHDIR}=$scratchDirPath
         |  --hiveconf conf1=conftest
         |  --hiveconf conf2=1
         |  $warehouseConf
       """.stripMargin.split("\\s+").toSeq ++ extraArgs
    }

    var next = 0
    val foundAllExpectedAnswers = Promise.apply[Unit]()
    val buffer = new ArrayBuffer[String]()
    val lock = new Object

    def captureOutput(source: String)(line: String): Unit = lock.synchronized {
      // This test suite sometimes gets extremely slow out of unknown reason on Jenkins.  Here we
      // add a timestamp to provide more diagnosis information.
      buffer += s"${new Timestamp(new Date().getTime)} - $source> $line"

      // If we haven't found all expected answers and another expected answer comes up...
      if (next < expectedAnswers.size && line.contains(expectedAnswers(next))) {
        next += 1
        // If all expected answers have been found...
        if (next == expectedAnswers.size) {
          foundAllExpectedAnswers.trySuccess(())
        }
      } else {
        errorResponses.foreach { r =>
          if (line.contains(r)) {
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
      ThreadUtils.awaitResult(foundAllExpectedAnswers.future, timeout)
    } catch { case cause: Throwable =>
      val message =
        s"""
           |=======================
           |CliSuite failure output
           |=======================
           |Spark SQL CLI command line: ${command.mkString(" ")}
           |Exception: $cause
           |Executed query $next "${queries(next)}",
           |But failed to capture expected output "${expectedAnswers(next)}" within $timeout.
           |
           |${buffer.mkString("\n")}
           |===========================
           |End CliSuite failure output
           |===========================
         """.stripMargin
      logError(message, cause)
      fail(message, cause)
    } finally {
      process.destroy()
    }
  }

  test("load warehouse dir from hive-site.xml") {
    runCliWithin(1.minute, maybeWarehouse = None, useExternalHiveFile = true)(
      "desc database default;" -> "hive_one",
      "set spark.sql.warehouse.dir;" -> "hive_one")
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
    runCliWithin(
      2.minute,
      extraArgs = Seq("--conf", s"spark.hadoop.${ConfVars.METASTOREWAREHOUSE}=$sparkWareHouseDir"),
      maybeWarehouse = None,
      useExternalHiveFile = true)(
      "desc database default;" -> sparkWareHouseDir.getAbsolutePath,
      "create database cliTestDb;" -> "",
      "desc database cliTestDb;" -> sparkWareHouseDir.getAbsolutePath,
      "set spark.sql.warehouse.dir;" -> sparkWareHouseDir.getAbsolutePath)

    // override conf from --hiveconf too
    runCliWithin(
      2.minute,
      extraArgs = Seq("--conf", s"spark.${ConfVars.METASTOREWAREHOUSE}=$sparkWareHouseDir"))(
      "desc database default;" -> sparkWareHouseDir.getAbsolutePath,
      "create database cliTestDb;" -> "",
      "desc database cliTestDb;" -> sparkWareHouseDir.getAbsolutePath,
      "set spark.sql.warehouse.dir;" -> sparkWareHouseDir.getAbsolutePath)
  }

  test("load warehouse dir from spark.sql.warehouse.dir") {
    // spark.sql.warehouse.dir overrides all hive ones
    runCliWithin(
      2.minute,
      extraArgs =
        Seq("--conf",
          s"${StaticSQLConf.WAREHOUSE_PATH.key}=${sparkWareHouseDir}1",
          "--conf", s"spark.hadoop.${ConfVars.METASTOREWAREHOUSE}=${sparkWareHouseDir}2"))(
      "desc database default;" -> sparkWareHouseDir.getAbsolutePath.concat("1"))
  }

  test("Simple commands") {
    val dataFilePath =
      Thread.currentThread().getContextClassLoader.getResource("data/files/small_kv.txt")

    runCliWithin(3.minute)(
      "CREATE TABLE hive_test(key INT, val STRING) USING hive;"
        -> "",
      "SHOW TABLES;"
        -> "hive_test",
      s"LOAD DATA LOCAL INPATH '$dataFilePath' OVERWRITE INTO TABLE hive_test;"
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
      "CREATE DATABASE hive_test_db;"
        -> "",
      "USE hive_test_db;"
        -> "",
      "CREATE TABLE hive_test(key INT, val STRING);"
        -> "",
      "SHOW TABLES;"
        -> "hive_test"
    )

    runCliWithin(2.minute, Seq("--database", "hive_test_db", "-e", "SHOW TABLES;"))(
      "" -> "hive_test"
    )
  }

  test("Commands using SerDe provided in --jars") {
    val jarFile = HiveTestJars.getHiveHcatalogCoreJar().getCanonicalPath

    val dataFilePath =
      Thread.currentThread().getContextClassLoader.getResource("data/files/small_kv.txt")

    runCliWithin(3.minute, Seq("--jars", s"$jarFile"))(
      """CREATE TABLE t1(key string, val string)
        |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe';
      """.stripMargin
        -> "",
      "CREATE TABLE sourceTable (key INT, val STRING) USING hive;"
        -> "",
      s"LOAD DATA LOCAL INPATH '$dataFilePath' OVERWRITE INTO TABLE sourceTable;"
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
        |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe';
      """.stripMargin
        -> "",
      "CREATE TABLE sourceTableForWithHiveAux (key INT, val STRING) USING hive;"
        -> "",
      s"LOAD DATA LOCAL INPATH '$dataFilePath' OVERWRITE INTO TABLE sourceTableForWithHiveAux;"
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

  test("SPARK-11188 Analysis error reporting") {
    runCliWithin(timeout = 2.minute,
      errorResponses = Seq("AnalysisException"))(
      "select * from nonexistent_table;"
        -> "Error in query: Table or view not found: nonexistent_table;"
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
      1.minute,
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
        |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe';
      """.stripMargin
        -> "",
      "CREATE TABLE sourceTableForWithSQL(key INT, val STRING) USING hive;"
        -> "",
      s"LOAD DATA LOCAL INPATH '$dataFilePath' OVERWRITE INTO TABLE sourceTableForWithSQL;"
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

  test("SPARK-30049 Should not complain for quotes in commented with multi-lines") {
    runCliWithin(1.minute)(
      """SELECT concat('test', 'comment') -- someone's comment here \\
        | comment continues here with single ' quote \\
        | extra ' \\
        |;""".stripMargin -> "testcomment"
    )
    runCliWithin(1.minute)(
      """SELECT concat('test', 'comment') -- someone's comment here \\
        |   comment continues here with single ' quote \\
        |   extra ' \\
        |   ;""".stripMargin -> "testcomment"
    )
  }
}
