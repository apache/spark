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

package org.apache.spark.sql.hive

import java.io.{BufferedWriter, File, FileWriter}

import scala.util.Properties

import org.apache.hadoop.fs.Path
import org.scalatest.{BeforeAndAfterEach, Matchers}

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.test.{TestHive, TestHiveContext}
import org.apache.spark.sql.types.{DecimalType, StructType}
import org.apache.spark.tags.ExtendedHiveTest
import org.apache.spark.util.{ResetSystemProperties, Utils}

/**
 * This suite tests spark-submit with applications using HiveContext.
 */
@ExtendedHiveTest
class HiveSparkSubmitSuite
  extends SparkSubmitTestUtils
  with Matchers
  with BeforeAndAfterEach
  with ResetSystemProperties {

  override protected val enableAutoThreadAudit = false

  override def beforeEach() {
    super.beforeEach()
  }

  test("temporary Hive UDF: define a UDF and use it") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val jar1 = TestUtils.createJarWithClasses(Seq("SparkSubmitClassA"))
    val jar2 = TestUtils.createJarWithClasses(Seq("SparkSubmitClassB"))
    val jarsString = Seq(jar1, jar2).map(j => j.toString).mkString(",")
    val args = Seq(
      "--class", TemporaryHiveUDFTest.getClass.getName.stripSuffix("$"),
      "--name", "TemporaryHiveUDFTest",
      "--master", "local-cluster[2,1,1024]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--driver-java-options", "-Dderby.system.durability=test",
      "--jars", jarsString,
      unusedJar.toString, "SparkSubmitClassA", "SparkSubmitClassB")
    runSparkSubmit(args)
  }

  test("permanent Hive UDF: define a UDF and use it") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val jar1 = TestUtils.createJarWithClasses(Seq("SparkSubmitClassA"))
    val jar2 = TestUtils.createJarWithClasses(Seq("SparkSubmitClassB"))
    val jarsString = Seq(jar1, jar2).map(j => j.toString).mkString(",")
    val args = Seq(
      "--class", PermanentHiveUDFTest1.getClass.getName.stripSuffix("$"),
      "--name", "PermanentHiveUDFTest1",
      "--master", "local-cluster[2,1,1024]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--driver-java-options", "-Dderby.system.durability=test",
      "--jars", jarsString,
      unusedJar.toString, "SparkSubmitClassA", "SparkSubmitClassB")
    runSparkSubmit(args)
  }

  test("permanent Hive UDF: use a already defined permanent function") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val jar1 = TestUtils.createJarWithClasses(Seq("SparkSubmitClassA"))
    val jar2 = TestUtils.createJarWithClasses(Seq("SparkSubmitClassB"))
    val jarsString = Seq(jar1, jar2).map(j => j.toString).mkString(",")
    val args = Seq(
      "--class", PermanentHiveUDFTest2.getClass.getName.stripSuffix("$"),
      "--name", "PermanentHiveUDFTest2",
      "--master", "local-cluster[2,1,1024]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--driver-java-options", "-Dderby.system.durability=test",
      "--jars", jarsString,
      unusedJar.toString, "SparkSubmitClassA", "SparkSubmitClassB")
    runSparkSubmit(args)
  }

  test("SPARK-8368: includes jars passed in through --jars") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val jar1 = TestUtils.createJarWithClasses(Seq("SparkSubmitClassA"))
    val jar2 = TestUtils.createJarWithClasses(Seq("SparkSubmitClassB"))
    val jar3 = TestHive.getHiveContribJar().getCanonicalPath
    val jar4 = TestHive.getHiveHcatalogCoreJar().getCanonicalPath
    val jarsString = Seq(jar1, jar2, jar3, jar4).map(j => j.toString).mkString(",")
    val args = Seq(
      "--class", SparkSubmitClassLoaderTest.getClass.getName.stripSuffix("$"),
      "--name", "SparkSubmitClassLoaderTest",
      "--master", "local-cluster[2,1,1024]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--driver-java-options", "-Dderby.system.durability=test",
      "--jars", jarsString,
      unusedJar.toString, "SparkSubmitClassA", "SparkSubmitClassB")
    runSparkSubmit(args)
  }

  test("SPARK-8020: set sql conf in spark conf") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val args = Seq(
      "--class", SparkSQLConfTest.getClass.getName.stripSuffix("$"),
      "--name", "SparkSQLConfTest",
      "--master", "local-cluster[2,1,1024]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--conf", "spark.sql.hive.metastore.version=0.12",
      "--conf", "spark.sql.hive.metastore.jars=maven",
      "--driver-java-options", "-Dderby.system.durability=test",
      unusedJar.toString)
    runSparkSubmit(args)
  }

  test("SPARK-8489: MissingRequirementError during reflection") {
    // This test uses a pre-built jar to test SPARK-8489. In a nutshell, this test creates
    // a HiveContext and uses it to create a data frame from an RDD using reflection.
    // Before the fix in SPARK-8470, this results in a MissingRequirementError because
    // the HiveContext code mistakenly overrides the class loader that contains user classes.
    // For more detail, see sql/hive/src/test/resources/regression-test-SPARK-8489/*scala.
    // TODO: revisit for Scala 2.13 support
    val version = Properties.versionNumberString match {
      case v if v.startsWith("2.12") => v.substring(0, 4)
      case x => throw new Exception(s"Unsupported Scala Version: $x")
    }
    val jarDir = getTestResourcePath("regression-test-SPARK-8489")
    val testJar = s"$jarDir/test-$version.jar"
    val args = Seq(
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--driver-java-options", "-Dderby.system.durability=test",
      "--class", "Main",
      testJar)
    runSparkSubmit(args)
  }

  test("SPARK-9757 Persist Parquet relation with decimal column") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val args = Seq(
      "--class", SPARK_9757.getClass.getName.stripSuffix("$"),
      "--name", "SparkSQLConfTest",
      "--master", "local-cluster[2,1,1024]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--driver-java-options", "-Dderby.system.durability=test",
      unusedJar.toString)
    runSparkSubmit(args)
  }

  test("SPARK-11009 fix wrong result of Window function in cluster mode") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val args = Seq(
      "--class", SPARK_11009.getClass.getName.stripSuffix("$"),
      "--name", "SparkSQLConfTest",
      "--master", "local-cluster[2,1,1024]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--driver-java-options", "-Dderby.system.durability=test",
      unusedJar.toString)
    runSparkSubmit(args)
  }

  test("SPARK-14244 fix window partition size attribute binding failure") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val args = Seq(
      "--class", SPARK_14244.getClass.getName.stripSuffix("$"),
      "--name", "SparkSQLConfTest",
      "--master", "local-cluster[2,1,1024]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--driver-java-options", "-Dderby.system.durability=test",
      unusedJar.toString)
    runSparkSubmit(args)
  }

  test("set spark.sql.warehouse.dir") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val args = Seq(
      "--class", SetWarehouseLocationTest.getClass.getName.stripSuffix("$"),
      "--name", "SetSparkWarehouseLocationTest",
      "--master", "local-cluster[2,1,1024]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--driver-java-options", "-Dderby.system.durability=test",
      unusedJar.toString)
    runSparkSubmit(args)
  }

  test("set hive.metastore.warehouse.dir") {
    // In this test, we set hive.metastore.warehouse.dir in hive-site.xml but
    // not set spark.sql.warehouse.dir. So, the warehouse dir should be
    // the value of hive.metastore.warehouse.dir. Also, the value of
    // spark.sql.warehouse.dir should be set to the value of hive.metastore.warehouse.dir.

    val hiveWarehouseLocation = Utils.createTempDir()
    hiveWarehouseLocation.delete()
    val hiveSiteXmlContent =
      s"""
         |<configuration>
         |  <property>
         |    <name>hive.metastore.warehouse.dir</name>
         |    <value>$hiveWarehouseLocation</value>
         |  </property>
         |</configuration>
     """.stripMargin

    // Write a hive-site.xml containing a setting of hive.metastore.warehouse.dir.
    val hiveSiteDir = Utils.createTempDir()
    val file = new File(hiveSiteDir.getCanonicalPath, "hive-site.xml")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(hiveSiteXmlContent)
    bw.close()

    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val args = Seq(
      "--class", SetWarehouseLocationTest.getClass.getName.stripSuffix("$"),
      "--name", "SetHiveWarehouseLocationTest",
      "--master", "local-cluster[2,1,1024]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--conf", s"spark.sql.test.expectedWarehouseDir=$hiveWarehouseLocation",
      "--conf", s"spark.driver.extraClassPath=${hiveSiteDir.getCanonicalPath}",
      "--driver-java-options", "-Dderby.system.durability=test",
      unusedJar.toString)
    runSparkSubmit(args)
  }

  test("SPARK-16901: set javax.jdo.option.ConnectionURL") {
    // In this test, we set javax.jdo.option.ConnectionURL and set metastore version to
    // 0.13. This test will make sure that javax.jdo.option.ConnectionURL will not be
    // overridden by hive's default settings when we create a HiveConf object inside
    // HiveClientImpl. Please see SPARK-16901 for more details.

    val metastoreLocation = Utils.createTempDir()
    metastoreLocation.delete()
    val metastoreURL =
      s"jdbc:derby:memory:;databaseName=${metastoreLocation.getAbsolutePath};create=true"
    val hiveSiteXmlContent =
      s"""
         |<configuration>
         |  <property>
         |    <name>javax.jdo.option.ConnectionURL</name>
         |    <value>$metastoreURL</value>
         |  </property>
         |</configuration>
     """.stripMargin

    // Write a hive-site.xml containing a setting of hive.metastore.warehouse.dir.
    val hiveSiteDir = Utils.createTempDir()
    val file = new File(hiveSiteDir.getCanonicalPath, "hive-site.xml")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(hiveSiteXmlContent)
    bw.close()

    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val args = Seq(
      "--class", SetMetastoreURLTest.getClass.getName.stripSuffix("$"),
      "--name", "SetMetastoreURLTest",
      "--master", "local[1]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--conf", s"spark.sql.test.expectedMetastoreURL=$metastoreURL",
      "--conf", s"spark.driver.extraClassPath=${hiveSiteDir.getCanonicalPath}",
      "--driver-java-options", "-Dderby.system.durability=test",
      unusedJar.toString)
    runSparkSubmit(args)
  }

  test("SPARK-18360: default table path of tables in default database should depend on the " +
    "location of default database") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val args = Seq(
      "--class", SPARK_18360.getClass.getName.stripSuffix("$"),
      "--name", "SPARK-18360",
      "--master", "local-cluster[2,1,1024]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--driver-java-options", "-Dderby.system.durability=test",
      unusedJar.toString)
    runSparkSubmit(args)
  }

  test("SPARK-18989: DESC TABLE should not fail with format class not found") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)

    val argsForCreateTable = Seq(
      "--class", SPARK_18989_CREATE_TABLE.getClass.getName.stripSuffix("$"),
      "--name", "SPARK-18947",
      "--master", "local-cluster[2,1,1024]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--jars", TestHive.getHiveContribJar().getCanonicalPath,
      unusedJar.toString)
    runSparkSubmit(argsForCreateTable)

    val argsForShowTables = Seq(
      "--class", SPARK_18989_DESC_TABLE.getClass.getName.stripSuffix("$"),
      "--name", "SPARK-18947",
      "--master", "local-cluster[2,1,1024]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      unusedJar.toString)
    runSparkSubmit(argsForShowTables)
  }
}

object SetMetastoreURLTest extends Logging {
  def main(args: Array[String]): Unit = {
    TestUtils.configTestLog4j("INFO")

    val sparkConf = new SparkConf(loadDefaults = true)
    val builder = SparkSession.builder()
      .config(sparkConf)
      .config(UI_ENABLED.key, "false")
      .config("spark.sql.hive.metastore.version", "0.13.1")
      // The issue described in SPARK-16901 only appear when
      // spark.sql.hive.metastore.jars is not set to builtin.
      .config("spark.sql.hive.metastore.jars", "maven")
      .enableHiveSupport()

    val spark = builder.getOrCreate()
    val expectedMetastoreURL =
      spark.conf.get("spark.sql.test.expectedMetastoreURL")
    logInfo(s"spark.sql.test.expectedMetastoreURL is $expectedMetastoreURL")

    if (expectedMetastoreURL == null) {
      throw new Exception(
        s"spark.sql.test.expectedMetastoreURL should be set.")
    }

    // HiveExternalCatalog is used when Hive support is enabled.
    val actualMetastoreURL =
      spark.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client
        .getConf("javax.jdo.option.ConnectionURL", "this_is_a_wrong_URL")
    logInfo(s"javax.jdo.option.ConnectionURL is $actualMetastoreURL")

    if (actualMetastoreURL != expectedMetastoreURL) {
      throw new Exception(
        s"Expected value of javax.jdo.option.ConnectionURL is $expectedMetastoreURL. But, " +
          s"the actual value is $actualMetastoreURL")
    }
  }
}

object SetWarehouseLocationTest extends Logging {
  def main(args: Array[String]): Unit = {
    TestUtils.configTestLog4j("INFO")

    val sparkConf = new SparkConf(loadDefaults = true).set(UI_ENABLED, false)
    val providedExpectedWarehouseLocation =
      sparkConf.getOption("spark.sql.test.expectedWarehouseDir")

    val (sparkSession, expectedWarehouseLocation) = providedExpectedWarehouseLocation match {
      case Some(warehouseDir) =>
        // If spark.sql.test.expectedWarehouseDir is set, the warehouse dir is set
        // through spark-summit. So, neither spark.sql.warehouse.dir nor
        // hive.metastore.warehouse.dir is set at here.
        (new TestHiveContext(new SparkContext(sparkConf)).sparkSession, warehouseDir)
      case None =>
        val warehouseLocation = Utils.createTempDir()
        warehouseLocation.delete()
        val hiveWarehouseLocation = Utils.createTempDir()
        hiveWarehouseLocation.delete()
        // If spark.sql.test.expectedWarehouseDir is not set, we will set
        // spark.sql.warehouse.dir and hive.metastore.warehouse.dir.
        // We are expecting that the value of spark.sql.warehouse.dir will override the
        // value of hive.metastore.warehouse.dir.
        val session = new TestHiveContext(new SparkContext(sparkConf
          .set("spark.sql.warehouse.dir", warehouseLocation.toString)
          .set("hive.metastore.warehouse.dir", hiveWarehouseLocation.toString)))
          .sparkSession
        (session, warehouseLocation.toString)

    }

    if (sparkSession.conf.get("spark.sql.warehouse.dir") != expectedWarehouseLocation) {
      throw new Exception(
        "spark.sql.warehouse.dir is not set to the expected warehouse location " +
        s"$expectedWarehouseLocation.")
    }

    val catalog = sparkSession.sessionState.catalog

    sparkSession.sql("drop table if exists testLocation")
    sparkSession.sql("drop database if exists testLocationDB cascade")

    {
      sparkSession.sql("create table testLocation (a int)")
      val tableMetadata =
        catalog.getTableMetadata(TableIdentifier("testLocation", Some("default")))
      val expectedLocation =
        CatalogUtils.stringToURI(s"file:${expectedWarehouseLocation.toString}/testlocation")
      val actualLocation = tableMetadata.location
      if (actualLocation != expectedLocation) {
        throw new Exception(
          s"Expected table location is $expectedLocation. But, it is actually $actualLocation")
      }
      sparkSession.sql("drop table testLocation")
    }

    {
      sparkSession.sql("create database testLocationDB")
      sparkSession.sql("use testLocationDB")
      sparkSession.sql("create table testLocation (a int)")
      val tableMetadata =
        catalog.getTableMetadata(TableIdentifier("testLocation", Some("testLocationDB")))
      val expectedLocation = CatalogUtils.stringToURI(
        s"file:${expectedWarehouseLocation.toString}/testlocationdb.db/testlocation")
      val actualLocation = tableMetadata.location
      if (actualLocation != expectedLocation) {
        throw new Exception(
          s"Expected table location is $expectedLocation. But, it is actually $actualLocation")
      }
      sparkSession.sql("drop table testLocation")
      sparkSession.sql("use default")
      sparkSession.sql("drop database testLocationDB")
    }
  }
}

// This application is used to test defining a new Hive UDF (with an associated jar)
// and use this UDF. We need to run this test in separate JVM to make sure we
// can load the jar defined with the function.
object TemporaryHiveUDFTest extends Logging {
  def main(args: Array[String]) {
    TestUtils.configTestLog4j("INFO")
    val conf = new SparkConf()
    conf.set(UI_ENABLED, false)
    val sc = new SparkContext(conf)
    val hiveContext = new TestHiveContext(sc)

    // Load a Hive UDF from the jar.
    logInfo("Registering a temporary Hive UDF provided in a jar.")
    val jar = hiveContext.getHiveContribJar().getCanonicalPath
    hiveContext.sql(
      s"""
         |CREATE TEMPORARY FUNCTION example_max
         |AS 'org.apache.hadoop.hive.contrib.udaf.example.UDAFExampleMax'
         |USING JAR '$jar'
      """.stripMargin)
    val source =
      hiveContext.createDataFrame((1 to 10).map(i => (i, s"str$i"))).toDF("key", "val")
    source.createOrReplaceTempView("sourceTable")
    // Actually use the loaded UDF.
    logInfo("Using the UDF.")
    val result = hiveContext.sql(
      "SELECT example_max(key) as key, val FROM sourceTable GROUP BY val")
    logInfo("Running a simple query on the table.")
    val count = result.orderBy("key", "val").count()
    if (count != 10) {
      throw new Exception(s"Result table should have 10 rows instead of $count rows")
    }
    hiveContext.sql("DROP temporary FUNCTION example_max")
    logInfo("Test finishes.")
    sc.stop()
  }
}

// This application is used to test defining a new Hive UDF (with an associated jar)
// and use this UDF. We need to run this test in separate JVM to make sure we
// can load the jar defined with the function.
object PermanentHiveUDFTest1 extends Logging {
  def main(args: Array[String]) {
    TestUtils.configTestLog4j("INFO")
    val conf = new SparkConf()
    conf.set(UI_ENABLED, false)
    val sc = new SparkContext(conf)
    val hiveContext = new TestHiveContext(sc)

    // Load a Hive UDF from the jar.
    logInfo("Registering a permanent Hive UDF provided in a jar.")
    val jar = hiveContext.getHiveContribJar().getCanonicalPath
    hiveContext.sql(
      s"""
         |CREATE FUNCTION example_max
         |AS 'org.apache.hadoop.hive.contrib.udaf.example.UDAFExampleMax'
         |USING JAR '$jar'
      """.stripMargin)
    val source =
      hiveContext.createDataFrame((1 to 10).map(i => (i, s"str$i"))).toDF("key", "val")
    source.createOrReplaceTempView("sourceTable")
    // Actually use the loaded UDF.
    logInfo("Using the UDF.")
    val result = hiveContext.sql(
      "SELECT example_max(key) as key, val FROM sourceTable GROUP BY val")
    logInfo("Running a simple query on the table.")
    val count = result.orderBy("key", "val").count()
    if (count != 10) {
      throw new Exception(s"Result table should have 10 rows instead of $count rows")
    }
    hiveContext.sql("DROP FUNCTION example_max")
    logInfo("Test finishes.")
    sc.stop()
  }
}

// This application is used to test that a pre-defined permanent function with a jar
// resources can be used. We need to run this test in separate JVM to make sure we
// can load the jar defined with the function.
object PermanentHiveUDFTest2 extends Logging {
  def main(args: Array[String]) {
    TestUtils.configTestLog4j("INFO")
    val conf = new SparkConf()
    conf.set(UI_ENABLED, false)
    val sc = new SparkContext(conf)
    val hiveContext = new TestHiveContext(sc)
    // Load a Hive UDF from the jar.
    logInfo("Write the metadata of a permanent Hive UDF into metastore.")
    val jar = hiveContext.getHiveContribJar().getCanonicalPath
    val function = CatalogFunction(
      FunctionIdentifier("example_max"),
      "org.apache.hadoop.hive.contrib.udaf.example.UDAFExampleMax",
      FunctionResource(JarResource, jar) :: Nil)
    hiveContext.sessionState.catalog.createFunction(function, ignoreIfExists = false)
    val source =
      hiveContext.createDataFrame((1 to 10).map(i => (i, s"str$i"))).toDF("key", "val")
    source.createOrReplaceTempView("sourceTable")
    // Actually use the loaded UDF.
    logInfo("Using the UDF.")
    val result = hiveContext.sql(
      "SELECT example_max(key) as key, val FROM sourceTable GROUP BY val")
    logInfo("Running a simple query on the table.")
    val count = result.orderBy("key", "val").count()
    if (count != 10) {
      throw new Exception(s"Result table should have 10 rows instead of $count rows")
    }
    hiveContext.sql("DROP FUNCTION example_max")
    logInfo("Test finishes.")
    sc.stop()
  }
}

// This object is used for testing SPARK-8368: https://issues.apache.org/jira/browse/SPARK-8368.
// We test if we can load user jars in both driver and executors when HiveContext is used.
object SparkSubmitClassLoaderTest extends Logging {
  def main(args: Array[String]) {
    TestUtils.configTestLog4j("INFO")
    val conf = new SparkConf()
    val hiveWarehouseLocation = Utils.createTempDir()
    conf.set(UI_ENABLED, false)
    conf.set("spark.sql.warehouse.dir", hiveWarehouseLocation.toString)
    val sc = new SparkContext(conf)
    val hiveContext = new TestHiveContext(sc)
    val df = hiveContext.createDataFrame((1 to 100).map(i => (i, i))).toDF("i", "j")
    logInfo("Testing load classes at the driver side.")
    // First, we load classes at driver side.
    try {
      Utils.classForName(args(0))
      Utils.classForName(args(1))
    } catch {
      case t: Throwable =>
        throw new Exception("Could not load user class from jar:\n", t)
    }
    // Second, we load classes at the executor side.
    logInfo("Testing load classes at the executor side.")
    val result = df.rdd.mapPartitions { x =>
      var exception: String = null
      try {
        Utils.classForName(args(0))
        Utils.classForName(args(1))
      } catch {
        case t: Throwable =>
          exception = t + "\n" + Utils.exceptionString(t)
          exception = exception.replaceAll("\n", "\n\t")
      }
      Option(exception).toSeq.iterator
    }.collect()
    if (result.nonEmpty) {
      throw new Exception("Could not load user class from jar:\n" + result(0))
    }

    // Load a Hive UDF from the jar.
    logInfo("Registering temporary Hive UDF provided in a jar.")
    hiveContext.sql(
      """
        |CREATE TEMPORARY FUNCTION example_max
        |AS 'org.apache.hadoop.hive.contrib.udaf.example.UDAFExampleMax'
      """.stripMargin)
    val source =
      hiveContext.createDataFrame((1 to 10).map(i => (i, s"str$i"))).toDF("key", "val")
    source.createOrReplaceTempView("sourceTable")
    // Load a Hive SerDe from the jar.
    logInfo("Creating a Hive table with a SerDe provided in a jar.")
    hiveContext.sql(
      """
        |CREATE TABLE t1(key int, val string)
        |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
      """.stripMargin)
    // Actually use the loaded UDF and SerDe.
    logInfo("Writing data into the table.")
    hiveContext.sql(
      "INSERT INTO TABLE t1 SELECT example_max(key) as key, val FROM sourceTable GROUP BY val")
    logInfo("Running a simple query on the table.")
    val count = hiveContext.table("t1").orderBy("key", "val").count()
    if (count != 10) {
      throw new Exception(s"table t1 should have 10 rows instead of $count rows")
    }
    logInfo("Test finishes.")
    sc.stop()
  }
}

// This object is used for testing SPARK-8020: https://issues.apache.org/jira/browse/SPARK-8020.
// We test if we can correctly set spark sql configurations when HiveContext is used.
object SparkSQLConfTest extends Logging {
  def main(args: Array[String]) {
    TestUtils.configTestLog4j("INFO")
    // We override the SparkConf to add spark.sql.hive.metastore.version and
    // spark.sql.hive.metastore.jars to the beginning of the conf entry array.
    // So, if metadataHive get initialized after we set spark.sql.hive.metastore.version but
    // before spark.sql.hive.metastore.jars get set, we will see the following exception:
    // Exception in thread "main" java.lang.IllegalArgumentException: Builtin jars can only
    // be used when hive execution version == hive metastore version.
    // Execution: 0.13.1 != Metastore: 0.12. Specify a valid path to the correct hive jars
    // using $HIVE_METASTORE_JARS or change spark.sql.hive.metastore.version to 0.13.1.
    val conf = new SparkConf() {
      override def getAll: Array[(String, String)] = {
        def isMetastoreSetting(conf: String): Boolean = {
          conf == "spark.sql.hive.metastore.version" || conf == "spark.sql.hive.metastore.jars"
        }
        // If there is any metastore settings, remove them.
        val filteredSettings = super.getAll.filterNot(e => isMetastoreSetting(e._1))

        // Always add these two metastore settings at the beginning.
        ("spark.sql.hive.metastore.version" -> "0.12") +:
        ("spark.sql.hive.metastore.jars" -> "maven") +:
        filteredSettings
      }

      // For this simple test, we do not really clone this object.
      override def clone: SparkConf = this
    }
    conf.set(UI_ENABLED, false)
    val sc = new SparkContext(conf)
    val hiveContext = new TestHiveContext(sc)
    // Run a simple command to make sure all lazy vals in hiveContext get instantiated.
    hiveContext.tables().collect()
    sc.stop()
  }
}

object SPARK_9757 extends QueryTest {
  import org.apache.spark.sql.functions._

  protected var spark: SparkSession = _

  def main(args: Array[String]): Unit = {
    TestUtils.configTestLog4j("INFO")

    val hiveWarehouseLocation = Utils.createTempDir()
    val sparkContext = new SparkContext(
      new SparkConf()
        .set("spark.sql.hive.metastore.version", "0.13.1")
        .set("spark.sql.hive.metastore.jars", "maven")
        .set(UI_ENABLED, false)
        .set("spark.sql.warehouse.dir", hiveWarehouseLocation.toString))

    val hiveContext = new TestHiveContext(sparkContext)
    spark = hiveContext.sparkSession
    import hiveContext.implicits._

    val dir = Utils.createTempDir()
    dir.delete()

    try {
      {
        val df =
          hiveContext
            .range(10)
            .select(('id + 0.1) cast DecimalType(10, 3) as 'dec)
        df.write.option("path", dir.getCanonicalPath).mode("overwrite").saveAsTable("t")
        checkAnswer(hiveContext.table("t"), df)
      }

      {
        val df =
          hiveContext
            .range(10)
            .select(callUDF("struct", ('id + 0.2) cast DecimalType(10, 3)) as 'dec_struct)
        df.write.option("path", dir.getCanonicalPath).mode("overwrite").saveAsTable("t")
        checkAnswer(hiveContext.table("t"), df)
      }
    } finally {
      dir.delete()
      hiveContext.sql("DROP TABLE t")
      sparkContext.stop()
    }
  }
}

object SPARK_11009 extends QueryTest {
  import org.apache.spark.sql.functions._

  protected var spark: SparkSession = _

  def main(args: Array[String]): Unit = {
    TestUtils.configTestLog4j("INFO")

    val sparkContext = new SparkContext(
      new SparkConf()
        .set(UI_ENABLED, false)
        .set("spark.sql.shuffle.partitions", "100"))

    val hiveContext = new TestHiveContext(sparkContext)
    spark = hiveContext.sparkSession

    try {
      val df = spark.range(1 << 20)
      val df2 = df.select((df("id") % 1000).alias("A"), (df("id") / 1000).alias("B"))
      val ws = Window.partitionBy(df2("A")).orderBy(df2("B"))
      val df3 = df2.select(df2("A"), df2("B"), row_number().over(ws).alias("rn")).filter("rn < 0")
      if (df3.rdd.count() != 0) {
        throw new Exception("df3 should have 0 output row.")
      }
    } finally {
      sparkContext.stop()
    }
  }
}

object SPARK_14244 extends QueryTest {
  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions._

  protected var spark: SparkSession = _

  def main(args: Array[String]): Unit = {
    TestUtils.configTestLog4j("INFO")

    val sparkContext = new SparkContext(
      new SparkConf()
        .set(UI_ENABLED, false)
        .set("spark.sql.shuffle.partitions", "100"))

    val hiveContext = new TestHiveContext(sparkContext)
    spark = hiveContext.sparkSession

    import hiveContext.implicits._

    try {
      val window = Window.orderBy('id)
      val df = spark.range(2).select(cume_dist().over(window).as('cdist)).orderBy('cdist)
      checkAnswer(df, Seq(Row(0.5D), Row(1.0D)))
    } finally {
      sparkContext.stop()
    }
  }
}

object SPARK_18360 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config(UI_ENABLED.key, "false")
      .enableHiveSupport().getOrCreate()

    val defaultDbLocation = spark.catalog.getDatabase("default").locationUri
    assert(new Path(defaultDbLocation) == new Path(spark.sharedState.warehousePath))

    val hiveClient =
      spark.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client

    try {
      val tableMeta = CatalogTable(
        identifier = TableIdentifier("test_tbl", Some("default")),
        tableType = CatalogTableType.MANAGED,
        storage = CatalogStorageFormat.empty,
        schema = new StructType().add("i", "int"),
        provider = Some(DDLUtils.HIVE_PROVIDER))

      val newWarehousePath = Utils.createTempDir().getAbsolutePath
      hiveClient.runSqlHive(s"SET hive.metastore.warehouse.dir=$newWarehousePath")
      hiveClient.createTable(tableMeta, ignoreIfExists = false)
      val rawTable = hiveClient.getTable("default", "test_tbl")
      // Hive will use the value of `hive.metastore.warehouse.dir` to generate default table
      // location for tables in default database.
      assert(rawTable.storage.locationUri.map(
        CatalogUtils.URIToString(_)).get.contains(newWarehousePath))
      hiveClient.dropTable("default", "test_tbl", ignoreIfNotExists = false, purge = false)

      spark.sharedState.externalCatalog.createTable(tableMeta, ignoreIfExists = false)
      val readBack = spark.sharedState.externalCatalog.getTable("default", "test_tbl")
      // Spark SQL will use the location of default database to generate default table
      // location for tables in default database.
      assert(readBack.storage.locationUri.map(CatalogUtils.URIToString(_))
        .get.contains(defaultDbLocation))
    } finally {
      hiveClient.dropTable("default", "test_tbl", ignoreIfNotExists = true, purge = false)
      hiveClient.runSqlHive(s"SET hive.metastore.warehouse.dir=$defaultDbLocation")
    }
  }
}

object SPARK_18989_CREATE_TABLE {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS base64_tbl(val string) STORED AS
        |INPUTFORMAT 'org.apache.hadoop.hive.contrib.fileformat.base64.Base64TextInputFormat'
        |OUTPUTFORMAT 'org.apache.hadoop.hive.contrib.fileformat.base64.Base64TextOutputFormat'
      """.stripMargin)
  }
}

object SPARK_18989_DESC_TABLE {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    try {
      spark.sql("DESC base64_tbl")
    } finally {
      spark.sql("DROP TABLE IF EXISTS base64_tbl")
    }
  }
}
