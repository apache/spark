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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.sys.process._
import scala.util.control.NonFatal

import org.apache.commons.lang3.{JavaVersion, SystemUtils}
import org.apache.hadoop.conf.Configuration
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkConf, TestUtils}
import org.apache.spark.deploy.SparkSubmitTestUtils
import org.apache.spark.internal.config.MASTER_REST_SERVER_ENABLED
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.launcher.JavaModuleOptions
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.tags.{ExtendedHiveTest, SlowHiveTest}
import org.apache.spark.util.{Utils, VersionUtils}

/**
 * Test HiveExternalCatalog backward compatibility.
 *
 * Note that, this test suite will automatically download spark binary packages of different
 * versions to a local directory. If the `spark.test.cache-dir` system property is defined, this
 * directory will be used. If there is already a spark folder with expected version under this
 * local directory, e.g. `/{cache-dir}/spark-2.0.3`, downloading for this spark version will be
 * skipped. If the system property is not present, a temporary directory will be used and cleaned
 * up after the test.
 */
@SlowHiveTest
@ExtendedHiveTest
class HiveExternalCatalogVersionsSuite extends SparkSubmitTestUtils {
  import HiveExternalCatalogVersionsSuite._
  override protected val defaultSparkSubmitTimeout: Span = 5.minutes
  private val wareHousePath = Utils.createTempDir(namePrefix = "warehouse")
  private val tmpDataDir = Utils.createTempDir(namePrefix = "test-data")
  // For local test, you can set `spark.test.cache-dir` to a static value like `/tmp/test-spark`, to
  // avoid downloading Spark of different versions in each run.
  private val sparkTestingDir = Option(System.getProperty(SPARK_TEST_CACHE_DIR_SYSTEM_PROPERTY))
      .map(new File(_)).getOrElse(Utils.createTempDir(namePrefix = "test-spark"))
  private val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
  val hiveVersion = if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_9)) {
    HiveUtils.builtinHiveVersion
  } else {
    "1.2.1"
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(wareHousePath)
      Utils.deleteRecursively(tmpDataDir)
      // Only delete sparkTestingDir if it wasn't defined to a static location by the system prop
      if (Option(System.getProperty(SPARK_TEST_CACHE_DIR_SYSTEM_PROPERTY)).isEmpty) {
        Utils.deleteRecursively(sparkTestingDir)
      }
    } finally {
      super.afterAll()
    }
  }

  private def tryDownloadSpark(version: String, path: String): Unit = {
    // Try a few mirrors first; fall back to Apache archive
    val mirrors =
      (0 until 2).flatMap { _ =>
        try {
          Some(getStringFromUrl("https://www.apache.org/dyn/closer.lua?preferred=true"))
        } catch {
          // If we can't get a mirror URL, skip it. No retry.
          case _: Exception => None
        }
      }
    val sites =
      mirrors.distinct :+ "https://archive.apache.org/dist" :+ PROCESS_TABLES.releaseMirror
    logInfo(s"Trying to download Spark $version from $sites")
    for (site <- sites) {
      val filename = VersionUtils.majorMinorPatchVersion(version) match {
        case Some((major, _, _)) if major > 3 => s"spark-$version-bin-hadoop3.tgz"
        case Some((3, minor, _)) if minor >= 3 => s"spark-$version-bin-hadoop3.tgz"
        case Some((3, minor, _)) if minor < 3 => s"spark-$version-bin-hadoop3.2.tgz"
        case Some((_, _, _)) => s"spark-$version-bin-hadoop2.7.tgz"
        case None => s"spark-$version-bin-hadoop2.7.tgz"
      }
      val url = s"$site/spark/spark-$version/$filename"
      logInfo(s"Downloading Spark $version from $url")
      try {
        getFileFromUrl(url, path, filename)
        val downloaded = new File(sparkTestingDir, filename).getCanonicalPath
        val targetDir = new File(sparkTestingDir, s"spark-$version").getCanonicalPath

        Seq("mkdir", targetDir).!
        val exitCode = Seq("tar", "-xzf", downloaded, "-C", targetDir, "--strip-components=1",
          "--no-same-owner").!
        Seq("rm", downloaded).!

        // For a corrupted file, `tar` returns non-zero values. However, we also need to check
        // the extracted file because `tar` returns 0 for empty file.
        val sparkSubmit = new File(sparkTestingDir, s"spark-$version/bin/spark-submit")
        if (exitCode == 0 && sparkSubmit.exists()) {
          return
        } else {
          Seq("rm", "-rf", targetDir).!
        }
      } catch {
        case ex: Exception =>
          logWarning(s"Failed to download Spark $version from $url: ${ex.getMessage}")
      }
    }
    fail(s"Unable to download Spark $version")
  }

  private def genDataDir(name: String): String = {
    new File(tmpDataDir, name).getCanonicalPath
  }

  private def getFileFromUrl(urlString: String, targetDir: String, filename: String): Unit = {
    val conf = new SparkConf
    // if the caller passes the name of an existing file, we want doFetchFile to write over it with
    // the contents from the specified url.
    conf.set("spark.files.overwrite", "true")
    val hadoopConf = new Configuration

    val outDir = new File(targetDir)
    if (!outDir.exists()) {
      outDir.mkdirs()
    }

    // propagate exceptions up to the caller of getFileFromUrl
    Utils.doFetchFile(urlString, outDir, filename, conf, hadoopConf)
  }

  private def getStringFromUrl(urlString: String): String = {
    val contentFile = File.createTempFile("string-", ".txt")
    contentFile.deleteOnExit()

    // exceptions will propagate to the caller of getStringFromUrl
    getFileFromUrl(urlString, contentFile.getParent, contentFile.getName)

    val contentPath = Paths.get(contentFile.toURI)
    new String(Files.readAllBytes(contentPath), StandardCharsets.UTF_8)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    val tempPyFile = File.createTempFile("test", ".py")
    // scalastyle:off line.size.limit
    Files.write(tempPyFile.toPath,
      s"""
        |from pyspark.sql import SparkSession
        |import os
        |
        |spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        |version_index = spark.conf.get("spark.sql.test.version.index", None)
        |
        |spark.sql("create table data_source_tbl_{} using json as select 1 i".format(version_index))
        |
        |spark.sql("create table hive_compatible_data_source_tbl_{} using parquet as select 1 i".format(version_index))
        |
        |json_file = "${genDataDir("json_")}" + str(version_index)
        |spark.range(1, 2).selectExpr("cast(id as int) as i").write.json(json_file)
        |spark.sql("create table external_data_source_tbl_{}(i int) using json options (path '{}')".format(version_index, json_file))
        |
        |parquet_file = "${genDataDir("parquet_")}" + str(version_index)
        |spark.range(1, 2).selectExpr("cast(id as int) as i").write.parquet(parquet_file)
        |spark.sql("create table hive_compatible_external_data_source_tbl_{}(i int) using parquet options (path '{}')".format(version_index, parquet_file))
        |
        |json_file2 = "${genDataDir("json2_")}" + str(version_index)
        |spark.range(1, 2).selectExpr("cast(id as int) as i").write.json(json_file2)
        |spark.sql("create table external_table_without_schema_{} using json options (path '{}')".format(version_index, json_file2))
        |
        |parquet_file2 = "${genDataDir("parquet2_")}" + str(version_index)
        |spark.range(1, 3).selectExpr("1 as i", "cast(id as int) as p", "1 as j").write.parquet(os.path.join(parquet_file2, "p=1"))
        |spark.sql("create table tbl_with_col_overlap_{} using parquet options(path '{}')".format(version_index, parquet_file2))
        |
        |spark.sql("create view v_{} as select 1 i".format(version_index))
      """.stripMargin.getBytes("utf8"))
    // scalastyle:on line.size.limit

    if (PROCESS_TABLES.testingVersions.isEmpty) {
      if (PROCESS_TABLES.isPythonVersionAvailable) {
        if (SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_17)) {
          logError("Fail to get the latest Spark versions to test.")
        } else {
          logInfo("Skip tests because old Spark versions don't support Java 21.")
        }
      } else {
        logError(s"Python version <  ${TestUtils.minimumPythonSupportedVersion}, " +
          "the running environment is unavailable.")
      }
    }

    PROCESS_TABLES.testingVersions.zipWithIndex.foreach { case (version, index) =>
      val sparkHome = new File(sparkTestingDir, s"spark-$version")
      if (!sparkHome.exists()) {
        tryDownloadSpark(version, sparkTestingDir.getCanonicalPath)
      }

      // Extract major.minor for testing Spark 3.1.x and 3.0.x with metastore 2.3.9 and Java 11.
      val hiveMetastoreVersion = """^\d+\.\d+""".r.findFirstIn(hiveVersion).get
      val args = Seq(
        "--name", "prepare testing tables",
        "--master", "local[2]",
        "--conf", s"${UI_ENABLED.key}=false",
        "--conf", s"${MASTER_REST_SERVER_ENABLED.key}=false",
        "--conf", s"${HiveUtils.HIVE_METASTORE_VERSION.key}=$hiveMetastoreVersion",
        "--conf", s"${HiveUtils.HIVE_METASTORE_JARS.key}=maven",
        "--conf", s"${WAREHOUSE_PATH.key}=${wareHousePath.getCanonicalPath}",
        "--conf", s"spark.sql.test.version.index=$index",
        "--driver-java-options", s"-Dderby.system.home=${wareHousePath.getCanonicalPath} " +
          // TODO SPARK-37159 Consider to remove the following
          // JVM module options once the Spark 3.2 line is EOL.
          JavaModuleOptions.defaultModuleOptions(),
        tempPyFile.getCanonicalPath)
      runSparkSubmit(args, Some(sparkHome.getCanonicalPath), isSparkTesting = false)
    }

    tempPyFile.delete()
  }

  test("backward compatibility") {
    assume(PROCESS_TABLES.isPythonVersionAvailable)
    val args = Seq(
      "--class", PROCESS_TABLES.getClass.getName.stripSuffix("$"),
      "--name", "HiveExternalCatalog backward compatibility test",
      "--master", "local[2]",
      "--conf", s"${UI_ENABLED.key}=false",
      "--conf", s"${MASTER_REST_SERVER_ENABLED.key}=false",
      "--conf", s"${HiveUtils.HIVE_METASTORE_VERSION.key}=$hiveVersion",
      "--conf", s"${HiveUtils.HIVE_METASTORE_JARS.key}=maven",
      "--conf", s"${WAREHOUSE_PATH.key}=${wareHousePath.getCanonicalPath}",
      "--driver-java-options", s"-Dderby.system.home=${wareHousePath.getCanonicalPath}",
      unusedJar.toString)
    if (PROCESS_TABLES.testingVersions.nonEmpty) runSparkSubmit(args)
  }
}

object PROCESS_TABLES extends QueryTest with SQLTestUtils {
  val isPythonVersionAvailable = TestUtils.isPythonVersionAvailable
  val releaseMirror = sys.env.getOrElse("SPARK_RELEASE_MIRROR",
    "https://dist.apache.org/repos/dist/release")
  // Tests the latest version of every release line if Java version is at most 17.
  val testingVersions: Seq[String] = if (isPythonVersionAvailable &&
      SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_17)) {
    import scala.io.Source
    try Utils.tryWithResource(
      Source.fromURL(s"$releaseMirror/spark")) { source =>
      source.mkString
        .split("\n")
        .filter(_.contains("""<a href="spark-"""))
        .filterNot(_.contains("preview"))
        .map("""<a href="spark-(\d.\d.\d)/">""".r.findFirstMatchIn(_).get.group(1))
        .filter(_ < org.apache.spark.SPARK_VERSION)
    } catch {
      // Do not throw exception during object initialization.
      case NonFatal(_) => Nil
    }
  } else Seq.empty[String]

  protected var spark: SparkSession = _

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()
    spark = session
    import session.implicits._

    testingVersions.indices.foreach { index =>
      Seq(
        s"data_source_tbl_$index",
        s"hive_compatible_data_source_tbl_$index",
        s"external_data_source_tbl_$index",
        s"hive_compatible_external_data_source_tbl_$index",
        s"external_table_without_schema_$index").foreach { tbl =>
        val tableMeta = spark.sharedState.externalCatalog.getTable("default", tbl)

        // make sure we can insert and query these tables.
        session.sql(s"insert into $tbl select 2")
        checkAnswer(session.sql(s"select * from $tbl"), Row(1) :: Row(2) :: Nil)
        checkAnswer(session.sql(s"select i from $tbl where i > 1"), Row(2))

        // make sure we can rename table.
        val newName = tbl + "_renamed"
        sql(s"ALTER TABLE $tbl RENAME TO $newName")
        val readBack = spark.sharedState.externalCatalog.getTable("default", newName)

        val actualTableLocation = readBack.storage.locationUri.get.getPath
        val expectedLocation = if (tableMeta.tableType == CatalogTableType.EXTERNAL) {
          tableMeta.storage.locationUri.get.getPath
        } else {
          spark.sessionState.catalog.defaultTablePath(TableIdentifier(newName, None)).getPath
        }
        assert(actualTableLocation == expectedLocation)

        // make sure we can alter table location.
        withTempDir { dir =>
          val path = dir.toURI.toString.stripSuffix("/")
          sql(s"ALTER TABLE ${tbl}_renamed SET LOCATION '$path'")
          val readBack = spark.sharedState.externalCatalog.getTable("default", tbl + "_renamed")
          val actualTableLocation = readBack.storage.locationUri.get.getPath
          val expected = dir.toURI.getPath.stripSuffix("/")
          assert(actualTableLocation == expected)
        }
      }

      // test permanent view
      checkAnswer(sql(s"select i from v_$index"), Row(1))

      // SPARK-22356: overlapped columns between data and partition schema in data source tables
      val tbl_with_col_overlap = s"tbl_with_col_overlap_$index"
      assert(spark.table(tbl_with_col_overlap).columns === Array("i", "p", "j"))
      checkAnswer(spark.table(tbl_with_col_overlap), Row(1, 1, 1) :: Row(1, 1, 1) :: Nil)
      assert(sql("desc " + tbl_with_col_overlap).select("col_name")
        .as[String].collect().mkString(",").contains("i,p,j"))
    }
  }
}

object HiveExternalCatalogVersionsSuite {
  private val SPARK_TEST_CACHE_DIR_SYSTEM_PROPERTY = "spark.test.cache-dir"
}

