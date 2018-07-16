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
import java.nio.file.Files

import scala.sys.process._

import org.apache.spark.TestUtils
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils

/**
 * Test HiveExternalCatalog backward compatibility.
 *
 * Note that, this test suite will automatically download spark binary packages of different
 * versions to a local directory `/tmp/spark-test`. If there is already a spark folder with
 * expected version under this local directory, e.g. `/tmp/spark-test/spark-2.0.3`, we will skip the
 * downloading for this spark version.
 */
class HiveExternalCatalogVersionsSuite extends SparkSubmitTestUtils {
  private val wareHousePath = Utils.createTempDir(namePrefix = "warehouse")
  private val tmpDataDir = Utils.createTempDir(namePrefix = "test-data")
  // For local test, you can set `sparkTestingDir` to a static value like `/tmp/test-spark`, to
  // avoid downloading Spark of different versions in each run.
  private val sparkTestingDir = new File("/tmp/test-spark")
  private val unusedJar = TestUtils.createJarWithClasses(Seq.empty)

  override def afterAll(): Unit = {
    Utils.deleteRecursively(wareHousePath)
    Utils.deleteRecursively(tmpDataDir)
    Utils.deleteRecursively(sparkTestingDir)
    super.afterAll()
  }

  private def tryDownloadSpark(version: String, path: String): Unit = {
    // Try a few mirrors first; fall back to Apache archive
    val mirrors =
      (0 until 2).flatMap { _ =>
        try {
          Some(Seq("wget",
            "https://www.apache.org/dyn/closer.lua?preferred=true", "-q", "-O", "-").!!.trim)
        } catch {
          // If we can't get a mirror URL, skip it. No retry.
          case _: Exception => None
        }
      }
    val sites = mirrors.distinct :+ "https://archive.apache.org/dist"
    logInfo(s"Trying to download Spark $version from $sites")
    for (site <- sites) {
      val filename = s"spark-$version-bin-hadoop2.7.tgz"
      val url = s"$site/spark/spark-$version/$filename"
      logInfo(s"Downloading Spark $version from $url")
      if (Seq("wget", url, "-q", "-P", path).! == 0) {
        val downloaded = new File(sparkTestingDir, filename).getCanonicalPath
        val targetDir = new File(sparkTestingDir, s"spark-$version").getCanonicalPath

        Seq("mkdir", targetDir).!
        val exitCode = Seq("tar", "-xzf", downloaded, "-C", targetDir, "--strip-components=1").!
        Seq("rm", downloaded).!

        // For a corrupted file, `tar` returns non-zero values. However, we also need to check
        // the extracted file because `tar` returns 0 for empty file.
        val sparkSubmit = new File(sparkTestingDir, s"spark-$version/bin/spark-submit")
        if (exitCode == 0 && sparkSubmit.exists()) {
          return
        } else {
          Seq("rm", "-rf", targetDir).!
        }
      }
      logWarning(s"Failed to download Spark $version from $url")
    }
    fail(s"Unable to download Spark $version")
  }

  private def genDataDir(name: String): String = {
    new File(tmpDataDir, name).getCanonicalPath
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

    PROCESS_TABLES.testingVersions.zipWithIndex.foreach { case (version, index) =>
      val sparkHome = new File(sparkTestingDir, s"spark-$version")
      if (!sparkHome.exists()) {
        tryDownloadSpark(version, sparkTestingDir.getCanonicalPath)
      }

      val args = Seq(
        "--name", "prepare testing tables",
        "--master", "local[2]",
        "--conf", "spark.ui.enabled=false",
        "--conf", "spark.master.rest.enabled=false",
        "--conf", s"spark.sql.warehouse.dir=${wareHousePath.getCanonicalPath}",
        "--conf", s"spark.sql.test.version.index=$index",
        "--driver-java-options", s"-Dderby.system.home=${wareHousePath.getCanonicalPath}",
        tempPyFile.getCanonicalPath)
      runSparkSubmit(args, Some(sparkHome.getCanonicalPath))
    }

    tempPyFile.delete()
  }

  test("backward compatibility") {
    val args = Seq(
      "--class", PROCESS_TABLES.getClass.getName.stripSuffix("$"),
      "--name", "HiveExternalCatalog backward compatibility test",
      "--master", "local[2]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--conf", s"spark.sql.warehouse.dir=${wareHousePath.getCanonicalPath}",
      "--driver-java-options", s"-Dderby.system.home=${wareHousePath.getCanonicalPath}",
      unusedJar.toString)
    runSparkSubmit(args)
  }
}

object PROCESS_TABLES extends QueryTest with SQLTestUtils {
  // Tests the latest version of every release line.
  val testingVersions = Seq("2.0.2", "2.1.3", "2.2.2")

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
      // For Spark 2.2.0 and 2.1.x, the behavior is different from Spark 2.0.
      if (testingVersions(index).startsWith("2.1") || testingVersions(index) == "2.2.0") {
        spark.sql("msck repair table " + tbl_with_col_overlap)
        assert(spark.table(tbl_with_col_overlap).columns === Array("i", "j", "p"))
        checkAnswer(spark.table(tbl_with_col_overlap), Row(1, 1, 1) :: Row(1, 1, 1) :: Nil)
        assert(sql("desc " + tbl_with_col_overlap).select("col_name")
          .as[String].collect().mkString(",").contains("i,j,p"))
      } else {
        assert(spark.table(tbl_with_col_overlap).columns === Array("i", "p", "j"))
        checkAnswer(spark.table(tbl_with_col_overlap), Row(1, 1, 1) :: Row(1, 1, 1) :: Nil)
        assert(sql("desc " + tbl_with_col_overlap).select("col_name")
          .as[String].collect().mkString(",").contains("i,p,j"))
      }
    }
  }
}
