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
package org.apache.spark.sql.execution

import java.io.File

import scala.util.Random

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite base for testing the redaction of DataSourceScanExec/BatchScanExec.
 */
abstract class DataSourceScanRedactionTest extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.redaction.string.regex", "file:/[^\\]\\s]+")

  final protected def isIncluded(queryExecution: QueryExecution, msg: String): Boolean = {
    queryExecution.toString.contains(msg) ||
      queryExecution.simpleString.contains(msg) ||
      queryExecution.stringWithStats.contains(msg)
  }

  protected def getRootPath(df: DataFrame): Path

  test("treeString is redacted") {
    withTempDir { dir =>
      val basePath = dir.getCanonicalPath
      spark.range(0, 10).toDF("a").write.orc(new Path(basePath, "foo=1").toString)
      val df = spark.read.orc(basePath)

      val rootPath = getRootPath(df)
      assert(rootPath.toString.contains(dir.toURI.getPath.stripSuffix("/")))

      assert(!df.queryExecution.sparkPlan.treeString(verbose = true).contains(rootPath.getName))
      assert(!df.queryExecution.executedPlan.treeString(verbose = true).contains(rootPath.getName))
      assert(!df.queryExecution.toString.contains(rootPath.getName))
      assert(!df.queryExecution.simpleString.contains(rootPath.getName))

      val replacement = "*********"
      assert(df.queryExecution.sparkPlan.treeString(verbose = true).contains(replacement))
      assert(df.queryExecution.executedPlan.treeString(verbose = true).contains(replacement))
      assert(df.queryExecution.toString.contains(replacement))
      assert(df.queryExecution.simpleString.contains(replacement))
    }
  }
}

/**
 * Suite that tests the redaction of DataSourceScanExec
 */
class DataSourceScanExecRedactionSuite extends DataSourceScanRedactionTest {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.USE_V1_SOURCE_LIST.key, "orc")

  override protected def getRootPath(df: DataFrame): Path =
    df.queryExecution.sparkPlan.find(_.isInstanceOf[FileSourceScanExec]).get
      .asInstanceOf[FileSourceScanExec].relation.location.rootPaths.head

  test("explain is redacted using SQLConf") {
    withTempDir { dir =>
      val basePath = dir.getCanonicalPath
      spark.range(0, 10).toDF("a").write.orc(new Path(basePath, "foo=1").toString)
      val df = spark.read.orc(basePath)
      val replacement = "*********"

      // Respect SparkConf and replace file:/
      assert(isIncluded(df.queryExecution, replacement))

      assert(isIncluded(df.queryExecution, "FileScan"))
      assert(!isIncluded(df.queryExecution, "file:/"))

      withSQLConf(SQLConf.SQL_STRING_REDACTION_PATTERN.key -> "(?i)FileScan") {
        // Respect SQLConf and replace FileScan
        assert(isIncluded(df.queryExecution, replacement))

        assert(!isIncluded(df.queryExecution, "FileScan"))
        assert(isIncluded(df.queryExecution, "file:/"))
      }
    }
  }

  test("FileSourceScanExec metadata") {
    withTempPath { path =>
      val dir = path.getCanonicalPath
      spark.range(0, 10).write.orc(dir)
      val df = spark.read.orc(dir)

      assert(isIncluded(df.queryExecution, "Format"))
      assert(isIncluded(df.queryExecution, "ReadSchema"))
      assert(isIncluded(df.queryExecution, "Batched"))
      assert(isIncluded(df.queryExecution, "PartitionFilters"))
      assert(isIncluded(df.queryExecution, "PushedFilters"))
      assert(isIncluded(df.queryExecution, "DataFilters"))
      assert(isIncluded(df.queryExecution, "Location"))
    }
  }

  test("SPARK-31793: FileSourceScanExec metadata should contain limited file paths") {
    withTempPath { path =>
      // create a sub-directory with long name so that each root path will always exceed the limit
      // this is to ensure we always test the case for the path truncation
      val dataDirName = Random.alphanumeric.take(100).toList.mkString
      val dataDir = new File(path, dataDirName)
      dataDir.mkdir()

      val partitionCol = "partitionCol"
      spark.range(10)
        .select("id", "id")
        .toDF("value", partitionCol)
        .write
        .partitionBy(partitionCol)
        .orc(dataDir.getCanonicalPath)
      val paths = (0 to 9).map(i => new File(dataDir, s"$partitionCol=$i").getCanonicalPath)
      val plan = spark.read.orc(paths: _*).queryExecution.executedPlan
      val location = plan collectFirst {
        case f: FileSourceScanExec => f.metadata("Location")
      }
      assert(location.isDefined)
      // The location metadata should at least contain one path
      assert(location.get.contains(paths.head))

      // The location metadata should have the number of root paths
      assert(location.get.contains("(10 paths)"))

      // The location metadata should have bracket wrapping paths
      assert(location.get.indexOf('[') > -1)
      assert(location.get.indexOf(']') > -1)

      // extract paths in location metadata (removing classname, brackets, separators)
      val pathsInLocation = location.get.substring(
        location.get.indexOf('[') + 1, location.get.indexOf(']')).split(", ").toSeq

      // the only one path should be available
      assert(pathsInLocation.size == 2)
      // indicator ("...") should be available
      assert(pathsInLocation.exists(_.contains("...")))
    }
  }
}

/**
 * Suite that tests the redaction of BatchScanExec.
 */
class DataSourceV2ScanExecRedactionSuite extends DataSourceScanRedactionTest {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.USE_V1_SOURCE_LIST.key, "")

  override protected def getRootPath(df: DataFrame): Path =
    df.queryExecution.sparkPlan.find(_.isInstanceOf[BatchScanExec]).get
      .asInstanceOf[BatchScanExec].scan.asInstanceOf[OrcScan].fileIndex.rootPaths.head

  test("explain is redacted using SQLConf") {
    withTempDir { dir =>
      val basePath = dir.getCanonicalPath
      spark.range(0, 10).toDF("a").write.orc(new Path(basePath, "foo=1").toString)
      val df = spark.read.orc(basePath)
      val replacement = "*********"

      // Respect SparkConf and replace file:/
      assert(isIncluded(df.queryExecution, replacement))
      assert(isIncluded(df.queryExecution, "BatchScan"))
      assert(!isIncluded(df.queryExecution, "file:/"))

      withSQLConf(SQLConf.SQL_STRING_REDACTION_PATTERN.key -> "(?i)BatchScan") {
        // Respect SQLConf and replace FileScan
        assert(isIncluded(df.queryExecution, replacement))

        assert(!isIncluded(df.queryExecution, "BatchScan"))
        assert(isIncluded(df.queryExecution, "file:/"))
      }
    }
  }

  test("FileScan description") {
    Seq("json", "orc", "parquet").foreach { format =>
      withTempPath { path =>
        val dir = path.getCanonicalPath
        spark.range(0, 10).write.format(format).save(dir)
        val df = spark.read.format(format).load(dir)

        withClue(s"Source '$format':") {
          assert(isIncluded(df.queryExecution, "ReadSchema"))
          assert(isIncluded(df.queryExecution, "BatchScan"))
          if (Seq("orc", "parquet").contains(format)) {
            assert(isIncluded(df.queryExecution, "PushedFilters"))
          }
          assert(isIncluded(df.queryExecution, "Location"))
        }
      }
    }
  }
}
