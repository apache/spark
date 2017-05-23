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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class ParquetFileFormatSuite extends QueryTest with ParquetTest with SharedSQLContext {

  test("Number of threads doesn't grow extremely after parquet file reading") {
    withTempDir { dir =>
      val file = dir.toString + "/file"
      spark.range(1).toDF("a").coalesce(1).write.parquet(file)
      spark.read.parquet(file)
      val numThreadBefore = Thread.activeCount
      (1 to 100).map { _ =>
        spark.read.parquet(file)
      }
      val numThreadAfter = Thread.activeCount
      // Hard to test a correct thread number,
      // but it shouldn't increase more than a reasonable number.
      assert(numThreadAfter - numThreadBefore < 20)
    }
  }

  test("read parquet footers in parallel") {
    def testReadFooters(ignoreCorruptFiles: Boolean): Unit = {
      withTempDir { dir =>
        val fs = FileSystem.get(sparkContext.hadoopConfiguration)
        val basePath = dir.getCanonicalPath

        val path1 = new Path(basePath, "first")
        val path2 = new Path(basePath, "second")
        val path3 = new Path(basePath, "third")

        spark.range(1).toDF("a").coalesce(1).write.parquet(path1.toString)
        spark.range(1, 2).toDF("a").coalesce(1).write.parquet(path2.toString)
        spark.range(2, 3).toDF("a").coalesce(1).write.json(path3.toString)

        val fileStatuses =
          Seq(fs.listStatus(path1), fs.listStatus(path2), fs.listStatus(path3)).flatten

        val footers = ParquetFileFormat.readParquetFootersInParallel(
          sparkContext.hadoopConfiguration, fileStatuses, ignoreCorruptFiles)

        assert(footers.size == 2)
      }
    }

    testReadFooters(true)
    val exception = intercept[java.io.IOException] {
      testReadFooters(false)
    }
    assert(exception.getMessage().contains("Could not read footer for file"))
  }
}
