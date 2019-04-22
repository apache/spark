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

package org.apache.spark.sql.execution.datasources.binaryfile

import java.io.File
import java.nio.file.{Files, StandardOpenOption}
import java.sql.Timestamp

import scala.collection.JavaConverters._

import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.fs.{FileSystem, GlobFilter, Path}

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}
import org.apache.spark.util.Utils

class BinaryFileFormatSuite extends QueryTest with SharedSQLContext with SQLTestUtils {

  private var testDir: String = _

  private var fsTestDir: Path = _

  private var fs: FileSystem = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    testDir = Utils.createTempDir().getAbsolutePath
    fsTestDir = new Path(testDir)
    fs = fsTestDir.getFileSystem(sparkContext.hadoopConfiguration)

    val year2014Dir = new File(testDir, "year=2014")
    year2014Dir.mkdir()
    val year2015Dir = new File(testDir, "year=2015")
    year2015Dir.mkdir()

    Files.write(
      new File(year2014Dir, "data.txt").toPath,
      Seq("2014-test").asJava,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )
    Files.write(
      new File(year2014Dir, "data2.bin").toPath,
      "2014-test-bin".getBytes,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )

    Files.write(
      new File(year2015Dir, "bool.csv").toPath,
      Seq("bool", "True", "False", "true").asJava,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )
    Files.write(
      new File(year2015Dir, "data.txt").toPath,
      "2015-test".getBytes,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )
  }

  def testBinaryFileDataSource(pathGlobFilter: String): Unit = {
    val resultDF = spark.read.format("binaryFile")
      .option("pathGlobFilter", pathGlobFilter)
      .load(testDir)
      .select(
        col("status.path"),
        col("status.modificationTime"),
        col("status.length"),
        col("content"),
        col("year") // this is a partition column
      )

    val expectedRowSet = new collection.mutable.HashSet[Row]()

    val globFilter = new GlobFilter(pathGlobFilter)
    for (partitionDirStatus <- fs.listStatus(fsTestDir)) {
      val dirPath = partitionDirStatus.getPath

      val partitionName = dirPath.getName.split("=")(1)
      val year = partitionName.toInt // partition column "year" value which is `Int` type

      for (fileStatus <- fs.listStatus(dirPath)) {
        if (globFilter.accept(fileStatus.getPath)) {
          val fpath = fileStatus.getPath.toString.replace("file:/", "file:///")
          val flen = fileStatus.getLen
          val modificationTime = new Timestamp(fileStatus.getModificationTime)

          val fcontent = {
            val stream = fs.open(fileStatus.getPath)
            val content = try {
              ByteStreams.toByteArray(stream)
            } finally {
              Closeables.close(stream, true)
            }
            content
          }

          val row = Row(fpath, modificationTime, flen, fcontent, year)
          expectedRowSet.add(row)
        }
      }
    }

    checkAnswer(resultDF, expectedRowSet.toSeq)
  }

  test("binary file data source test") {
    testBinaryFileDataSource(pathGlobFilter = "*.*")
    testBinaryFileDataSource(pathGlobFilter = "*.bin")
    testBinaryFileDataSource(pathGlobFilter = "*.txt")
    testBinaryFileDataSource(pathGlobFilter = "*.{txt,csv}")
    testBinaryFileDataSource(pathGlobFilter = "*.json")
  }

  test ("binary file data source do not support write operation") {
    val df = spark.read.format("binaryFile").load(testDir)
    withTempDir { tmpDir =>
      val thrown = intercept[UnsupportedOperationException] {
        df.write
          .format("binaryFile")
          .save(tmpDir + "/test_save")
      }
      assert(thrown.getMessage.contains("Write is not supported for binary file data source"))
    }
  }

}
