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
import org.apache.hadoop.fs.{FileStatus, FileSystem, GlobFilter, Path}

import org.apache.spark.sql.{Column, QueryTest, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.sources.{And, Filter, GreaterThan, GreaterThanOrEqual,
  LessThan, LessThanOrEqual}
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}
import org.apache.spark.util.Utils

class BinaryFileFormatSuite extends QueryTest with SharedSQLContext with SQLTestUtils {

  private var testDir: String = _

  private var fsTestDir: Path = _

  private var fs: FileSystem = _

  private var file1Status: FileStatus = _
  private var file2Status: FileStatus = _
  private var file3Status: FileStatus = _
  private var file4Status: FileStatus = _
  private var fileStatusSet: Set[FileStatus] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    testDir = Utils.createTempDir().getAbsolutePath
    fsTestDir = new Path(testDir)
    fs = fsTestDir.getFileSystem(sparkContext.hadoopConfiguration)

    val year2014Dir = new File(testDir, "year=2014")
    year2014Dir.mkdir()
    val year2015Dir = new File(testDir, "year=2015")
    year2015Dir.mkdir()

    val file1 = new File(year2014Dir, "data.txt")
    Files.write(
      file1.toPath,
      Seq("2014-test").asJava, // file length = 10
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )
    file1Status = fs.getFileStatus(new Path(file1.getAbsolutePath))

    val file2 = new File(year2014Dir, "data2.bin")
    Files.write(
      file2.toPath,
      "2014-test-bin".getBytes, // file length = 13
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )
    file2Status = fs.getFileStatus(new Path(file2.getAbsolutePath))

    // sleep 1s to make the gen file modificationTime different,
    // for unit-test for push down filters on modificationTime column.
    Thread.sleep(1000)

    val file3 = new File(year2015Dir, "bool.csv")
    Files.write(
      file3.toPath,
      Seq("bool", "True", "False", "true").asJava, // file length = 21
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )
    file3Status = fs.getFileStatus(new Path(file3.getAbsolutePath))

    val file4 = new File(year2015Dir, "data.bin")
    Files.write(
      file4.toPath,
      "2015-test".getBytes, // file length = 9
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )
    file4Status = fs.getFileStatus(new Path(file4.getAbsolutePath))

    fileStatusSet = Set(file1Status, file2Status, file3Status, file4Status)
  }

  def testBinaryFileDataSource(
      pathGlobFilter: String,
      sqlFilter: Column,
      expectedFilter: FileStatus => Boolean): Unit = {
    val dfReader = spark.read.format("binaryFile")
    if (pathGlobFilter != null) {
      dfReader.option("pathGlobFilter", pathGlobFilter)
    }
    var resultDF = dfReader.load(testDir).select(
        col("path"),
        col("modificationTime"),
        col("length"),
        col("content"),
        col("year") // this is a partition column
      )
    if (sqlFilter != null) {
      resultDF = resultDF.filter(sqlFilter)
    }

    val expectedRowSet = new collection.mutable.HashSet[Row]()

    val globFilter = if (pathGlobFilter == null) null else new GlobFilter(pathGlobFilter)
    for (partitionDirStatus <- fs.listStatus(fsTestDir)) {
      val dirPath = partitionDirStatus.getPath

      val partitionName = dirPath.getName.split("=")(1)
      val year = partitionName.toInt // partition column "year" value which is `Int` type

      for (fileStatus <- fs.listStatus(dirPath)) {
        if ((globFilter == null || globFilter.accept(fileStatus.getPath))
          && (expectedFilter == null || expectedFilter(fileStatus))) {
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
    testBinaryFileDataSource(null, null, null)
    testBinaryFileDataSource("*.*", null, null)
    testBinaryFileDataSource("*.bin", null, null)
    testBinaryFileDataSource("*.txt", null, null)
    testBinaryFileDataSource("*.{txt,csv}", null, null)
    testBinaryFileDataSource("*.json", null, null)

    testBinaryFileDataSource(null, col("length") > 10, _.getLen > 10)
    testBinaryFileDataSource(null, col("length") >= 10, _.getLen >= 10)
    testBinaryFileDataSource(null, col("length") < 13, _.getLen < 13)
    testBinaryFileDataSource(null, col("length") <= 13, _.getLen <= 13)

    testBinaryFileDataSource(null,
      col("modificationTime") > new Timestamp(file2Status.getModificationTime),
      _.getModificationTime > file2Status.getModificationTime)
    testBinaryFileDataSource(null,
      col("modificationTime") >= new Timestamp(file2Status.getModificationTime),
      _.getModificationTime >= file2Status.getModificationTime)
    testBinaryFileDataSource(null,
      col("modificationTime") < new Timestamp(file3Status.getModificationTime),
      _.getModificationTime < file3Status.getModificationTime)
    testBinaryFileDataSource(null,
      col("modificationTime") <= new Timestamp(file3Status.getModificationTime),
      _.getModificationTime <= file3Status.getModificationTime)

    testBinaryFileDataSource(null,
      col("length") >= 10 && col("length") <= 13,
      s => s.getLen >= 10 && s.getLen <= 13)
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


  def testCreateFilterFunctions(
      sourceFilter: Filter,
      expectedFilterFunc: FileStatus => Boolean): Unit = {
    val filterFuncList = BinaryFileFormat.createFilterFunctions(sourceFilter)
    val result = fileStatusSet.filter(fileStatus => filterFuncList.forall(_.apply(fileStatus)))
    val expectedResult = fileStatusSet.filter(expectedFilterFunc.apply(_))
    assert(result === expectedResult)
  }

  test("test createFilterFunctions") {
    // file1 length = 10
    // file2 length = 13
    // file3 length = 21
    // file4 length = 9
    testCreateFilterFunctions(LessThan("length", 13L), _.getLen < 13L)
    testCreateFilterFunctions(LessThanOrEqual("length", 13L), _.getLen <= 13L)
    testCreateFilterFunctions(GreaterThan("length", 10L), _.getLen > 10L)
    testCreateFilterFunctions(GreaterThanOrEqual("length", 10L), _.getLen >= 10L)

    // file modificationTime: file1 < file2 < file3 < file4
    testCreateFilterFunctions(
      LessThan("modificationTime", new Timestamp(file3Status.getModificationTime)),
      _.getModificationTime < file3Status.getModificationTime)
    testCreateFilterFunctions(
      LessThanOrEqual("modificationTime", new Timestamp(file3Status.getModificationTime)),
      _.getModificationTime <= file3Status.getModificationTime)
    testCreateFilterFunctions(
      GreaterThan("modificationTime", new Timestamp(file2Status.getModificationTime)),
      _.getModificationTime > file2Status.getModificationTime)
    testCreateFilterFunctions(
      GreaterThanOrEqual("modificationTime", new Timestamp(file2Status.getModificationTime)),
      _.getModificationTime >= file2Status.getModificationTime)

    // test AND filter
    testCreateFilterFunctions(
      And(GreaterThan("length", 9L), LessThanOrEqual("length", 13L)),
      s => s.getLen > 9L && s.getLen <= 13L
    )
  }

}
