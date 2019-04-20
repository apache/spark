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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}
import org.apache.spark.sql.types.StructType
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

    val file1 = new File(year2014Dir, "data.txt")
    Files.write(
      file1.toPath,
      Seq("2014-test").asJava, // file length = 10
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )

    val file2 = new File(year2014Dir, "data2.bin")
    Files.write(
      file2.toPath,
      "2014-test-bin".getBytes, // file length = 13
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )

    val file3 = new File(year2015Dir, "bool.csv")
    Files.write(
      file3.toPath,
      Seq("bool", "True", "False", "true").asJava, // file length = 21
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )

    val file4 = new File(year2015Dir, "data.bin")
    Files.write(
      file4.toPath,
      "2015-test".getBytes, // file length = 9
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )
  }

  def testBinaryFileDataSource(pathGlobFilter: String): Unit = {
    val dfReader = spark.read.format("binaryFile")
    if (pathGlobFilter != null) {
      dfReader.option("pathGlobFilter", pathGlobFilter)
    }
    val resultDF = dfReader.load(testDir).select(
        col("path"),
        col("modificationTime"),
        col("length"),
        col("content"),
        col("year") // this is a partition column
      )

    val expectedRowSet = new collection.mutable.HashSet[Row]()

    val globFilter = if (pathGlobFilter == null) null else new GlobFilter(pathGlobFilter)
    for (partitionDirStatus <- fs.listStatus(fsTestDir)) {
      val dirPath = partitionDirStatus.getPath

      val partitionName = dirPath.getName.split("=")(1)
      val year = partitionName.toInt // partition column "year" value which is `Int` type

      for (fileStatus <- fs.listStatus(dirPath)) {
        if (globFilter == null || globFilter.accept(fileStatus.getPath)) {
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
    testBinaryFileDataSource(null)
    testBinaryFileDataSource("*.*")
    testBinaryFileDataSource("*.bin")
    testBinaryFileDataSource("*.txt")
    testBinaryFileDataSource("*.{txt,csv}")
    testBinaryFileDataSource("*.json")
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

  def genTestFile(path: String, length: Long, modificationTime: Long): Unit = {
    val file = new File(path)
    val bytes = Array.fill[Byte](length.toInt)(0)
    Files.write(
      file.toPath,
      bytes,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )
    file.setLastModified(modificationTime)
  }

  /**
   * @param filters filters to be tested.
   * @param testcases Each testcase is a Tuple (length, modificationTime, expectedPassFilter)
   *                  expectedPassFilter is a boolean value represent whether the file length
   *                  and modification satisfy the filters condition.
   */
  def testBuildReaderWithFilters(
      filters: Seq[Filter],
      testcases: Seq[(Long, Long, Boolean)]): Unit = {
    withTempDir { tmpDir =>
      val binaryFileFormat = new BinaryFileFormat

      val reader = binaryFileFormat.buildReaderWithPartitionValues(
        sparkSession = spark,
        dataSchema = BinaryFileFormat.schema,
        partitionSchema = StructType(Nil),
        requiredSchema = BinaryFileFormat.schema,
        filters = filters,
        options = Map[String, String](),
        hadoopConf = spark.sessionState.newHadoopConf()
      )

      var i = 0
      testcases.foreach { case (length, modTime, expectedPassFilter) =>
        i += 1
        val path = new File(tmpDir, i.toString).getAbsolutePath
        genTestFile(path, length, modTime)
        val partitionedFile = new PartitionedFile(
          partitionValues = InternalRow.empty,
          filePath = path,
          start = 0L,
          length = length
        )
        val passFilter = reader.apply(partitionedFile).size == 1

        assert(passFilter === expectedPassFilter)
      }
    }
  }

  test ("test buildReader with filters") {

    // test filter applied on `length` column
    testBuildReaderWithFilters(Seq(LessThan("length", 13L)),
      Seq((10L, 0L, true), (13L, 0L, false), (15L, 0L, false)))
    testBuildReaderWithFilters(Seq(LessThanOrEqual("length", 13L)),
      Seq((10L, 0L, true), (13L, 0L, true), (15L, 0L, false)))
    testBuildReaderWithFilters(Seq(GreaterThan("length", 13L)),
      Seq((10L, 0L, false), (13L, 0L, false), (15L, 0L, true)))
    testBuildReaderWithFilters(Seq(GreaterThanOrEqual("length", 13L)),
      Seq((10L, 0L, false), (13L, 0L, true), (15L, 0L, true)))
    testBuildReaderWithFilters(Seq(EqualTo("length", 13L)),
      Seq((10L, 0L, false), (13L, 0L, true), (15L, 0L, false)))

    // test filter applied on `modificationTime` column
    val t1 = 50000000L
    val t2 = 60000000L
    val t3 = 70000000L
    testBuildReaderWithFilters(Seq(LessThan("modificationTime", new Timestamp(t2))),
      Seq((0L, t1, true), (0L, t2, false), (0L, t3, false)))
    testBuildReaderWithFilters(Seq(LessThanOrEqual("modificationTime", new Timestamp(t2))),
      Seq((0L, t1, true), (0L, t2, true), (0L, t3, false)))
    testBuildReaderWithFilters(Seq(GreaterThan("modificationTime", new Timestamp(t2))),
      Seq((0L, t1, false), (0L, t2, false), (0L, t3, true)))
    testBuildReaderWithFilters(Seq(GreaterThanOrEqual("modificationTime", new Timestamp(t2))),
      Seq((0L, t1, false), (0L, t2, true), (0L, t3, true)))
    testBuildReaderWithFilters(Seq(EqualTo("modificationTime", new Timestamp(t2))),
      Seq((0L, t1, false), (0L, t2, true), (0L, t3, false)))

    // test AND filter
    testBuildReaderWithFilters(Seq(
      And(
        GreaterThan("length", 10L),
        LessThan("length", 20L)
      )),
      Seq((5L, 0L, false), (15L, 0L, true), (25L, 0L, false))
    )
    // test OR filter
    testBuildReaderWithFilters(Seq(
      Or(
        GreaterThan("length", 20L),
        LessThan("length", 10L)
      )),
      Seq((5L, 0L, true), (15L, 0L, false), (25L, 0L, true))
    )
    // test NOT filter
    testBuildReaderWithFilters(Seq(
      Not(
        EqualTo("length", 13L)
      )),
      Seq((10L, 0L, true), (13L, 0L, false), (15L, 0L, true)))
    // test 2 layer nested filter
    testBuildReaderWithFilters(Seq(
      Not(
        Or(
          GreaterThan("length", 20L),
          LessThan("length", 10L)
        )
      )),
      Seq((5L, 0L, false), (15L, 0L, true), (25L, 0L, false))
    )
    // test multiple filters
    testBuildReaderWithFilters(Seq(
        EqualTo("length", 13L),
        EqualTo("modificationTime", new Timestamp(t2))
      ),
      Seq((13L, t2, true), (10L, t2, false), (13L, t1, false), (10L, t1, false))
    )
  }
}
