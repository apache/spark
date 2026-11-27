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

import java.io.{ByteArrayInputStream, File, InputStream, IOException}
import java.lang.management.ManagementFactory
import java.nio.file.{Files, StandardOpenOption}
import java.sql.Timestamp

import scala.jdk.CollectionConverters._

import com.google.common.io.Closeables
import com.sun.management.ThreadMXBean
import org.apache.hadoop.fs.{FileStatus, FileSystem, GlobFilter, Path}
import org.mockito.Mockito.{mock, when}

import org.apache.spark.{DebugFilesystem, SparkException, SparkUnsupportedOperationException}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.SOURCES_BINARY_FILE_MAX_LENGTH
import org.apache.spark.sql.sources._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class BinaryFileFormatSuite extends SharedSparkSession {
  import BinaryFileFormat._

  private var testDir: String = _

  private var fsTestDir: Path = _

  private var fs: FileSystem = _

  private var file1Status: FileStatus = _

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
      Seq("2014-test").asJava,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )
    file1Status = fs.getFileStatus(new Path(file1.getPath))

    val file2 = new File(year2014Dir, "data2.bin")
    Files.write(
      file2.toPath,
      "2014-test-bin".getBytes,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )

    val file3 = new File(year2015Dir, "bool.csv")
    Files.write(
      file3.toPath,
      Seq("bool", "True", "False", "true").asJava,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )

    val file4 = new File(year2015Dir, "data.bin")
    Files.write(
      file4.toPath,
      "2015-test".getBytes,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )
  }

  test("BinaryFileFormat methods") {
    val format = new BinaryFileFormat
    assert(format.shortName() === "binaryFile")
    assert(format.isSplitable(spark, Map.empty, new Path("any")) === false)
    assert(format.inferSchema(spark, Map.empty, Seq.empty) === Some(BinaryFileFormat.schema))
    assert(BinaryFileFormat.schema === StructType(Seq(
      StructField("path", StringType, false),
      StructField("modificationTime", TimestampType, false),
      StructField("length", LongType, false),
      StructField("content", BinaryType, true))))
  }

  def testBinaryFileDataSource(pathGlobFilter: String): Unit = {
    val dfReader = spark.read.format(BINARY_FILE)
    if (pathGlobFilter != null) {
      dfReader.option("pathGlobFilter", pathGlobFilter)
    }
    val resultDF = dfReader.load(testDir).select(
        col(PATH),
        col(MODIFICATION_TIME),
        col(LENGTH),
        col(CONTENT),
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
          val fpath = fileStatus.getPath.toString
          val flen = fileStatus.getLen
          val modificationTime = new Timestamp(fileStatus.getModificationTime)

          val fcontent = {
            val stream = fs.open(fileStatus.getPath)
            val content = try {
              stream.readAllBytes()
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

  test("binary file data source do not support write operation") {
    val df = spark.read.format(BINARY_FILE).load(testDir)
    withTempDir { tmpDir =>
      checkError(
        exception = intercept[SparkUnsupportedOperationException] {
          df.write
            .format(BINARY_FILE)
            .save(s"$tmpDir/test_save")
        },
        condition = "UNSUPPORTED_FEATURE.WRITE_FOR_BINARY_SOURCE",
        parameters = Map.empty)
    }
  }

  def mockFileStatus(length: Long, modificationTime: Long): FileStatus = {
    val status = mock(classOf[FileStatus])
    when(status.getLen).thenReturn(length)
    when(status.getModificationTime).thenReturn(modificationTime)
    when(status.toString).thenReturn(
      s"FileStatus($LENGTH=$length, $MODIFICATION_TIME=$modificationTime)")
    status
  }

  def testCreateFilterFunction(
      filters: Seq[Filter],
      testCases: Seq[(FileStatus, Boolean)]): Unit = {
    val funcs = filters.flatMap(BinaryFileFormat.createFilterFunction)
    testCases.foreach { case (status, expected) =>
      assert(funcs.forall(f => f(status)) === expected,
        s"$filters applied to $status should be $expected.")
    }
  }

  test("createFilterFunction") {
    // test filter applied on `length` column
    val l1 = mockFileStatus(1L, 0L)
    val l2 = mockFileStatus(2L, 0L)
    val l3 = mockFileStatus(3L, 0L)
    testCreateFilterFunction(
      Seq(LessThan(LENGTH, 2L)),
      Seq((l1, true), (l2, false), (l3, false)))
    testCreateFilterFunction(
      Seq(LessThanOrEqual(LENGTH, 2L)),
      Seq((l1, true), (l2, true), (l3, false)))
    testCreateFilterFunction(
      Seq(GreaterThan(LENGTH, 2L)),
      Seq((l1, false), (l2, false), (l3, true)))
    testCreateFilterFunction(
      Seq(GreaterThanOrEqual(LENGTH, 2L)),
      Seq((l1, false), (l2, true), (l3, true)))
    testCreateFilterFunction(
      Seq(EqualTo(LENGTH, 2L)),
      Seq((l1, false), (l2, true), (l3, false)))
    testCreateFilterFunction(
      Seq(Not(EqualTo(LENGTH, 2L))),
      Seq((l1, true), (l2, false), (l3, true)))
    testCreateFilterFunction(
      Seq(And(GreaterThan(LENGTH, 1L), LessThan(LENGTH, 3L))),
      Seq((l1, false), (l2, true), (l3, false)))
    testCreateFilterFunction(
      Seq(Or(LessThanOrEqual(LENGTH, 1L), GreaterThanOrEqual(LENGTH, 3L))),
      Seq((l1, true), (l2, false), (l3, true)))

    // test filter applied on `modificationTime` column
    val t1 = mockFileStatus(0L, 1L)
    val t2 = mockFileStatus(0L, 2L)
    val t3 = mockFileStatus(0L, 3L)
    testCreateFilterFunction(
      Seq(LessThan(MODIFICATION_TIME, new Timestamp(2L))),
      Seq((t1, true), (t2, false), (t3, false)))
    testCreateFilterFunction(
      Seq(LessThanOrEqual(MODIFICATION_TIME, new Timestamp(2L))),
      Seq((t1, true), (t2, true), (t3, false)))
    testCreateFilterFunction(
      Seq(GreaterThan(MODIFICATION_TIME, new Timestamp(2L))),
      Seq((t1, false), (t2, false), (t3, true)))
    testCreateFilterFunction(
      Seq(GreaterThanOrEqual(MODIFICATION_TIME, new Timestamp(2L))),
      Seq((t1, false), (t2, true), (t3, true)))
    testCreateFilterFunction(
      Seq(EqualTo(MODIFICATION_TIME, new Timestamp(2L))),
      Seq((t1, false), (t2, true), (t3, false)))
    testCreateFilterFunction(
      Seq(Not(EqualTo(MODIFICATION_TIME, new Timestamp(2L)))),
      Seq((t1, true), (t2, false), (t3, true)))
    testCreateFilterFunction(
      Seq(And(GreaterThan(MODIFICATION_TIME, new Timestamp(1L)),
        LessThan(MODIFICATION_TIME, new Timestamp(3L)))),
      Seq((t1, false), (t2, true), (t3, false)))
    testCreateFilterFunction(
      Seq(Or(LessThanOrEqual(MODIFICATION_TIME, new Timestamp(1L)),
        GreaterThanOrEqual(MODIFICATION_TIME, new Timestamp(3L)))),
      Seq((t1, true), (t2, false), (t3, true)))
    testCreateFilterFunction(
      Seq(Not(IsNull(LENGTH))),
      Seq((t1, true), (t2, true), (t3, true)))

    // test filters applied on both columns
    testCreateFilterFunction(
      Seq(And(GreaterThan(LENGTH, 2L), LessThan(MODIFICATION_TIME, new Timestamp(2L)))),
      Seq((l1, false), (l2, false), (l3, true), (t1, false), (t2, false), (t3, false)))

    // test nested filters
    testCreateFilterFunction(
      // NOT (length > 2 OR modificationTime < 2)
      Seq(Not(Or(GreaterThan(LENGTH, 2L), LessThan(MODIFICATION_TIME, new Timestamp(2L))))),
      Seq((l1, false), (l2, false), (l3, false), (t1, false), (t2, true), (t3, true)))
  }

  test("buildReader") {
    def testBuildReader(fileStatus: FileStatus, filters: Seq[Filter], expected: Boolean): Unit = {
      val format = new BinaryFileFormat
      val reader = format.buildReaderWithPartitionValues(
        sparkSession = spark,
        dataSchema = schema,
        partitionSchema = StructType(Nil),
        requiredSchema = schema,
        filters = filters,
        options = Map.empty,
        hadoopConf = spark.sessionState.newHadoopConf())
      val partitionedFile = mock(classOf[PartitionedFile])
      when(partitionedFile.toPath).thenReturn(fileStatus.getPath)
      assert(reader(partitionedFile).nonEmpty === expected,
        s"Filters $filters applied to $fileStatus should be $expected.")
    }
    testBuildReader(file1Status, Seq.empty, true)
    testBuildReader(file1Status, Seq(LessThan(LENGTH, file1Status.getLen)), false)
    testBuildReader(file1Status, Seq(
      LessThan(MODIFICATION_TIME, new Timestamp(file1Status.getModificationTime))
    ), false)
    testBuildReader(file1Status, Seq(
      EqualTo(LENGTH, file1Status.getLen),
      EqualTo(MODIFICATION_TIME, file1Status.getModificationTime)
    ), true)
  }

  private def readBinaryFile(file: File, requiredSchema: StructType): Row = {
    val format = new BinaryFileFormat
    val reader = format.buildReaderWithPartitionValues(
      sparkSession = spark,
      dataSchema = schema,
      partitionSchema = StructType(Nil),
      requiredSchema = requiredSchema,
      filters = Seq.empty,
      options = Map.empty,
      hadoopConf = spark.sessionState.newHadoopConf()
    )
    val partitionedFile = mock(classOf[PartitionedFile])
    when(partitionedFile.toPath).thenReturn(new Path(file.toURI))
    val encoder = ExpressionEncoder(requiredSchema).resolveAndBind()
    encoder.createDeserializer().apply(reader(partitionedFile).next())
  }

  test("column pruning") {
    withTempPath { file =>
      val content = "123".getBytes
      Files.write(file.toPath, content, StandardOpenOption.CREATE, StandardOpenOption.WRITE)

      val actual = readBinaryFile(file, StructType(schema.takeRight(3)))
      val expected = Row(new Timestamp(file.lastModified()), content.length, content)

      assert(actual === expected)
    }
  }

  test("column pruning - non-readable file") {
    withTempPath { file =>
      val content = "abc".getBytes
      Files.write(file.toPath, content, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
      file.setReadable(false)

      // If content is selected, it throws an exception because it's not readable.
      intercept[IOException] {
        readBinaryFile(file, StructType(schema(CONTENT) :: Nil))
      }

      // Otherwise, it should be able to read.
      assert(
        readBinaryFile(file, StructType(schema(LENGTH) :: Nil)) === Row(content.length),
        "Get length should not read content.")
      assert(
        spark.read.format(BINARY_FILE).load(file.getPath).count() === 1,
        "Count should not read content.")
    }
  }

  test("fail fast and do not attempt to read if a file is too big") {
    assert(sqlConf.getConf(SOURCES_BINARY_FILE_MAX_LENGTH) === Int.MaxValue)
    withTempPath { file =>
      val path = file.getPath
      val content = "123".getBytes
      Files.write(file.toPath, content, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
      def readContent(): DataFrame = {
        spark.read.format(BINARY_FILE)
          .load(path)
          .select(CONTENT)
      }
      val expected = Seq(Row(content))
      checkAnswer(readContent(), expected)
      withSQLConf(SOURCES_BINARY_FILE_MAX_LENGTH.key -> content.length.toString) {
        checkAnswer(readContent(), expected)
      }
      // Disable read. If the implementation attempts to read, the exception would be different.
      file.setReadable(false)
      val caught = intercept[SparkException] {
        withSQLConf(SOURCES_BINARY_FILE_MAX_LENGTH.key -> (content.length - 1).toString) {
          checkAnswer(readContent(), expected)
        }
      }
      assert(caught.getCondition.startsWith("FAILED_READ_FILE"))
      assert(caught.getCause.getMessage.contains("exceeds the max length allowed"))
    }
  }

  private val chunkBytes = UnsafeWriter.STREAM_READ_CHUNK_BYTES

  private def withContentCopyFlags[T](
      singleContentCopy: Boolean,
      streamRead: Boolean = true,
      streamReadMinFileSize: Long = 0L)(f: => T): T = withSQLConf(
    SQLConf.BINARY_FILE_SINGLE_CONTENT_COPY_ENABLED.key -> singleContentCopy.toString,
    SQLConf.BINARY_FILE_STREAM_CONTENT_READ_ENABLED.key -> streamRead.toString,
    SQLConf.BINARY_FILE_STREAM_CONTENT_READ_MIN_FILE_SIZE.key -> streamReadMinFileSize.toString)(f)

  private def contentOf(numBytes: Int, seed: Int): Array[Byte] =
    Array.tabulate[Byte](numBytes)(i => ((i * 31 + seed) % 251 + 1).toByte)

  private def bufferLength(row: UnsafeRow): Int =
    row.getBaseObject.asInstanceOf[Array[Byte]].length

  private def parseFile(
      status: FileStatus,
      requiredSchema: StructType,
      open: () => InputStream): UnsafeRow = {
    BinaryFileFormat.parse(open, status, requiredSchema, Int.MaxValue, Seq.empty)
      .next().asInstanceOf[UnsafeRow]
  }

  private def buildFileReader(
      file: File,
      requiredSchema: StructType): (PartitionedFile => Iterator[InternalRow], PartitionedFile) = {
    val reader = new BinaryFileFormat().buildReaderWithPartitionValues(
      spark, schema, StructType(Nil), requiredSchema, Seq.empty, Map.empty,
      spark.sessionState.newHadoopConf())
    val partitionedFile = mock(classOf[PartitionedFile])
    when(partitionedFile.toPath).thenReturn(new Path(file.toURI))
    when(partitionedFile.partitionValues).thenReturn(InternalRow.empty)
    (reader, partitionedFile)
  }

  private class TrackingInputStream(bytes: Array[Byte]) extends InputStream {
    private val in = new ByteArrayInputStream(bytes)
    var readAllBytesCalls = 0
    var closed = false

    override def read(): Int = in.read()

    // Remote file systems may return short, unaligned reads.
    override def read(b: Array[Byte], off: Int, len: Int): Int =
      in.read(b, off, math.min(len, 4093))

    override def readAllBytes(): Array[Byte] = {
      readAllBytesCalls += 1
      super.readAllBytes()
    }

    override def close(): Unit = { closed = true }
  }

  test("SPARK-62256: exact row buffers across column selections, paths and content sizes") {
    val sizes = Seq(0, 7, 8, 2 * chunkBytes + 3)
    // scalastyle:off nonascii
    val paths = Seq("file:/data.bin", "file:/a b%c/\u6570\u636e\u6587\u4ef6.bin")
    // scalastyle:on nonascii
    val columnSets = Seq(
      Seq(PATH, MODIFICATION_TIME, LENGTH, CONTENT), Seq(CONTENT), Seq(CONTENT, PATH),
      Seq(LENGTH, CONTENT, MODIFICATION_TIME), Seq(PATH, LENGTH))
    for {
      columns <- columnSets
      path <- paths
      numBytes <- sizes
    } {
      val bytes = contentOf(numBytes, numBytes)
      val status = new FileStatus(numBytes, false, 0, 0, 1234567L, new Path(path))
      val required = StructType(columns.map(schema(_)))
      val expected = withContentCopyFlags(singleContentCopy = false) {
        parseFile(status, required, () => new TrackingInputStream(bytes))
      }
      Seq(0L, numBytes + 1L).foreach { minSize =>
        withContentCopyFlags(singleContentCopy = true, streamReadMinFileSize = minSize) {
          val input = new TrackingInputStream(bytes)
          val row = parseFile(status, required, () => input)
          assert(row === expected, s"$columns, $path, $numBytes bytes, minimum $minSize")
          columns.zipWithIndex.foreach {
            case (CONTENT, i) => assert(row.getBinary(i) === bytes)
            case (PATH, i) => assert(row.getUTF8String(i).toString === status.getPath.toString)
            case (LENGTH, i) => assert(row.getLong(i) === numBytes)
            case (MODIFICATION_TIME, i) =>
              assert(row.getLong(i) === DateTimeUtils.millisToMicros(1234567L))
          }
          if (columns.contains(CONTENT)) {
            assert(bufferLength(row) === row.getSizeInBytes)
            assert(input.readAllBytesCalls === (if (numBytes >= minSize) 0 else 1))
            assert(input.closed)
          } else {
            assert(!input.closed, "a metadata-only read must not open the stream")
          }
        }
      }
    }
  }

  test("SPARK-62256: partition and metadata projections preserve query results") {
    withTempDir { dir =>
      val entries = Seq("year=2014/small.bin" -> 1001,
        "year=2014/large.bin" -> (2 * chunkBytes + 3), "year=2015/word.bin" -> 8)
      entries.zipWithIndex.foreach { case ((name, size), seed) =>
        val file = new File(dir, name)
        file.getParentFile.mkdirs()
        Files.write(file.toPath, contentOf(size, seed))
      }
      for {
        path <- Seq(dir.getPath, new File(dir, "year=2014").getPath)
        columns <- Seq(Seq(CONTENT), Seq(PATH, LENGTH, CONTENT),
          Seq(CONTENT, "_metadata.file_name", "_metadata.file_size"))
      } {
        def read(): DataFrame = spark.read.format(BINARY_FILE).load(path)
          .select(columns.map(col): _*)
        val expected = withContentCopyFlags(singleContentCopy = false)(read().collect())
        withContentCopyFlags(singleContentCopy = true, streamReadMinFileSize = 4096L) {
          checkAnswer(read(), expected.toSeq)
        }
      }
      withContentCopyFlags(singleContentCopy = true) {
        val result = spark.read.format(BINARY_FILE).load(dir.getPath).select("year", CONTENT)
        checkAnswer(result, entries.zipWithIndex.map { case ((name, size), seed) =>
          Row(name.substring(5, 9).toInt, contentOf(size, seed))
        })
      }
    }
  }

  test("SPARK-62256: stale and unknown listed lengths preserve all stream content") {
    val bytes = contentOf(chunkBytes + 5, seed = 1)
    Seq(-1L, 0L, bytes.length - 40L, bytes.length.toLong, bytes.length + 50L, Int.MaxValue.toLong)
      .foreach { listed =>
        val status = new FileStatus(listed, false, 0, 0, 0, new Path("file:/stale.bin"))
        val expected = withContentCopyFlags(singleContentCopy = false) {
          parseFile(status, schema, () => new TrackingInputStream(bytes))
        }
        assert(expected.getBinary(schema.fieldIndex(CONTENT)) === bytes)
        Seq(true, false).foreach { streamRead =>
          withContentCopyFlags(singleContentCopy = true, streamRead = streamRead) {
            assert(parseFile(status, schema, () => new TrackingInputStream(bytes)) === expected)
          }
        }
      }
  }

  test("SPARK-62256: filesystem length changes preserve content through a scan") {
    withTempDir { dir =>
      val bytes = contentOf(1001, seed = 7)
      Files.write(new File(dir, "data.bin").toPath, bytes)
      Seq(classOf[LongerFileStatusFileSystem] -> 1009L,
        classOf[ShorterFileStatusFileSystem] -> 993L).foreach { case (fileSystem, listed) =>
        withSQLConf("fs.file.impl" -> fileSystem.getName, "fs.file.impl.disable.cache" -> "true") {
          Seq((false, false), (true, false), (true, true)).foreach {
            case (singleCopy, streamRead) =>
              withContentCopyFlags(singleCopy, streamRead) {
                val df = spark.read.format(BINARY_FILE).load(dir.getPath).select(LENGTH, CONTENT)
                checkAnswer(df, Row(listed, bytes))
              }
          }
        }
      }
    }
  }

  test("SPARK-62256: length limits and filters apply before allocating or opening content") {
    val threads = ManagementFactory.getThreadMXBean.asInstanceOf[ThreadMXBean]
    assume(threads.isThreadAllocatedMemorySupported && threads.isThreadAllocatedMemoryEnabled)
    val listed = 64L * chunkBytes
    val status = new FileStatus(listed, false, 0, 0, 0, new Path("file:/large.bin"))
    withContentCopyFlags(singleContentCopy = true) {
      val open = () => fail("content must not be opened")
      assert(BinaryFileFormat.parse(open, status, schema, 1024, Seq(_ => false)).isEmpty)
      val start = threads.getThreadAllocatedBytes(Thread.currentThread().threadId())
      val error = intercept[SparkException] {
        BinaryFileFormat.parse(open, status, schema, 1024, Seq.empty)
      }
      val allocated = threads.getThreadAllocatedBytes(Thread.currentThread().threadId()) - start
      assert(error.getMessage.contains("exceeds the max length allowed"))
      assert(allocated < listed / 8, s"allocated $allocated bytes for a file listed at $listed")
    }
  }

  test("SPARK-62256: stream failures propagate and close the content") {
    Seq(true, false).foreach { streamRead =>
      withContentCopyFlags(singleContentCopy = true, streamRead = streamRead) {
        val failure = new IOException("read failed")
        var closed = false
        val input = new InputStream {
          override def read(): Int = throw failure
          override def read(b: Array[Byte], off: Int, len: Int): Int = throw failure
          override def close(): Unit = { closed = true }
        }
        val status = new FileStatus(1001, false, 0, 0, 0, new Path("file:/failed.bin"))
        assert(intercept[IOException](parseFile(status, schema, () => input)) eq failure)
        assert(closed)
      }
    }
  }

  test("SPARK-62256: a whole-file read allocates the content once") {
    val threads = ManagementFactory.getThreadMXBean.asInstanceOf[ThreadMXBean]
    assume(threads.isThreadAllocatedMemorySupported && threads.isThreadAllocatedMemoryEnabled)
    val numBytes = 16 * chunkBytes + 3
    withTempPath { file =>
      Files.write(file.toPath, contentOf(numBytes, seed = 5))
      val required = StructType(Seq(schema(PATH), schema(CONTENT)))
      /** The fewest bytes this thread allocates to read the file's row, after a warm-up read. */
      def allocatedBytes(): Long = {
        val (reader, partitionedFile) =
          buildFileReader(file, required)
        reader(partitionedFile).next()
        Seq.fill(3) {
          val start = threads.getThreadAllocatedBytes(Thread.currentThread().threadId())
          val row = reader(partitionedFile).next().asInstanceOf[UnsafeRow]
          val allocated = threads.getThreadAllocatedBytes(Thread.currentThread().threadId()) - start
          assert(row.getSizeInBytes > numBytes)
          allocated
        }.min
      }
      // The 1 MiB read chunk and the small objects of opening and parsing one file.
      val slack = 4L * chunkBytes
      def measure(singleContentCopy: Boolean, streamRead: Boolean): Long =
        withContentCopyFlags(singleContentCopy = singleContentCopy, streamRead = streamRead) {
          allocatedBytes()
        }
      val streamed = measure(singleContentCopy = true, streamRead = true)
      val wholeFileArray = measure(singleContentCopy = true, streamRead = false)
      val legacy = measure(singleContentCopy = false, streamRead = false)
      val clue = s"$numBytes-byte file: streamed $streamed, whole-file array $wholeFileArray, " +
        s"legacy $legacy bytes"
      assert(streamed <= numBytes + slack, clue)
      // readAllBytes buffers the stream in pieces before copying it into one array.
      assert(wholeFileArray <= 3L * numBytes + slack, clue)
      // The measurement sees the copies this change removes.
      assert(legacy >= 4L * numBytes, clue)
    }
  }

  test("SPARK-28030: support chars in file names that require URL encoding") {
    withTempDir { dir =>
      val file = new File(dir, "test space.txt")
      val content = "123".getBytes
      Files.write(file.toPath, content, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
      val df = spark.read.format(BINARY_FILE).load(dir.getPath)
      df.select(col(PATH), col(CONTENT)).first() match {
        case Row(p: String, c: Array[Byte]) =>
          assert(p.endsWith(file.getAbsolutePath), "should support space in file name")
          assert(c === content, "should read file with space in file name")
      }
    }
  }
}

/** Reports a different file length, as if the file changed after it was listed. */
abstract class MisreportedLengthFileSystem(delta: Long) extends DebugFilesystem {
  override def getFileStatus(f: Path): FileStatus = {
    val status = super.getFileStatus(f)
    if (status.isFile) {
      new FileStatus(status.getLen + delta, false, status.getReplication, status.getBlockSize,
        status.getModificationTime, status.getPath)
    } else {
      status
    }
  }
}

class LongerFileStatusFileSystem extends MisreportedLengthFileSystem(8)

class ShorterFileStatusFileSystem extends MisreportedLengthFileSystem(-8)
