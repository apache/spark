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

package org.apache.spark.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataOutput, DataOutputStream, File,
  FileOutputStream, PrintStream}
import java.lang.{Double => JDouble, Float => JFloat}
import java.net.{BindException, ServerSocket, URI}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.text.DecimalFormatSymbols
import java.util.Locale
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPOutputStream

import scala.collection.mutable.ListBuffer
import scala.util.Random

import com.google.common.io.Files
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.SystemUtils
import org.apache.commons.math3.stat.inference.ChiSquareTest
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkFunSuite, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ByteUnit

class UtilsSuite extends SparkFunSuite with ResetSystemProperties with Logging {

  test("truncatedString") {
    assert(Utils.truncatedString(Nil, "[", ", ", "]", 2) == "[]")
    assert(Utils.truncatedString(Seq(1, 2), "[", ", ", "]", 2) == "[1, 2]")
    assert(Utils.truncatedString(Seq(1, 2, 3), "[", ", ", "]", 2) == "[1, ... 2 more fields]")
    assert(Utils.truncatedString(Seq(1, 2, 3), "[", ", ", "]", -5) == "[, ... 3 more fields]")
    assert(Utils.truncatedString(Seq(1, 2, 3), ", ") == "1, 2, 3")
  }

  test("timeConversion") {
    // Test -1
    assert(Utils.timeStringAsSeconds("-1") === -1)

    // Test zero
    assert(Utils.timeStringAsSeconds("0") === 0)

    assert(Utils.timeStringAsSeconds("1") === 1)
    assert(Utils.timeStringAsSeconds("1s") === 1)
    assert(Utils.timeStringAsSeconds("1000ms") === 1)
    assert(Utils.timeStringAsSeconds("1000000us") === 1)
    assert(Utils.timeStringAsSeconds("1m") === TimeUnit.MINUTES.toSeconds(1))
    assert(Utils.timeStringAsSeconds("1min") === TimeUnit.MINUTES.toSeconds(1))
    assert(Utils.timeStringAsSeconds("1h") === TimeUnit.HOURS.toSeconds(1))
    assert(Utils.timeStringAsSeconds("1d") === TimeUnit.DAYS.toSeconds(1))

    assert(Utils.timeStringAsMs("1") === 1)
    assert(Utils.timeStringAsMs("1ms") === 1)
    assert(Utils.timeStringAsMs("1000us") === 1)
    assert(Utils.timeStringAsMs("1s") === TimeUnit.SECONDS.toMillis(1))
    assert(Utils.timeStringAsMs("1m") === TimeUnit.MINUTES.toMillis(1))
    assert(Utils.timeStringAsMs("1min") === TimeUnit.MINUTES.toMillis(1))
    assert(Utils.timeStringAsMs("1h") === TimeUnit.HOURS.toMillis(1))
    assert(Utils.timeStringAsMs("1d") === TimeUnit.DAYS.toMillis(1))

    // Test invalid strings
    intercept[NumberFormatException] {
      Utils.timeStringAsMs("600l")
    }

    intercept[NumberFormatException] {
      Utils.timeStringAsMs("This breaks 600s")
    }

    intercept[NumberFormatException] {
      Utils.timeStringAsMs("This breaks 600ds")
    }

    intercept[NumberFormatException] {
      Utils.timeStringAsMs("600s This breaks")
    }

    intercept[NumberFormatException] {
      Utils.timeStringAsMs("This 123s breaks")
    }
  }

  test("Test byteString conversion") {
    // Test zero
    assert(Utils.byteStringAsBytes("0") === 0)

    assert(Utils.byteStringAsGb("1") === 1)
    assert(Utils.byteStringAsGb("1g") === 1)
    assert(Utils.byteStringAsGb("1023m") === 0)
    assert(Utils.byteStringAsGb("1024m") === 1)
    assert(Utils.byteStringAsGb("1048575k") === 0)
    assert(Utils.byteStringAsGb("1048576k") === 1)
    assert(Utils.byteStringAsGb("1k") === 0)
    assert(Utils.byteStringAsGb("1t") === ByteUnit.TiB.toGiB(1))
    assert(Utils.byteStringAsGb("1p") === ByteUnit.PiB.toGiB(1))

    assert(Utils.byteStringAsMb("1") === 1)
    assert(Utils.byteStringAsMb("1m") === 1)
    assert(Utils.byteStringAsMb("1048575b") === 0)
    assert(Utils.byteStringAsMb("1048576b") === 1)
    assert(Utils.byteStringAsMb("1023k") === 0)
    assert(Utils.byteStringAsMb("1024k") === 1)
    assert(Utils.byteStringAsMb("3645k") === 3)
    assert(Utils.byteStringAsMb("1024gb") === 1048576)
    assert(Utils.byteStringAsMb("1g") === ByteUnit.GiB.toMiB(1))
    assert(Utils.byteStringAsMb("1t") === ByteUnit.TiB.toMiB(1))
    assert(Utils.byteStringAsMb("1p") === ByteUnit.PiB.toMiB(1))

    assert(Utils.byteStringAsKb("1") === 1)
    assert(Utils.byteStringAsKb("1k") === 1)
    assert(Utils.byteStringAsKb("1m") === ByteUnit.MiB.toKiB(1))
    assert(Utils.byteStringAsKb("1g") === ByteUnit.GiB.toKiB(1))
    assert(Utils.byteStringAsKb("1t") === ByteUnit.TiB.toKiB(1))
    assert(Utils.byteStringAsKb("1p") === ByteUnit.PiB.toKiB(1))

    assert(Utils.byteStringAsBytes("1") === 1)
    assert(Utils.byteStringAsBytes("1k") === ByteUnit.KiB.toBytes(1))
    assert(Utils.byteStringAsBytes("1m") === ByteUnit.MiB.toBytes(1))
    assert(Utils.byteStringAsBytes("1g") === ByteUnit.GiB.toBytes(1))
    assert(Utils.byteStringAsBytes("1t") === ByteUnit.TiB.toBytes(1))
    assert(Utils.byteStringAsBytes("1p") === ByteUnit.PiB.toBytes(1))

    // Overflow handling, 1073741824p exceeds Long.MAX_VALUE if converted straight to Bytes
    // This demonstrates that we can have e.g 1024^3 PB without overflowing.
    assert(Utils.byteStringAsGb("1073741824p") === ByteUnit.PiB.toGiB(1073741824))
    assert(Utils.byteStringAsMb("1073741824p") === ByteUnit.PiB.toMiB(1073741824))

    // Run this to confirm it doesn't throw an exception
    assert(Utils.byteStringAsBytes("9223372036854775807") === 9223372036854775807L)
    assert(ByteUnit.PiB.toPiB(9223372036854775807L) === 9223372036854775807L)

    // Test overflow exception
    intercept[IllegalArgumentException] {
      // This value exceeds Long.MAX when converted to bytes
      Utils.byteStringAsBytes("9223372036854775808")
    }

    // Test overflow exception
    intercept[IllegalArgumentException] {
      // This value exceeds Long.MAX when converted to TB
      ByteUnit.PiB.toTiB(9223372036854775807L)
    }

    // Test fractional string
    intercept[NumberFormatException] {
      Utils.byteStringAsMb("0.064")
    }

    // Test fractional string
    intercept[NumberFormatException] {
      Utils.byteStringAsMb("0.064m")
    }

    // Test invalid strings
    intercept[NumberFormatException] {
      Utils.byteStringAsBytes("500ub")
    }

    // Test invalid strings
    intercept[NumberFormatException] {
      Utils.byteStringAsBytes("This breaks 600b")
    }

    intercept[NumberFormatException] {
      Utils.byteStringAsBytes("This breaks 600")
    }

    intercept[NumberFormatException] {
      Utils.byteStringAsBytes("600gb This breaks")
    }

    intercept[NumberFormatException] {
      Utils.byteStringAsBytes("This 123mb breaks")
    }
  }

  test("bytesToString") {
    assert(Utils.bytesToString(10) === "10.0 B")
    assert(Utils.bytesToString(1500) === "1500.0 B")
    assert(Utils.bytesToString(2000000) === "1953.1 KB")
    assert(Utils.bytesToString(2097152) === "2.0 MB")
    assert(Utils.bytesToString(2306867) === "2.2 MB")
    assert(Utils.bytesToString(5368709120L) === "5.0 GB")
    assert(Utils.bytesToString(5L * (1L << 40)) === "5.0 TB")
    assert(Utils.bytesToString(5L * (1L << 50)) === "5.0 PB")
    assert(Utils.bytesToString(5L * (1L << 60)) === "5.0 EB")
    assert(Utils.bytesToString(BigInt(1L << 11) * (1L << 60)) === "2.36E+21 B")
  }

  test("copyStream") {
    // input array initialization
    val bytes = Array.ofDim[Byte](9000)
    Random.nextBytes(bytes)

    val os = new ByteArrayOutputStream()
    Utils.copyStream(new ByteArrayInputStream(bytes), os)

    assert(os.toByteArray.toList.equals(bytes.toList))
  }

  test("memoryStringToMb") {
    assert(Utils.memoryStringToMb("1") === 0)
    assert(Utils.memoryStringToMb("1048575") === 0)
    assert(Utils.memoryStringToMb("3145728") === 3)

    assert(Utils.memoryStringToMb("1024k") === 1)
    assert(Utils.memoryStringToMb("5000k") === 4)
    assert(Utils.memoryStringToMb("4024k") === Utils.memoryStringToMb("4024K"))

    assert(Utils.memoryStringToMb("1024m") === 1024)
    assert(Utils.memoryStringToMb("5000m") === 5000)
    assert(Utils.memoryStringToMb("4024m") === Utils.memoryStringToMb("4024M"))

    assert(Utils.memoryStringToMb("2g") === 2048)
    assert(Utils.memoryStringToMb("3g") === Utils.memoryStringToMb("3G"))

    assert(Utils.memoryStringToMb("2t") === 2097152)
    assert(Utils.memoryStringToMb("3t") === Utils.memoryStringToMb("3T"))
  }

  test("splitCommandString") {
    assert(Utils.splitCommandString("") === Seq())
    assert(Utils.splitCommandString("a") === Seq("a"))
    assert(Utils.splitCommandString("aaa") === Seq("aaa"))
    assert(Utils.splitCommandString("a b c") === Seq("a", "b", "c"))
    assert(Utils.splitCommandString("  a   b\t c ") === Seq("a", "b", "c"))
    assert(Utils.splitCommandString("a 'b c'") === Seq("a", "b c"))
    assert(Utils.splitCommandString("a 'b c' d") === Seq("a", "b c", "d"))
    assert(Utils.splitCommandString("'b c'") === Seq("b c"))
    assert(Utils.splitCommandString("a \"b c\"") === Seq("a", "b c"))
    assert(Utils.splitCommandString("a \"b c\" d") === Seq("a", "b c", "d"))
    assert(Utils.splitCommandString("\"b c\"") === Seq("b c"))
    assert(Utils.splitCommandString("a 'b\" c' \"d' e\"") === Seq("a", "b\" c", "d' e"))
    assert(Utils.splitCommandString("a\t'b\nc'\nd") === Seq("a", "b\nc", "d"))
    assert(Utils.splitCommandString("a \"b\\\\c\"") === Seq("a", "b\\c"))
    assert(Utils.splitCommandString("a \"b\\\"c\"") === Seq("a", "b\"c"))
    assert(Utils.splitCommandString("a 'b\\\"c'") === Seq("a", "b\\\"c"))
    assert(Utils.splitCommandString("'a'b") === Seq("ab"))
    assert(Utils.splitCommandString("'a''b'") === Seq("ab"))
    assert(Utils.splitCommandString("\"a\"b") === Seq("ab"))
    assert(Utils.splitCommandString("\"a\"\"b\"") === Seq("ab"))
    assert(Utils.splitCommandString("''") === Seq(""))
    assert(Utils.splitCommandString("\"\"") === Seq(""))
  }

  test("string formatting of time durations") {
    val second = 1000
    val minute = second * 60
    val hour = minute * 60
    def str: (Long) => String = Utils.msDurationToString(_)

    val sep = new DecimalFormatSymbols(Locale.US).getDecimalSeparator

    assert(str(123) === "123 ms")
    assert(str(second) === "1" + sep + "0 s")
    assert(str(second + 462) === "1" + sep + "5 s")
    assert(str(hour) === "1" + sep + "00 h")
    assert(str(minute) === "1" + sep + "0 m")
    assert(str(minute + 4 * second + 34) === "1" + sep + "1 m")
    assert(str(10 * hour + minute + 4 * second) === "10" + sep + "02 h")
    assert(str(10 * hour + 59 * minute + 59 * second + 999) === "11" + sep + "00 h")
  }

  def getSuffix(isCompressed: Boolean): String = {
    if (isCompressed) {
      ".gz"
    } else {
      ""
    }
  }

  def writeLogFile(path: String, content: Array[Byte]): Unit = {
    val outputStream = if (path.endsWith(".gz")) {
      new GZIPOutputStream(new FileOutputStream(path))
    } else {
      new FileOutputStream(path)
    }
    IOUtils.write(content, outputStream)
    outputStream.close()
    content.size
  }

  private val workerConf = new SparkConf()

  def testOffsetBytes(isCompressed: Boolean): Unit = {
    val tmpDir2 = Utils.createTempDir()
    val suffix = getSuffix(isCompressed)
    val f1Path = tmpDir2 + "/f1" + suffix
    writeLogFile(f1Path, "1\n2\n3\n4\n5\n6\n7\n8\n9\n".getBytes(StandardCharsets.UTF_8))
    val f1Length = Utils.getFileLength(new File(f1Path), workerConf)

    // Read first few bytes
    assert(Utils.offsetBytes(f1Path, f1Length, 0, 5) === "1\n2\n3")

    // Read some middle bytes
    assert(Utils.offsetBytes(f1Path, f1Length, 4, 11) === "3\n4\n5\n6")

    // Read last few bytes
    assert(Utils.offsetBytes(f1Path, f1Length, 12, 18) === "7\n8\n9\n")

    // Read some nonexistent bytes in the beginning
    assert(Utils.offsetBytes(f1Path, f1Length, -5, 5) === "1\n2\n3")

    // Read some nonexistent bytes at the end
    assert(Utils.offsetBytes(f1Path, f1Length, 12, 22) === "7\n8\n9\n")

    // Read some nonexistent bytes on both ends
    assert(Utils.offsetBytes(f1Path, f1Length, -3, 25) === "1\n2\n3\n4\n5\n6\n7\n8\n9\n")

    Utils.deleteRecursively(tmpDir2)
  }

  test("reading offset bytes of a file") {
    testOffsetBytes(isCompressed = false)
  }

  test("reading offset bytes of a file (compressed)") {
    testOffsetBytes(isCompressed = true)
  }

  def testOffsetBytesMultipleFiles(isCompressed: Boolean): Unit = {
    val tmpDir = Utils.createTempDir()
    val suffix = getSuffix(isCompressed)
    val files = (1 to 3).map(i => new File(tmpDir, i.toString + suffix)) :+ new File(tmpDir, "4")
    writeLogFile(files(0).getAbsolutePath, "0123456789".getBytes(StandardCharsets.UTF_8))
    writeLogFile(files(1).getAbsolutePath, "abcdefghij".getBytes(StandardCharsets.UTF_8))
    writeLogFile(files(2).getAbsolutePath, "ABCDEFGHIJ".getBytes(StandardCharsets.UTF_8))
    writeLogFile(files(3).getAbsolutePath, "9876543210".getBytes(StandardCharsets.UTF_8))
    val fileLengths = files.map(Utils.getFileLength(_, workerConf))

    // Read first few bytes in the 1st file
    assert(Utils.offsetBytes(files, fileLengths, 0, 5) === "01234")

    // Read bytes within the 1st file
    assert(Utils.offsetBytes(files, fileLengths, 5, 8) === "567")

    // Read bytes across 1st and 2nd file
    assert(Utils.offsetBytes(files, fileLengths, 8, 18) === "89abcdefgh")

    // Read bytes across 1st, 2nd and 3rd file
    assert(Utils.offsetBytes(files, fileLengths, 5, 24) === "56789abcdefghijABCD")

    // Read bytes across 3rd and 4th file
    assert(Utils.offsetBytes(files, fileLengths, 25, 35) === "FGHIJ98765")

    // Read some nonexistent bytes in the beginning
    assert(Utils.offsetBytes(files, fileLengths, -5, 18) === "0123456789abcdefgh")

    // Read some nonexistent bytes at the end
    assert(Utils.offsetBytes(files, fileLengths, 18, 45) === "ijABCDEFGHIJ9876543210")

    // Read some nonexistent bytes on both ends
    assert(Utils.offsetBytes(files, fileLengths, -5, 45) ===
      "0123456789abcdefghijABCDEFGHIJ9876543210")

    Utils.deleteRecursively(tmpDir)
  }

  test("reading offset bytes across multiple files") {
    testOffsetBytesMultipleFiles(isCompressed = false)
  }

  test("reading offset bytes across multiple files (compressed)") {
    testOffsetBytesMultipleFiles(isCompressed = true)
  }

  test("deserialize long value") {
    val testval : Long = 9730889947L
    val bbuf = ByteBuffer.allocate(8)
    assert(bbuf.hasArray)
    bbuf.order(ByteOrder.BIG_ENDIAN)
    bbuf.putLong(testval)
    assert(bbuf.array.length === 8)
    assert(Utils.deserializeLongValue(bbuf.array) === testval)
  }

  test("writeByteBuffer should not change ByteBuffer position") {
    // Test a buffer with an underlying array, for both writeByteBuffer methods.
    val testBuffer = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4))
    assert(testBuffer.hasArray)
    val bytesOut = new ByteBufferOutputStream(4096)
    Utils.writeByteBuffer(testBuffer, bytesOut)
    assert(testBuffer.position() === 0)

    val dataOut = new DataOutputStream(bytesOut)
    Utils.writeByteBuffer(testBuffer, dataOut: DataOutput)
    assert(testBuffer.position() === 0)

    // Test a buffer without an underlying array, for both writeByteBuffer methods.
    val testDirectBuffer = ByteBuffer.allocateDirect(8)
    assert(!testDirectBuffer.hasArray())
    Utils.writeByteBuffer(testDirectBuffer, bytesOut)
    assert(testDirectBuffer.position() === 0)

    Utils.writeByteBuffer(testDirectBuffer, dataOut: DataOutput)
    assert(testDirectBuffer.position() === 0)
  }

  test("get iterator size") {
    val empty = Seq[Int]()
    assert(Utils.getIteratorSize(empty.toIterator) === 0L)
    val iterator = Iterator.range(0, 5)
    assert(Utils.getIteratorSize(iterator) === 5L)
  }

  test("getIteratorZipWithIndex") {
    val iterator = Utils.getIteratorZipWithIndex(Iterator(0, 1, 2), -1L + Int.MaxValue)
    assert(iterator.toArray === Array(
      (0, -1L + Int.MaxValue), (1, 0L + Int.MaxValue), (2, 1L + Int.MaxValue)
    ))
    intercept[IllegalArgumentException] {
      Utils.getIteratorZipWithIndex(Iterator(0, 1, 2), -1L)
    }
  }

  test("doesDirectoryContainFilesNewerThan") {
    // create some temporary directories and files
    val parent: File = Utils.createTempDir()
    // The parent directory has two child directories
    val child1: File = Utils.createTempDir(parent.getCanonicalPath)
    val child2: File = Utils.createTempDir(parent.getCanonicalPath)
    val child3: File = Utils.createTempDir(child1.getCanonicalPath)
    // set the last modified time of child1 to 30 secs old
    child1.setLastModified(System.currentTimeMillis() - (1000 * 30))

    // although child1 is old, child2 is still new so return true
    assert(Utils.doesDirectoryContainAnyNewFiles(parent, 5))

    child2.setLastModified(System.currentTimeMillis - (1000 * 30))
    assert(Utils.doesDirectoryContainAnyNewFiles(parent, 5))

    parent.setLastModified(System.currentTimeMillis - (1000 * 30))
    // although parent and its immediate children are new, child3 is still old
    // we expect a full recursive search for new files.
    assert(Utils.doesDirectoryContainAnyNewFiles(parent, 5))

    child3.setLastModified(System.currentTimeMillis - (1000 * 30))
    assert(!Utils.doesDirectoryContainAnyNewFiles(parent, 5))
  }

  test("resolveURI") {
    def assertResolves(before: String, after: String): Unit = {
      // This should test only single paths
      assume(before.split(",").length === 1)
      // Repeated invocations of resolveURI should yield the same result
      def resolve(uri: String): String = Utils.resolveURI(uri).toString
      assert(resolve(after) === after)
      assert(resolve(resolve(after)) === after)
      assert(resolve(resolve(resolve(after))) === after)
      // Also test resolveURIs with single paths
      assert(new URI(Utils.resolveURIs(before)) === new URI(after))
      assert(new URI(Utils.resolveURIs(after)) === new URI(after))
    }
    val rawCwd = System.getProperty("user.dir")
    val cwd = if (Utils.isWindows) s"/$rawCwd".replace("\\", "/") else rawCwd
    assertResolves("hdfs:/root/spark.jar", "hdfs:/root/spark.jar")
    assertResolves("hdfs:///root/spark.jar#app.jar", "hdfs:/root/spark.jar#app.jar")
    assertResolves("spark.jar", s"file:$cwd/spark.jar")
    assertResolves("spark.jar#app.jar", s"file:$cwd/spark.jar#app.jar")
    assertResolves("path to/file.txt", s"file:$cwd/path%20to/file.txt")
    if (Utils.isWindows) {
      assertResolves("C:\\path\\to\\file.txt", "file:/C:/path/to/file.txt")
      assertResolves("C:\\path to\\file.txt", "file:/C:/path%20to/file.txt")
    }
    assertResolves("file:/C:/path/to/file.txt", "file:/C:/path/to/file.txt")
    assertResolves("file:///C:/path/to/file.txt", "file:/C:/path/to/file.txt")
    assertResolves("file:/C:/file.txt#alias.txt", "file:/C:/file.txt#alias.txt")
    assertResolves("file:foo", s"file:foo")
    assertResolves("file:foo:baby", s"file:foo:baby")
  }

  test("resolveURIs with multiple paths") {
    def assertResolves(before: String, after: String): Unit = {
      assume(before.split(",").length > 1)
      assert(Utils.resolveURIs(before) === after)
      assert(Utils.resolveURIs(after) === after)
      // Repeated invocations of resolveURIs should yield the same result
      def resolve(uri: String): String = Utils.resolveURIs(uri)
      assert(resolve(after) === after)
      assert(resolve(resolve(after)) === after)
      assert(resolve(resolve(resolve(after))) === after)
    }
    val rawCwd = System.getProperty("user.dir")
    val cwd = if (Utils.isWindows) s"/$rawCwd".replace("\\", "/") else rawCwd
    assertResolves("jar1,jar2", s"file:$cwd/jar1,file:$cwd/jar2")
    assertResolves("file:/jar1,file:/jar2", "file:/jar1,file:/jar2")
    assertResolves("hdfs:/jar1,file:/jar2,jar3", s"hdfs:/jar1,file:/jar2,file:$cwd/jar3")
    assertResolves("hdfs:/jar1,file:/jar2,jar3,jar4#jar5,path to/jar6",
      s"hdfs:/jar1,file:/jar2,file:$cwd/jar3,file:$cwd/jar4#jar5,file:$cwd/path%20to/jar6")
    if (Utils.isWindows) {
      assertResolves("""hdfs:/jar1,file:/jar2,jar3,C:\pi.py#py.pi,C:\path to\jar4""",
        s"hdfs:/jar1,file:/jar2,file:$cwd/jar3,file:/C:/pi.py%23py.pi,file:/C:/path%20to/jar4")
    }
    assertResolves(",jar1,jar2", s"file:$cwd/jar1,file:$cwd/jar2")
  }

  test("nonLocalPaths") {
    assert(Utils.nonLocalPaths("spark.jar") === Array.empty)
    assert(Utils.nonLocalPaths("file:/spark.jar") === Array.empty)
    assert(Utils.nonLocalPaths("file:///spark.jar") === Array.empty)
    assert(Utils.nonLocalPaths("local:/spark.jar") === Array.empty)
    assert(Utils.nonLocalPaths("local:///spark.jar") === Array.empty)
    assert(Utils.nonLocalPaths("hdfs:/spark.jar") === Array("hdfs:/spark.jar"))
    assert(Utils.nonLocalPaths("hdfs:///spark.jar") === Array("hdfs:///spark.jar"))
    assert(Utils.nonLocalPaths("file:/spark.jar,local:/smart.jar,family.py") === Array.empty)
    assert(Utils.nonLocalPaths("local:/spark.jar,file:/smart.jar,family.py") === Array.empty)
    assert(Utils.nonLocalPaths("hdfs:/spark.jar,s3:/smart.jar") ===
      Array("hdfs:/spark.jar", "s3:/smart.jar"))
    assert(Utils.nonLocalPaths("hdfs:/spark.jar,path to/a.jar,s3:/smart.jar") ===
      Array("hdfs:/spark.jar", "s3:/smart.jar"))
    assert(Utils.nonLocalPaths("hdfs:/spark.jar,s3:/smart.jar,local.py,file:/hello/pi.py") ===
      Array("hdfs:/spark.jar", "s3:/smart.jar"))
    assert(Utils.nonLocalPaths("local.py,hdfs:/spark.jar,file:/hello/pi.py,s3:/smart.jar") ===
      Array("hdfs:/spark.jar", "s3:/smart.jar"))

    // Test Windows paths
    assert(Utils.nonLocalPaths("C:/some/path.jar", testWindows = true) === Array.empty)
    assert(Utils.nonLocalPaths("file:/C:/some/path.jar", testWindows = true) === Array.empty)
    assert(Utils.nonLocalPaths("file:///C:/some/path.jar", testWindows = true) === Array.empty)
    assert(Utils.nonLocalPaths("local:/C:/some/path.jar", testWindows = true) === Array.empty)
    assert(Utils.nonLocalPaths("local:///C:/some/path.jar", testWindows = true) === Array.empty)
    assert(Utils.nonLocalPaths("hdfs:/a.jar,C:/my.jar,s3:/another.jar", testWindows = true) ===
      Array("hdfs:/a.jar", "s3:/another.jar"))
    assert(Utils.nonLocalPaths("D:/your.jar,hdfs:/a.jar,s3:/another.jar", testWindows = true) ===
      Array("hdfs:/a.jar", "s3:/another.jar"))
    assert(Utils.nonLocalPaths("hdfs:/a.jar,s3:/another.jar,e:/our.jar", testWindows = true) ===
      Array("hdfs:/a.jar", "s3:/another.jar"))
  }

  test("isBindCollision") {
    // Negatives
    assert(!Utils.isBindCollision(null))
    assert(!Utils.isBindCollision(new Exception))
    assert(!Utils.isBindCollision(new Exception(new Exception)))
    assert(!Utils.isBindCollision(new Exception(new BindException)))

    // Positives
    val be = new BindException("Random Message")
    val be1 = new Exception(new BindException("Random Message"))
    val be2 = new Exception(new Exception(new BindException("Random Message")))
    assert(Utils.isBindCollision(be))
    assert(Utils.isBindCollision(be1))
    assert(Utils.isBindCollision(be2))

    // Actual bind exception
    var server1: ServerSocket = null
    var server2: ServerSocket = null
    try {
      server1 = new java.net.ServerSocket(0)
      server2 = new java.net.ServerSocket(server1.getLocalPort)
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[java.net.BindException])
        assert(Utils.isBindCollision(e))
    } finally {
      Option(server1).foreach(_.close())
      Option(server2).foreach(_.close())
    }
  }

  // Test for using the util function to change our log levels.
  test("log4j log level change") {
    val current = org.apache.log4j.Logger.getRootLogger().getLevel()
    try {
      Utils.setLogLevel(org.apache.log4j.Level.ALL)
      assert(log.isInfoEnabled())
      Utils.setLogLevel(org.apache.log4j.Level.ERROR)
      assert(!log.isInfoEnabled())
      assert(log.isErrorEnabled())
    } finally {
      // Best effort at undoing changes this test made.
      Utils.setLogLevel(current)
    }
  }

  test("deleteRecursively") {
    val tempDir1 = Utils.createTempDir()
    assert(tempDir1.exists())
    Utils.deleteRecursively(tempDir1)
    assert(!tempDir1.exists())

    val tempDir2 = Utils.createTempDir()
    val sourceFile1 = new File(tempDir2, "foo.txt")
    Files.touch(sourceFile1)
    assert(sourceFile1.exists())
    Utils.deleteRecursively(sourceFile1)
    assert(!sourceFile1.exists())

    val tempDir3 = new File(tempDir2, "subdir")
    assert(tempDir3.mkdir())
    val sourceFile2 = new File(tempDir3, "bar.txt")
    Files.touch(sourceFile2)
    assert(sourceFile2.exists())
    Utils.deleteRecursively(tempDir2)
    assert(!tempDir2.exists())
    assert(!tempDir3.exists())
    assert(!sourceFile2.exists())
  }

  test("loading properties from file") {
    val tmpDir = Utils.createTempDir()
    val outFile = File.createTempFile("test-load-spark-properties", "test", tmpDir)
    try {
      System.setProperty("spark.test.fileNameLoadB", "2")
      Files.write("spark.test.fileNameLoadA true\n" +
        "spark.test.fileNameLoadB 1\n", outFile, StandardCharsets.UTF_8)
      val properties = Utils.getPropertiesFromFile(outFile.getAbsolutePath)
      properties
        .filter { case (k, v) => k.startsWith("spark.")}
        .foreach { case (k, v) => sys.props.getOrElseUpdate(k, v)}
      val sparkConf = new SparkConf
      assert(sparkConf.getBoolean("spark.test.fileNameLoadA", false) === true)
      assert(sparkConf.getInt("spark.test.fileNameLoadB", 1) === 2)
    } finally {
      Utils.deleteRecursively(tmpDir)
    }
  }

  test("timeIt with prepare") {
    var cnt = 0
    val prepare = () => {
      cnt += 1
      Thread.sleep(1000)
    }
    val time = Utils.timeIt(2)({}, Some(prepare))
    require(cnt === 2, "prepare should be called twice")
    require(time < 500, "preparation time should not count")
  }

  test("fetch hcfs dir") {
    val tempDir = Utils.createTempDir()
    val sourceDir = new File(tempDir, "source-dir")
    val innerSourceDir = Utils.createTempDir(root = sourceDir.getPath)
    val sourceFile = File.createTempFile("someprefix", "somesuffix", innerSourceDir)
    val targetDir = new File(tempDir, "target-dir")
    Files.write("some text", sourceFile, StandardCharsets.UTF_8)

    val path =
      if (Utils.isWindows) {
        new Path("file:/" + sourceDir.getAbsolutePath.replace("\\", "/"))
      } else {
        new Path("file://" + sourceDir.getAbsolutePath)
      }
    val conf = new Configuration()
    val fs = Utils.getHadoopFileSystem(path.toString, conf)

    assert(!targetDir.isDirectory())
    Utils.fetchHcfsFile(path, targetDir, fs, new SparkConf(), conf, false)
    assert(targetDir.isDirectory())

    // Copy again to make sure it doesn't error if the dir already exists.
    Utils.fetchHcfsFile(path, targetDir, fs, new SparkConf(), conf, false)

    val destDir = new File(targetDir, sourceDir.getName())
    assert(destDir.isDirectory())

    val destInnerDir = new File(destDir, innerSourceDir.getName)
    assert(destInnerDir.isDirectory())

    val destInnerFile = new File(destInnerDir, sourceFile.getName)
    assert(destInnerFile.isFile())

    val filePath =
      if (Utils.isWindows) {
        new Path("file:/" + sourceFile.getAbsolutePath.replace("\\", "/"))
      } else {
        new Path("file://" + sourceFile.getAbsolutePath)
      }
    val testFileDir = new File(tempDir, "test-filename")
    val testFileName = "testFName"
    val testFilefs = Utils.getHadoopFileSystem(filePath.toString, conf)
    Utils.fetchHcfsFile(filePath, testFileDir, testFilefs, new SparkConf(),
                        conf, false, Some(testFileName))
    val newFileName = new File(testFileDir, testFileName)
    assert(newFileName.isFile())
  }

  test("shutdown hook manager") {
    val manager = new SparkShutdownHookManager()
    val output = new ListBuffer[Int]()

    val hook1 = manager.add(1, () => output += 1)
    manager.add(3, () => output += 3)
    manager.add(2, () => output += 2)
    manager.add(4, () => output += 4)
    manager.remove(hook1)

    manager.runAll()
    assert(output.toList === List(4, 3, 2))
  }

  test("isInDirectory") {
    val tmpDir = new File(sys.props("java.io.tmpdir"))
    val parentDir = new File(tmpDir, "parent-dir")
    val childDir1 = new File(parentDir, "child-dir-1")
    val childDir1b = new File(parentDir, "child-dir-1b")
    val childFile1 = new File(parentDir, "child-file-1.txt")
    val childDir2 = new File(childDir1, "child-dir-2")
    val childDir2b = new File(childDir1, "child-dir-2b")
    val childFile2 = new File(childDir1, "child-file-2.txt")
    val childFile3 = new File(childDir2, "child-file-3.txt")
    val nullFile: File = null
    parentDir.mkdir()
    childDir1.mkdir()
    childDir1b.mkdir()
    childDir2.mkdir()
    childDir2b.mkdir()
    childFile1.createNewFile()
    childFile2.createNewFile()
    childFile3.createNewFile()

    // Identity
    assert(Utils.isInDirectory(parentDir, parentDir))
    assert(Utils.isInDirectory(childDir1, childDir1))
    assert(Utils.isInDirectory(childDir2, childDir2))

    // Valid ancestor-descendant pairs
    assert(Utils.isInDirectory(parentDir, childDir1))
    assert(Utils.isInDirectory(parentDir, childFile1))
    assert(Utils.isInDirectory(parentDir, childDir2))
    assert(Utils.isInDirectory(parentDir, childFile2))
    assert(Utils.isInDirectory(parentDir, childFile3))
    assert(Utils.isInDirectory(childDir1, childDir2))
    assert(Utils.isInDirectory(childDir1, childFile2))
    assert(Utils.isInDirectory(childDir1, childFile3))
    assert(Utils.isInDirectory(childDir2, childFile3))

    // Inverted ancestor-descendant pairs should fail
    assert(!Utils.isInDirectory(childDir1, parentDir))
    assert(!Utils.isInDirectory(childDir2, parentDir))
    assert(!Utils.isInDirectory(childDir2, childDir1))
    assert(!Utils.isInDirectory(childFile1, parentDir))
    assert(!Utils.isInDirectory(childFile2, parentDir))
    assert(!Utils.isInDirectory(childFile3, parentDir))
    assert(!Utils.isInDirectory(childFile2, childDir1))
    assert(!Utils.isInDirectory(childFile3, childDir1))
    assert(!Utils.isInDirectory(childFile3, childDir2))

    // Non-existent files or directories should fail
    assert(!Utils.isInDirectory(parentDir, new File(parentDir, "one.txt")))
    assert(!Utils.isInDirectory(parentDir, new File(parentDir, "one/two.txt")))
    assert(!Utils.isInDirectory(parentDir, new File(parentDir, "one/two/three.txt")))

    // Siblings should fail
    assert(!Utils.isInDirectory(childDir1, childDir1b))
    assert(!Utils.isInDirectory(childDir1, childFile1))
    assert(!Utils.isInDirectory(childDir2, childDir2b))
    assert(!Utils.isInDirectory(childDir2, childFile2))

    // Null files should fail without throwing NPE
    assert(!Utils.isInDirectory(parentDir, nullFile))
    assert(!Utils.isInDirectory(childFile3, nullFile))
    assert(!Utils.isInDirectory(nullFile, parentDir))
    assert(!Utils.isInDirectory(nullFile, childFile3))
  }

  test("circular buffer: if nothing was written to the buffer, display nothing") {
    val buffer = new CircularBuffer(4)
    assert(buffer.toString === "")
  }

  test("circular buffer: if the buffer isn't full, print only the contents written") {
    val buffer = new CircularBuffer(10)
    val stream = new PrintStream(buffer, true, "UTF-8")
    stream.print("test")
    assert(buffer.toString === "test")
  }

  test("circular buffer: data written == size of the buffer") {
    val buffer = new CircularBuffer(4)
    val stream = new PrintStream(buffer, true, "UTF-8")

    // fill the buffer to its exact size so that it just hits overflow
    stream.print("test")
    assert(buffer.toString === "test")

    // add more data to the buffer
    stream.print("12")
    assert(buffer.toString === "st12")
  }

  test("circular buffer: multiple overflow") {
    val buffer = new CircularBuffer(25)
    val stream = new PrintStream(buffer, true, "UTF-8")

    stream.print("test circular test circular test circular test circular test circular")
    assert(buffer.toString === "st circular test circular")
  }

  test("nanSafeCompareDoubles") {
    def shouldMatchDefaultOrder(a: Double, b: Double): Unit = {
      assert(Utils.nanSafeCompareDoubles(a, b) === JDouble.compare(a, b))
      assert(Utils.nanSafeCompareDoubles(b, a) === JDouble.compare(b, a))
    }
    shouldMatchDefaultOrder(0d, 0d)
    shouldMatchDefaultOrder(0d, 1d)
    shouldMatchDefaultOrder(Double.MinValue, Double.MaxValue)
    assert(Utils.nanSafeCompareDoubles(Double.NaN, Double.NaN) === 0)
    assert(Utils.nanSafeCompareDoubles(Double.NaN, Double.PositiveInfinity) === 1)
    assert(Utils.nanSafeCompareDoubles(Double.NaN, Double.NegativeInfinity) === 1)
    assert(Utils.nanSafeCompareDoubles(Double.PositiveInfinity, Double.NaN) === -1)
    assert(Utils.nanSafeCompareDoubles(Double.NegativeInfinity, Double.NaN) === -1)
  }

  test("nanSafeCompareFloats") {
    def shouldMatchDefaultOrder(a: Float, b: Float): Unit = {
      assert(Utils.nanSafeCompareFloats(a, b) === JFloat.compare(a, b))
      assert(Utils.nanSafeCompareFloats(b, a) === JFloat.compare(b, a))
    }
    shouldMatchDefaultOrder(0f, 0f)
    shouldMatchDefaultOrder(1f, 1f)
    shouldMatchDefaultOrder(Float.MinValue, Float.MaxValue)
    assert(Utils.nanSafeCompareFloats(Float.NaN, Float.NaN) === 0)
    assert(Utils.nanSafeCompareFloats(Float.NaN, Float.PositiveInfinity) === 1)
    assert(Utils.nanSafeCompareFloats(Float.NaN, Float.NegativeInfinity) === 1)
    assert(Utils.nanSafeCompareFloats(Float.PositiveInfinity, Float.NaN) === -1)
    assert(Utils.nanSafeCompareFloats(Float.NegativeInfinity, Float.NaN) === -1)
  }

  test("isDynamicAllocationEnabled") {
    val conf = new SparkConf()
    conf.set("spark.master", "yarn")
    conf.set("spark.submit.deployMode", "client")
    assert(Utils.isDynamicAllocationEnabled(conf) === false)
    assert(Utils.isDynamicAllocationEnabled(
      conf.set("spark.dynamicAllocation.enabled", "false")) === false)
    assert(Utils.isDynamicAllocationEnabled(
      conf.set("spark.dynamicAllocation.enabled", "true")) === true)
    assert(Utils.isDynamicAllocationEnabled(
      conf.set("spark.executor.instances", "1")) === true)
    assert(Utils.isDynamicAllocationEnabled(
      conf.set("spark.executor.instances", "0")) === true)
    assert(Utils.isDynamicAllocationEnabled(conf.set("spark.master", "local")) === false)
    assert(Utils.isDynamicAllocationEnabled(conf.set("spark.dynamicAllocation.testing", "true")))
  }

  test("getDynamicAllocationInitialExecutors") {
    val conf = new SparkConf()
    assert(Utils.getDynamicAllocationInitialExecutors(conf) === 0)
    assert(Utils.getDynamicAllocationInitialExecutors(
      conf.set("spark.dynamicAllocation.minExecutors", "3")) === 3)
    assert(Utils.getDynamicAllocationInitialExecutors( // should use minExecutors
      conf.set("spark.executor.instances", "2")) === 3)
    assert(Utils.getDynamicAllocationInitialExecutors( // should use executor.instances
      conf.set("spark.executor.instances", "4")) === 4)
    assert(Utils.getDynamicAllocationInitialExecutors( // should use executor.instances
      conf.set("spark.dynamicAllocation.initialExecutors", "3")) === 4)
    assert(Utils.getDynamicAllocationInitialExecutors( // should use initialExecutors
      conf.set("spark.dynamicAllocation.initialExecutors", "5")) === 5)
    assert(Utils.getDynamicAllocationInitialExecutors( // should use minExecutors
      conf.set("spark.dynamicAllocation.initialExecutors", "2")
        .set("spark.executor.instances", "1")) === 3)
  }

  test("Set Spark CallerContext") {
    val context = "test"
    new CallerContext(context).setCurrentContext()
    if (CallerContext.callerContextSupported) {
      val callerContext = Utils.classForName("org.apache.hadoop.ipc.CallerContext")
      assert(s"SPARK_$context" ===
        callerContext.getMethod("getCurrent").invoke(null).toString)
    }
  }

  test("encodeFileNameToURIRawPath") {
    assert(Utils.encodeFileNameToURIRawPath("abc") === "abc")
    assert(Utils.encodeFileNameToURIRawPath("abc xyz") === "abc%20xyz")
    assert(Utils.encodeFileNameToURIRawPath("abc:xyz") === "abc:xyz")
  }

  test("decodeFileNameInURI") {
    assert(Utils.decodeFileNameInURI(new URI("files:///abc/xyz")) === "xyz")
    assert(Utils.decodeFileNameInURI(new URI("files:///abc")) === "abc")
    assert(Utils.decodeFileNameInURI(new URI("files:///abc%20xyz")) === "abc xyz")
  }

  test("Kill process") {
    // Verify that we can terminate a process even if it is in a bad state. This is only run
    // on UNIX since it does some OS specific things to verify the correct behavior.
    if (SystemUtils.IS_OS_UNIX) {
      def getPid(p: Process): Int = {
        val f = p.getClass().getDeclaredField("pid")
        f.setAccessible(true)
        f.get(p).asInstanceOf[Int]
      }

      def pidExists(pid: Int): Boolean = {
        val p = Runtime.getRuntime.exec(s"kill -0 $pid")
        p.waitFor()
        p.exitValue() == 0
      }

      def signal(pid: Int, s: String): Unit = {
        val p = Runtime.getRuntime.exec(s"kill -$s $pid")
        p.waitFor()
      }

      // Start up a process that runs 'sleep 10'. Terminate the process and assert it takes
      // less time and the process is no longer there.
      val startTimeMs = System.currentTimeMillis()
      val process = new ProcessBuilder("sleep", "10").start()
      val pid = getPid(process)
      try {
        assert(pidExists(pid))
        val terminated = Utils.terminateProcess(process, 5000)
        assert(terminated.isDefined)
        process.waitFor(5, TimeUnit.SECONDS)
        val durationMs = System.currentTimeMillis() - startTimeMs
        assert(durationMs < 5000)
        assert(!pidExists(pid))
      } finally {
        // Forcibly kill the test process just in case.
        signal(pid, "SIGKILL")
      }

      val versionParts = System.getProperty("java.version").split("[+.\\-]+", 3)
      var majorVersion = versionParts(0).toInt
      if (majorVersion == 1) majorVersion = versionParts(1).toInt
      if (majorVersion >= 8) {
        // We'll make sure that forcibly terminating a process works by
        // creating a very misbehaving process. It ignores SIGTERM and has been SIGSTOPed. On
        // older versions of java, this will *not* terminate.
        val file = File.createTempFile("temp-file-name", ".tmp")
        val cmd =
          s"""
             |#!/bin/bash
             |trap "" SIGTERM
             |sleep 10
           """.stripMargin
        Files.write(cmd.getBytes(StandardCharsets.UTF_8), file)
        file.getAbsoluteFile.setExecutable(true)

        val process = new ProcessBuilder(file.getAbsolutePath).start()
        val pid = getPid(process)
        assert(pidExists(pid))
        try {
          signal(pid, "SIGSTOP")
          val start = System.currentTimeMillis()
          val terminated = Utils.terminateProcess(process, 5000)
          assert(terminated.isDefined)
          process.waitFor(5, TimeUnit.SECONDS)
          val duration = System.currentTimeMillis() - start
          assert(duration < 6000) // add a little extra time to allow a force kill to finish
          assert(!pidExists(pid))
        } finally {
          signal(pid, "SIGKILL")
        }
      }
    }
  }

  test("chi square test of randomizeInPlace") {
    // Parameters
    val arraySize = 10
    val numTrials = 1000
    val threshold = 0.05
    val seed = 1L

    // results(i)(j): how many times Utils.randomize moves an element from position j to position i
    val results = Array.ofDim[Long](arraySize, arraySize)

    // This must be seeded because even a fair random process will fail this test with
    // probability equal to the value of `threshold`, which is inconvenient for a unit test.
    val rand = new java.util.Random(seed)
    val range = 0 until arraySize

    for {
      _ <- 0 until numTrials
      trial = Utils.randomizeInPlace(range.toArray, rand)
      i <- range
    } results(i)(trial(i)) += 1L

    val chi = new ChiSquareTest()

    // We expect an even distribution; this array will be rescaled by `chiSquareTest`
    val expected = Array.fill(arraySize * arraySize)(1.0)
    val observed = results.flatten

    // Performs Pearson's chi-squared test. Using the sum-of-squares as the test statistic, gives
    // the probability of a uniform distribution producing results as extreme as `observed`
    val pValue = chi.chiSquareTest(expected, observed)

    assert(pValue > threshold)
  }

  test("redact sensitive information") {
    val sparkConf = new SparkConf

    // Set some secret keys
    val secretKeys = Seq(
      "spark.executorEnv.HADOOP_CREDSTORE_PASSWORD",
      "spark.my.password",
      "spark.my.sECreT")
    secretKeys.foreach { key => sparkConf.set(key, "sensitive_value") }
    // Set a non-secret key
    sparkConf.set("spark.regular.property", "regular_value")
    // Set a property with a regular key but secret in the value
    sparkConf.set("spark.sensitive.property", "has_secret_in_value")

    // Redact sensitive information
    val redactedConf = Utils.redact(sparkConf, sparkConf.getAll).toMap

    // Assert that secret information got redacted while the regular property remained the same
    secretKeys.foreach { key => assert(redactedConf(key) === Utils.REDACTION_REPLACEMENT_TEXT) }
    assert(redactedConf("spark.regular.property") === "regular_value")
    assert(redactedConf("spark.sensitive.property") === Utils.REDACTION_REPLACEMENT_TEXT)

  }

  test("tryWithSafeFinally") {
    var e = new Error("Block0")
    val finallyBlockError = new Error("Finally Block")
    var isErrorOccurred = false
    // if the try and finally blocks throw different exception instances
    try {
      Utils.tryWithSafeFinally { throw e }(finallyBlock = { throw finallyBlockError })
    } catch {
      case t: Error =>
        assert(t.getSuppressed.head == finallyBlockError)
        isErrorOccurred = true
    }
    assert(isErrorOccurred)
    // if the try and finally blocks throw the same exception instance then it should not
    // try to add to suppressed and get IllegalArgumentException
    e = new Error("Block1")
    isErrorOccurred = false
    try {
      Utils.tryWithSafeFinally { throw e }(finallyBlock = { throw e })
    } catch {
      case t: Error =>
        assert(t.getSuppressed.length == 0)
        isErrorOccurred = true
    }
    assert(isErrorOccurred)
    // if the try throws the exception and finally doesn't throw exception
    e = new Error("Block2")
    isErrorOccurred = false
    try {
      Utils.tryWithSafeFinally { throw e }(finallyBlock = {})
    } catch {
      case t: Error =>
        assert(t.getSuppressed.length == 0)
        isErrorOccurred = true
    }
    assert(isErrorOccurred)
    // if the try and finally block don't throw exception
    Utils.tryWithSafeFinally {}(finallyBlock = {})
  }

  test("tryWithSafeFinallyAndFailureCallbacks") {
    var e = new Error("Block0")
    val catchBlockError = new Error("Catch Block")
    val finallyBlockError = new Error("Finally Block")
    var isErrorOccurred = false
    TaskContext.setTaskContext(TaskContext.empty())
    // if the try, catch and finally blocks throw different exception instances
    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks { throw e }(
        catchBlock = { throw catchBlockError }, finallyBlock = { throw finallyBlockError })
    } catch {
      case t: Error =>
        assert(t.getSuppressed.head == catchBlockError)
        assert(t.getSuppressed.last == finallyBlockError)
        isErrorOccurred = true
    }
    assert(isErrorOccurred)
    // if the try, catch and finally blocks throw the same exception instance then it should not
    // try to add to suppressed and get IllegalArgumentException
    e = new Error("Block1")
    isErrorOccurred = false
    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks { throw e }(catchBlock = { throw e },
        finallyBlock = { throw e })
    } catch {
      case t: Error =>
        assert(t.getSuppressed.length == 0)
        isErrorOccurred = true
    }
    assert(isErrorOccurred)
    // if the try throws the exception, catch and finally don't throw exceptions
    e = new Error("Block2")
    isErrorOccurred = false
    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks { throw e }(catchBlock = {}, finallyBlock = {})
    } catch {
      case t: Error =>
        assert(t.getSuppressed.length == 0)
        isErrorOccurred = true
    }
    assert(isErrorOccurred)
    // if the try, catch and finally blocks don't throw exceptions
    Utils.tryWithSafeFinallyAndFailureCallbacks {}(catchBlock = {}, finallyBlock = {})
    TaskContext.unset
  }
}
