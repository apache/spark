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

package org.apache.spark.sql.execution.datasources

import java.nio.charset.{Charset, StandardCharsets}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FilterFileSystem, Path}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker.FILE_LENGTH_XATTR
import org.apache.spark.util.Utils

/**
 * Test how BasicWriteTaskStatsTracker handles files.
 *
 * Two different datasets are written (alongside 0), one of
 * length 10, one of 3. They were chosen to be distinct enough
 * that it is straightforward to determine which file lengths were added
 * from the sum of all files added. Lengths like "10" and "5" would
 * be less informative.
 */
class BasicWriteTaskStatsTrackerSuite extends SparkFunSuite {

  private val tempDir = Utils.createTempDir()
  private val tempDirPath = new Path(tempDir.toURI)
  private val conf = new Configuration()
  private val localfs = tempDirPath.getFileSystem(conf)
  private val data1 = "0123456789".getBytes(Charset.forName("US-ASCII"))
  private val data2 = "012".getBytes(Charset.forName("US-ASCII"))
  private val len1 = data1.length
  private val len2 = data2.length

  /**
   * In teardown delete the temp dir.
   */
  protected override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterAll()
    }
  }

  /**
   * Assert that the stats match that expected.
   * @param tracker tracker to check
   * @param files number of files expected
   * @param bytes total number of bytes expected
   */
  private def assertStats(
      tracker: BasicWriteTaskStatsTracker,
      files: Int,
      bytes: Int): Unit = {
    val stats = finalStatus(tracker)
    assert(files === stats.numFiles, "Wrong number of files")
    assert(bytes === stats.numBytes, "Wrong byte count of file size")
  }

  private def finalStatus(tracker: BasicWriteTaskStatsTracker): BasicWriteTaskStats = {
    tracker.getFinalStats(0L, 0L).asInstanceOf[BasicWriteTaskStats]
  }

  test("No files in run") {
    val tracker = new BasicWriteTaskStatsTracker(conf)
    assertStats(tracker, 0, 0)
  }

  test("Missing File") {
    val missing = new Path(tempDirPath, "missing")
    val tracker = new BasicWriteTaskStatsTracker(conf)
    tracker.newFile(missing.toString)
    tracker.closeFile(missing.toString)
    assertStats(tracker, 0, 0)
  }

  test("Empty filename is forwarded") {
    val tracker = new BasicWriteTaskStatsTracker(conf)
    tracker.newFile("")
    intercept[IllegalArgumentException] {
      tracker.closeFile("")
    }
  }

  test("Null filename is only picked up in final status") {
    val tracker = new BasicWriteTaskStatsTracker(conf)
    tracker.newFile(null)
    intercept[IllegalArgumentException] {
      tracker.closeFile(null)
    }
  }

  test("0 byte file") {
    val file = new Path(tempDirPath, "file0")
    val tracker = new BasicWriteTaskStatsTracker(conf)
    tracker.newFile(file.toString)
    touch(file)
    tracker.closeFile(file.toString)
    assertStats(tracker, 1, 0)
  }

  test("File with data") {
    val file = new Path(tempDirPath, "file-with-data")
    val tracker = new BasicWriteTaskStatsTracker(conf)
    tracker.newFile(file.toString)
    write1(file)
    tracker.closeFile(file.toString)
    assertStats(tracker, 1, len1)
  }

  test("Open file") {
    val file = new Path(tempDirPath, "file-open")
    val tracker = new BasicWriteTaskStatsTracker(conf)
    tracker.newFile(file.toString)
    val stream = localfs.create(file, true)
    tracker.closeFile(file.toString)
    try {
      assertStats(tracker, 1, 0)
      stream.write(data1)
      stream.flush()
      assert(1 === finalStatus(tracker).numFiles, "Wrong number of files")
    } finally {
      stream.close()
    }
  }

  test("Two files") {
    val file1 = new Path(tempDirPath, "f-2-1")
    val file2 = new Path(tempDirPath, "f-2-2")
    val tracker = new BasicWriteTaskStatsTracker(conf)
    tracker.newFile(file1.toString)
    write1(file1)
    tracker.closeFile(file1.toString)
    tracker.newFile(file2.toString)
    write2(file2)
    tracker.closeFile(file2.toString)
    assertStats(tracker, 2, len1 + len2)
  }

  test("Three files, last one empty") {
    val file1 = new Path(tempDirPath, "f-3-1")
    val file2 = new Path(tempDirPath, "f-3-2")
    val file3 = new Path(tempDirPath, "f-3-3")
    val tracker = new BasicWriteTaskStatsTracker(conf)
    tracker.newFile(file1.toString)
    write1(file1)
    tracker.closeFile(file1.toString)
    tracker.newFile(file2.toString)
    write2(file2)
    tracker.closeFile(file2.toString)
    tracker.newFile(file3.toString)
    touch(file3)
    tracker.closeFile(file3.toString)
    assertStats(tracker, 3, len1 + len2)
  }

  test("Three files, one not found") {
    val file1 = new Path(tempDirPath, "f-4-1")
    val file2 = new Path(tempDirPath, "f-4-2")
    val file3 = new Path(tempDirPath, "f-3-2")
    val tracker = new BasicWriteTaskStatsTracker(conf)
    // file 1
    tracker.newFile(file1.toString)
    write1(file1)
    tracker.closeFile(file1.toString)

    // file 2 is noted, but not created
    tracker.newFile(file2.toString)
    tracker.closeFile(file2.toString)

    // file 3 is noted & then created
    tracker.newFile(file3.toString)
    write2(file3)
    tracker.closeFile(file3.toString)

    // the expected size is file1 + file3; only two files are reported
    // as found
    assertStats(tracker, 2, len1 + len2)
  }

  /**
   * Write a 0-byte file.
   * @param file file path
   */
  private def touch(file: Path): Unit = {
    localfs.create(file, true).close()
  }

  /**
   * Write a byte array.
   * @param file path to file
   * @param data data
   * @return bytes written
   */
  private def write(file: Path, data: Array[Byte]): Integer = {
    val stream = localfs.create(file, true)
    try {
      stream.write(data)
    } finally {
      stream.close()
    }
    data.length
  }

  /**
   * Write a data1 array.
   * @param file file
   */
  private def write1(file: Path): Unit = {
    write(file, data1)
  }

  /**
   * Write a data2 array.
   *
   * @param file file
   */
  private def write2(file: Path): Unit = {
    write(file, data2)
  }

  /**
   * Does a length specified in the XAttr header get picked up?
   */
  test("XAttr sourced length") {
    val file = new Path(tempDirPath, "file")
    touch(file)
    val xattrFS = new FsWithFakeXAttrs(localfs)
    val bigLong = 34359738368L
    xattrFS.set(FILE_LENGTH_XATTR, s"$bigLong")
    val tracker = new BasicWriteTaskStatsTracker(conf)
    assert(Some(bigLong) === tracker.getFileSize(xattrFS, file),
      "Size not collected from XAttr entry")
  }

  /**
   * If a file is non-empty then the XAttr size declaration
   * is not used.
   */
  test("XAttr sourced length only used for 0-byte-files") {
    val file = new Path(tempDirPath, "file")
    write2(file)
    val xattrFS = new FsWithFakeXAttrs(localfs)
    val bigLong = 34359738368L
    xattrFS.set(FILE_LENGTH_XATTR, s"$bigLong")
    val tracker = new BasicWriteTaskStatsTracker(conf)
    assert(Some(len2) === tracker.getFileSize(xattrFS, file),
      "Size not collected from XAttr entry")
  }

  /**
   * Any FS which supports XAttr must raise an FNFE if the
   * file is missing. This verifies resilience on a path
   * which the local FS would not normally take.
   */
  test("Missing File with XAttr") {
    val missing = new Path(tempDirPath, "missing")
    val xattrFS = new FsWithFakeXAttrs(localfs)
    val tracker = new BasicWriteTaskStatsTracker(conf)
    tracker.newFile(missing.toString)
    assert(None === tracker.getFileSize(xattrFS, missing))
  }

  /**
   * If there are any problems parsing/validating the
   * header attribute, fall back to the file length.
   */
  test("XAttr error recovery") {
    val file = new Path(tempDirPath, "file")
    touch(file)
    val xattrFS = new FsWithFakeXAttrs(localfs)

    val tracker = new BasicWriteTaskStatsTracker(conf)

    // without a header
    assert(Some(0) === tracker.getFileSize(xattrFS, file))

    // will fail to parse as a long
    xattrFS.set(FILE_LENGTH_XATTR, "Not-a-long")
    assert(Some(0) === tracker.getFileSize(xattrFS, file))

    // a negative value
    xattrFS.set(FILE_LENGTH_XATTR, "-1")
    assert(Some(0) === tracker.getFileSize(xattrFS, file))

    // empty string
    xattrFS.set(FILE_LENGTH_XATTR, "")
    assert(Some(0) === tracker.getFileSize(xattrFS, file))

    // then a zero byte array
    xattrFS.setXAttr(file, FILE_LENGTH_XATTR,
      new Array[Byte](0))
    assert(Some(0) === tracker.getFileSize(xattrFS, file))
  }

  /**
   * Extend any FS with a mock get/setXAttr.
   * A map of attributes is used, these are returned on a getXAttr(path, key)
   * call to any path; the other XAttr list/get calls are not implemented.
   */
  class FsWithFakeXAttrs(fs: FileSystem) extends FilterFileSystem(fs) {

    private val xattrs = scala.collection.mutable.Map[String, Array[Byte]]()

    /**
     * Mock implementation of setAttr.
     *
     * @param path path (ignored)
     * @param name attribute name.
     * @param value byte array value
     */
    override def setXAttr(
      path: Path,
      name: String,
      value: Array[Byte]): Unit = {

      xattrs.put(name, value)
    }

    /**
     * Set an attribute to the UTF-8 byte value of a string.
     *
     * @param name  attribute name.
     * @param value string value
     */
    def set(name: String, value: String): Unit = {
      setXAttr(null, name, value.getBytes(StandardCharsets.UTF_8))
    }

    /**
     * Get any attribute if it is found in the map, else null.
     * @param path path (ignored)
     * @param name attribute name.
     * @return the byte[] value or null.
     */
    override def getXAttr(
      path: Path,
      name: String): Array[Byte] = {
      // force a check for the file and raise an FNFE if not found
      getFileStatus(path)

      xattrs.getOrElse(name, null)
    }
  }
}
