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

import java.nio.charset.Charset

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkFunSuite
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
    tracker.getFinalStats().asInstanceOf[BasicWriteTaskStats]
  }

  test("No files in run") {
    val tracker = new BasicWriteTaskStatsTracker(conf)
    assertStats(tracker, 0, 0)
  }

  test("Missing File") {
    val missing = new Path(tempDirPath, "missing")
    val tracker = new BasicWriteTaskStatsTracker(conf)
    tracker.newFile(missing.toString)
    assertStats(tracker, 0, 0)
  }

  test("Empty filename is forwarded") {
    val tracker = new BasicWriteTaskStatsTracker(conf)
    tracker.newFile("")
    intercept[IllegalArgumentException] {
      finalStatus(tracker)
    }
  }

  test("Null filename is only picked up in final status") {
    val tracker = new BasicWriteTaskStatsTracker(conf)
    tracker.newFile(null)
    intercept[IllegalArgumentException] {
      finalStatus(tracker)
    }
  }

  test("0 byte file") {
    val file = new Path(tempDirPath, "file0")
    val tracker = new BasicWriteTaskStatsTracker(conf)
    tracker.newFile(file.toString)
    touch(file)
    assertStats(tracker, 1, 0)
  }

  test("File with data") {
    val file = new Path(tempDirPath, "file-with-data")
    val tracker = new BasicWriteTaskStatsTracker(conf)
    tracker.newFile(file.toString)
    write1(file)
    assertStats(tracker, 1, len1)
  }

  test("Open file") {
    val file = new Path(tempDirPath, "file-open")
    val tracker = new BasicWriteTaskStatsTracker(conf)
    tracker.newFile(file.toString)
    val stream = localfs.create(file, true)
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
    tracker.newFile(file2.toString)
    write2(file2)
    assertStats(tracker, 2, len1 + len2)
  }

  test("Three files, last one empty") {
    val file1 = new Path(tempDirPath, "f-3-1")
    val file2 = new Path(tempDirPath, "f-3-2")
    val file3 = new Path(tempDirPath, "f-3-2")
    val tracker = new BasicWriteTaskStatsTracker(conf)
    tracker.newFile(file1.toString)
    write1(file1)
    tracker.newFile(file2.toString)
    write2(file2)
    tracker.newFile(file3.toString)
    touch(file3)
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

    // file 2 is noted, but not created
    tracker.newFile(file2.toString)

    // file 3 is noted & then created
    tracker.newFile(file3.toString)
    write2(file3)

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

}
