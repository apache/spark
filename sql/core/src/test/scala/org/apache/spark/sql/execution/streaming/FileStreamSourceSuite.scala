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

package org.apache.spark.sql.execution.streaming

import java.io.File
import java.net.URI

import scala.util.Random

import org.apache.hadoop.fs.{FileStatus, Path, RawLocalFileSystem}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.streaming.ExistsThrowsExceptionFileSystem._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType

class FileStreamSourceSuite extends SparkFunSuite with SharedSQLContext {

  import FileStreamSource._

  test("SeenFilesMap") {
    val map = new SeenFilesMap(maxAgeMs = 10)

    map.add("a", 5)
    assert(map.size == 1)
    map.purge()
    assert(map.size == 1)

    // Add a new entry and purge should be no-op, since the gap is exactly 10 ms.
    map.add("b", 15)
    assert(map.size == 2)
    map.purge()
    assert(map.size == 2)

    // Add a new entry that's more than 10 ms than the first entry. We should be able to purge now.
    map.add("c", 16)
    assert(map.size == 3)
    map.purge()
    assert(map.size == 2)

    // Override existing entry shouldn't change the size
    map.add("c", 25)
    assert(map.size == 2)

    // Not a new file because we have seen c before
    assert(!map.isNewFile("c", 20))

    // Not a new file because timestamp is too old
    assert(!map.isNewFile("d", 5))

    // Finally a new file: never seen and not too old
    assert(map.isNewFile("e", 20))
  }

  test("SeenFilesMap should only consider a file old if it is earlier than last purge time") {
    val map = new SeenFilesMap(maxAgeMs = 10)

    map.add("a", 20)
    assert(map.size == 1)

    // Timestamp 5 should still considered a new file because purge time should be 0
    assert(map.isNewFile("b", 9))
    assert(map.isNewFile("b", 10))

    // Once purge, purge time should be 10 and then b would be a old file if it is less than 10.
    map.purge()
    assert(!map.isNewFile("b", 9))
    assert(map.isNewFile("b", 10))
  }

  testWithUninterruptibleThread("do not recheck that files exist during getBatch") {
    withTempDir { temp =>
      spark.conf.set(
        s"fs.$scheme.impl",
        classOf[ExistsThrowsExceptionFileSystem].getName)
      // add the metadata entries as a pre-req
      val dir = new File(temp, "dir") // use non-existent directory to test whether log make the dir
      val metadataLog =
        new FileStreamSourceLog(FileStreamSourceLog.VERSION, spark, dir.getAbsolutePath)
      assert(metadataLog.add(0, Array(FileEntry(s"$scheme:///file1", 100L, 0))))

      val newSource = new FileStreamSource(spark, s"$scheme:///", "parquet", StructType(Nil),
        dir.getAbsolutePath, Map.empty)
      // this method should throw an exception if `fs.exists` is called during resolveRelation
      newSource.getBatch(None, LongOffset(1))
    }
  }
}

/** Fake FileSystem to test whether the method `fs.exists` is called during
 * `DataSource.resolveRelation`.
 */
class ExistsThrowsExceptionFileSystem extends RawLocalFileSystem {
  override def getUri: URI = {
    URI.create(s"$scheme:///")
  }

  override def exists(f: Path): Boolean = {
    throw new IllegalArgumentException("Exists shouldn't have been called!")
  }

  /** Simply return an empty file for now. */
  override def listStatus(file: Path): Array[FileStatus] = {
    val emptyFile = new FileStatus()
    emptyFile.setPath(file)
    Array(emptyFile)
  }
}

object ExistsThrowsExceptionFileSystem {
  val scheme = s"FileStreamSourceSuite${math.abs(Random.nextInt)}fs"
}
