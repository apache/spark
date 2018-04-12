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

import java.io._
import java.net.URI

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

abstract class CheckpointFileManagerTests extends SparkFunSuite {

  def createManager(path: Path): CheckpointFileManager

  test("mkdirs, list, createAtomic, open, delete") {
    withTempPath { p =>
      val basePath = new Path(p.getAbsolutePath)
      val fm = createManager(basePath)
      // Mkdirs
      val dir = new Path(s"$basePath/dir/subdir/subsubdir")
      assert(!fm.exists(dir))
      fm.mkdirs(dir)
      assert(fm.exists(dir))
      fm.mkdirs(dir)

      // List
      val acceptAllFilter = new PathFilter {
        override def accept(path: Path): Boolean = true
      }
      val rejectAllFilter = new PathFilter {
        override def accept(path: Path): Boolean = false
      }
      assert(fm.list(basePath, acceptAllFilter).exists(_.getPath.getName == "dir"))
      assert(fm.list(basePath, rejectAllFilter).length === 0)

      // Create atomic without overwrite
      var path = new Path(s"$dir/file")
      assert(!fm.exists(path))
      fm.createAtomic(path, overwriteIfPossible = false).cancel()
      assert(!fm.exists(path))
      fm.createAtomic(path, overwriteIfPossible = false).close()
      assert(fm.exists(path))
      intercept[IOException] {
        // should throw exception since file exists and overwrite is false
        fm.createAtomic(path, overwriteIfPossible = false).close()
      }

      // Create atomic with overwrite if possible
      path = new Path(s"$dir/file2")
      assert(!fm.exists(path))
      fm.createAtomic(path, overwriteIfPossible = true).cancel()
      assert(!fm.exists(path))
      fm.createAtomic(path, overwriteIfPossible = true).close()
      assert(fm.exists(path))
      fm.createAtomic(path, overwriteIfPossible = true).close()  // should not throw exception

      // Open and delete
      fm.open(path).close()
      fm.delete(path)
      assert(!fm.exists(path))
      intercept[IOException] {
        fm.open(path)
      }
      fm.delete(path) // should not throw exception
    }
  }

  protected def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
  }
}

class CheckpointFileManagerSuite extends SparkFunSuite with SharedSparkSession {

  test("CheckpointFileManager.create() should pick up user-specified class from conf") {
    withSQLConf(
      SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key ->
        classOf[TestCheckpointFileManager].getName) {
      val fileManager =
        CheckpointFileManager.create(new Path("/"), spark.sessionState.newHadoopConf)
      assert(fileManager.isInstanceOf[TestCheckpointFileManager])
    }
  }

  test("CheckpointFileManager.create() should fallback from FileContext to FileSystem") {
    import FakeFileSystem.scheme
    spark.conf.set(
      s"fs.$scheme.impl",
      classOf[FakeFileSystem].getName)
    withTempDir { temp =>
      val metadataLog = new HDFSMetadataLog[String](spark, s"$scheme://${temp.toURI.getPath}")
      assert(metadataLog.add(0, "batch0"))
      assert(metadataLog.getLatest() === Some(0 -> "batch0"))
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.get(None, Some(0)) === Array(0 -> "batch0"))


      val metadataLog2 = new HDFSMetadataLog[String](spark, s"$scheme://${temp.toURI.getPath}")
      assert(metadataLog2.get(0) === Some("batch0"))
      assert(metadataLog2.getLatest() === Some(0 -> "batch0"))
      assert(metadataLog2.get(None, Some(0)) === Array(0 -> "batch0"))

    }
  }
}

class FileContextBasedCheckpointFileManagerSuite extends CheckpointFileManagerTests {
  override def createManager(path: Path): CheckpointFileManager = {
    new FileContextBasedCheckpointFileManager(path, new Configuration())
  }
}

class FileSystemBasedCheckpointFileManagerSuite extends CheckpointFileManagerTests {
  override def createManager(path: Path): CheckpointFileManager = {
    new FileSystemBasedCheckpointFileManager(path, new Configuration())
  }
}


/** A fake implementation to test different characteristics of CheckpointFileManager interface */
class TestCheckpointFileManager(path: Path, hadoopConf: Configuration)
  extends FileSystemBasedCheckpointFileManager(path, hadoopConf) {

  import CheckpointFileManager._

  override def createAtomic(path: Path, overwrite: Boolean): CancellableFSDataOutputStream = {
    if (TestCheckpointFileManager.shouldFailInCreateAtomic) {
      TestCheckpointFileManager.cancelCalledInCreateAtomic = false
    }
    val originalOut = super.createAtomic(path, overwrite)

    new CancellableFSDataOutputStream(originalOut) {
      override def close(): Unit = {
        if (TestCheckpointFileManager.shouldFailInCreateAtomic) {
          throw new IOException("Copy failed intentionally")
        }
        super.close()
      }

      override def cancel(): Unit = {
        TestCheckpointFileManager.cancelCalledInCreateAtomic = true
        originalOut.cancel()
      }
    }
  }
}

object TestCheckpointFileManager {
  @volatile var shouldFailInCreateAtomic = false
  @volatile var cancelCalledInCreateAtomic = false
}


/** FakeFileSystem to test fallback of the HDFSMetadataLog from FileContext to FileSystem API */
private class FakeFileSystem extends RawLocalFileSystem {
  import FakeFileSystem.scheme

  override def getUri: URI = {
    URI.create(s"$scheme:///")
  }
}

private object FakeFileSystem {
  val scheme = s"HDFSMetadataLogSuite${math.abs(Random.nextInt)}"
}
