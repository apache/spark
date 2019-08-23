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
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

abstract class CheckpointFileManagerTests extends SparkFunSuite with SQLHelper {

  def createManager(path: Path): CheckpointFileManager

  test("mkdirs, list, createAtomic, open, delete, exists") {
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
      quietly {
        intercept[IOException] {
          // should throw exception since file exists and overwrite is false
          fm.createAtomic(path, overwriteIfPossible = false).close()
        }
      }

      // Create atomic with overwrite if possible
      path = new Path(s"$dir/file2")
      assert(!fm.exists(path))
      fm.createAtomic(path, overwriteIfPossible = true).cancel()
      assert(!fm.exists(path))
      fm.createAtomic(path, overwriteIfPossible = true).close()
      assert(fm.exists(path))
      fm.createAtomic(path, overwriteIfPossible = true).close()  // should not throw exception

      // crc file should not be leaked when origin file doesn't exist.
      // The implementation of Hadoop filesystem may filter out checksum file, so
      // listing files from local filesystem.
      val fileNames = new File(path.getParent.toString).listFiles().toSeq
        .filter(p => p.isFile).map(p => p.getName)
      val crcFiles = fileNames.filter(n => n.startsWith(".") && n.endsWith(".crc"))
      val originFileNamesForExistingCrcFiles = crcFiles.map { name =>
        // remove first "." and last ".crc"
        name.substring(1, name.length - 4)
      }

      // Check all origin files exist for all crc files.
      assert(originFileNamesForExistingCrcFiles.toSet.subsetOf(fileNames.toSet),
        s"Some of origin files for crc files don't exist - crc files: $crcFiles / " +
          s"expected origin files: $originFileNamesForExistingCrcFiles / actual files: $fileNames")

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
}

class CheckpointFileManagerSuite extends SharedSparkSession {

  test("CheckpointFileManager.create() should pick up user-specified class from conf") {
    withSQLConf(
      SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key ->
        classOf[CreateAtomicTestManager].getName) {
      val fileManager =
        CheckpointFileManager.create(new Path("/"), spark.sessionState.newHadoopConf)
      assert(fileManager.isInstanceOf[CreateAtomicTestManager])
    }
  }

  test("CheckpointFileManager.create() should fallback from FileContext to FileSystem") {
    import CheckpointFileManagerSuiteFileSystem.scheme
    spark.conf.set(s"fs.$scheme.impl", classOf[CheckpointFileManagerSuiteFileSystem].getName)
    quietly {
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
class CreateAtomicTestManager(path: Path, hadoopConf: Configuration)
  extends FileSystemBasedCheckpointFileManager(path, hadoopConf) {

  import CheckpointFileManager._

  override def createAtomic(path: Path, overwrite: Boolean): CancellableFSDataOutputStream = {
    if (CreateAtomicTestManager.shouldFailInCreateAtomic) {
      CreateAtomicTestManager.cancelCalledInCreateAtomic = false
    }
    val originalOut = super.createAtomic(path, overwrite)

    new CancellableFSDataOutputStream(originalOut) {
      override def close(): Unit = {
        if (CreateAtomicTestManager.shouldFailInCreateAtomic) {
          throw new IOException("Copy failed intentionally")
        }
        super.close()
      }

      override def cancel(): Unit = {
        CreateAtomicTestManager.cancelCalledInCreateAtomic = true
        originalOut.cancel()
      }
    }
  }
}

object CreateAtomicTestManager {
  @volatile var shouldFailInCreateAtomic = false
  @volatile var cancelCalledInCreateAtomic = false
}


/**
 * CheckpointFileManagerSuiteFileSystem to test fallback of the CheckpointFileManager
 * from FileContext to FileSystem API.
 */
private class CheckpointFileManagerSuiteFileSystem extends RawLocalFileSystem {
  import CheckpointFileManagerSuiteFileSystem.scheme

  override def getUri: URI = {
    URI.create(s"$scheme:///")
  }
}

private object CheckpointFileManagerSuiteFileSystem {
  val scheme = s"CheckpointFileManagerSuiteFileSystem${math.abs(Random.nextInt)}"
}
