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
import java.nio.charset.StandardCharsets

import scala.io.Source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.SparkException
import org.apache.spark.sql.execution.streaming.checkpointing._
import org.apache.spark.sql.internal.SQLConf

/**
 * This inherits tests for the [[CheckpointFileManager]] from [[CheckpointFileManagerTests]].
 * It also adds specific test cases for the [[ChecksumCheckpointFileManager]] e.g. test cases
 * to detect corrupt files, non-sequential reads, backward-compatibility etc.
 */
abstract class ChecksumCheckpointFileManagerSuite extends CheckpointFileManagerTestsOnLocalFs {
  import ChecksumCheckpointFileManager._

  override protected def checkLeakingCrcFiles(path: Path): Unit = {
    // The local implementation of hadoop file system may create crc files (e.g .foo.crc).
    // This will validate that those crc files are not leaked (i.e. no orphan crc file).
    // Note that for cloud implementation of hadoop file system, crc files are not created.
    super.checkLeakingCrcFiles(path)

    // Now let's validate our own crc files to make sure no orphan.
    val files = new File(path.toString).listFiles().toSeq
      .filter(f => f.isFile).map(f => new Path(f.toPath.toUri))
    val checksumFiles = files
      // filter out hadoop crc files if present (e.g .foo.crc)
      .filterNot(p => p.getName.startsWith(".") && p.getName.endsWith(".crc"))
      .filter(isChecksumFile)
      .map(ChecksumFile)
    val mainFilesForExistingChecksumFiles = checksumFiles.map(_.mainFilePath)

    // Check all main files exist for all checksum files.
    assert(mainFilesForExistingChecksumFiles.toSet.subsetOf(files.toSet),
      s"Some checksum files don't have main files - checksum files: $checksumFiles / " +
        s"expected main files: $mainFilesForExistingChecksumFiles / actual files: $files")
  }

  override def createManager(path: Path): CheckpointFileManager = {
    createChecksumManager(
      path,
      skipCreationIfFileMissingChecksum =
        SQLConf.STREAMING_CHECKPOINT_FILE_CHECKSUM_SKIP_CREATION_IF_FILE_MISSING_CHECKSUM
        .defaultValue.get)
  }

  /** Create a normal CheckpointFileManager (not the checksum checkpoint manager) */
  protected def createNoChecksumManager(path: Path): CheckpointFileManager

  protected def createChecksumManager(
    path: Path,
    skipCreationIfFileMissingChecksum: Boolean): CheckpointFileManager = {
    new ChecksumCheckpointFileManager(
      createNoChecksumManager(path),
      allowConcurrentDelete = true,
      numThreads = 4,
      skipCreationIfFileMissingChecksum = skipCreationIfFileMissingChecksum)
  }

  private def makeDir(fm: CheckpointFileManager, dir: Path): Unit = {
    assert(!fm.exists(dir))
    fm.mkdirs(dir)
    assert(fm.exists(dir))
  }

  private val content = 123456789
  private val fileSize = 4

  test("detect corrupt file") {
    withTempHadoopPath { basePath =>
      val checksumFm = createManager(basePath)
      // Mkdirs
      val dir = new Path(s"$basePath/dir/subdir/subsubdir")
      makeDir(checksumFm, dir)

      // Create file with checksum
      val path = new Path(s"$dir/file")
      checksumFm.createAtomic(path, overwriteIfPossible = false).writeContent(content).close()
      assert(checksumFm.exists(path))

      // First verify the content of the checksum file
      val regularFm = createNoChecksumManager(basePath)
      val checksumStream = regularFm.open(getChecksumPath(path))
      val checksum = Checksum.fromJson(
        Source.fromInputStream(checksumStream, StandardCharsets.UTF_8.name()).mkString)
      assert(checksum.mainFileSize == fileSize)
      checksumStream.close()

      // now corrupt the file
      // overwrite the file with a different content. This wouldn't update the checksum file.
      regularFm.createAtomic(path, overwriteIfPossible = true).writeContent(content % 10).close()

      val checksumError = intercept[SparkException] {
        // Now try to read the file with the checksum manager.
        checksumFm.open(path).close()
      }

      checkError(
        exception = checksumError,
        condition = "CHECKPOINT_FILE_CHECKSUM_VERIFICATION_FAILED",
        parameters = Map(
          "fileName" -> path.toString,
          "expectedSize" -> fileSize.toString,
          "expectedChecksum" -> "^-?\\d+$", // integer
          "computedSize" -> fileSize.toString,
          "computedChecksum" -> "^-?\\d+$"), // integer
        matchPVals = true)

      checksumFm.close()
    }
  }

  test("non sequential read is not allowed") {
    withTempHadoopPath { basePath =>
      val checksumFm = createManager(basePath)
      // Mkdirs
      val dir = new Path(s"$basePath/dir/subdir/subsubdir")
      makeDir(checksumFm, dir)

      // Create file
      val path = new Path(s"$dir/file")
      checksumFm.createAtomic(path, overwriteIfPossible = false).writeContent(content).close()
      assert(checksumFm.exists(path))

      // Attempt non sequential read
      val inputStream = checksumFm.open(path)

      val unsupported: Seq[FSDataInputStream => Unit] = Seq(
        _.seek(1),
        _.getPos(),
        _.seekToNewSource(1),
        _.read(1, new Array[Byte](1), 0, 1),
        _.readFully(1, new Array[Byte](1), 0, 1),
        _.readFully(1, new Array[Byte](1))
      )

      unsupported.foreach(op => {
        intercept[UnsupportedOperationException] {
          op(inputStream)
        }
      })

      checksumFm.close()
    }
  }

  test("checksum manager can read a file written by other manager") {
    withTempHadoopPath { basePath =>
      val regularFm = createNoChecksumManager(basePath)
      // Mkdirs
      val dir = new Path(s"$basePath/dir/subdir/subsubdir")
      makeDir(regularFm, dir)

      // Create a file using another manager
      val path = new Path(s"$dir/file")
      regularFm.createAtomic(path, overwriteIfPossible = false).writeContent(content).close()
      assert(regularFm.exists(path))

      // Now try to read the file with the checksum manager.
      val checksumFm = createManager(basePath)
      assert(checksumFm.open(path).readContent() == content)
      checksumFm.close()
    }
  }

  test("other manager can read a file written by checksum manager") {
    withTempHadoopPath { basePath =>
      val checksumFm = createManager(basePath)
      // Mkdirs
      val dir = new Path(s"$basePath/dir/subdir/subsubdir")
      makeDir(checksumFm, dir)

      // Create a file using checksum manager
      val path = new Path(s"$dir/file")
      checksumFm.createAtomic(path, overwriteIfPossible = false).writeContent(content).close()
      assert(checksumFm.exists(path))
      checksumFm.close()

      // Now try to read the file with other manager.
      val regularFm = createNoChecksumManager(basePath)
      assert(regularFm.open(path).readContent() == content)
    }
  }

  test("skip checksum creation if file missing checksum") {
    withTempHadoopPath { basePath =>
      val regularFm = createNoChecksumManager(basePath)
      // Mkdirs
      val dir = new Path(s"$basePath/dir/subdir/subsubdir")
      makeDir(regularFm, dir)

      // Create a file using the regular file manager
      val path = new Path(s"$dir/file")
      regularFm.createAtomic(path, overwriteIfPossible = true).writeContent(content).close()
      assert(regularFm.exists(path))

      // Now try to write and read the file with the checksum manager with fallback.
      val checksumFmWithFallback =
        createChecksumManager(basePath, skipCreationIfFileMissingChecksum = true)
      // Overwrite the file with a different content.
      checksumFmWithFallback.createAtomic(
        path, overwriteIfPossible = true).writeContent(content + 1).close()
      assert(checksumFmWithFallback.open(path).readContent() == content + 1)
      // Checksum should not be created since we fallback to the underlying file manager.
      assert(!checksumFmWithFallback.exists(getChecksumPath(path)))

      // Now try to write and read the file with the checksum manager without fallback.
      val checksumFmWithoutFallback =
        createChecksumManager(basePath, skipCreationIfFileMissingChecksum = false)
      // Overwrite the file with a different content.
      checksumFmWithoutFallback.createAtomic(
        path, overwriteIfPossible = true).writeContent(content + 2).close()
      assert(checksumFmWithoutFallback.open(path).readContent() == content + 2)
      // Checksum should be created since we don't fallback to the underlying file manager.
      assert(checksumFmWithoutFallback.exists(getChecksumPath(path)))

      // Try to write and read the file with the checksum manager with fallback when the checksum
      // file already exists.
      checksumFmWithFallback.createAtomic(
        path, overwriteIfPossible = true).writeContent(content + 3).close()
      // This read should succeed since we do not fallback to the underlying file manager, since
      // the checksum file already exists.
      assert(checksumFmWithFallback.open(path).readContent() == content + 3)
      assert(checksumFmWithFallback.exists(getChecksumPath(path)))

      regularFm.close()
      checksumFmWithFallback.close()
      checksumFmWithoutFallback.close()
    }
  }
}

class FileContextChecksumCheckpointFileManagerSuite extends ChecksumCheckpointFileManagerSuite {
  protected def createNoChecksumManager(path: Path): CheckpointFileManager = {
    new FileContextBasedCheckpointFileManager(path, new Configuration())
  }
}

class FileSystemChecksumCheckpointFileManagerSuite extends ChecksumCheckpointFileManagerSuite {
  protected def createNoChecksumManager(path: Path): CheckpointFileManager = {
    new FileSystemBasedCheckpointFileManager(path, new Configuration())
  }
}
