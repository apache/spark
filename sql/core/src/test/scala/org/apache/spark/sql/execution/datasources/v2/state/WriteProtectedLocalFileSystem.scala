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
package org.apache.spark.sql.execution.datasources.v2.state

import java.net.URI
import java.nio.file.AccessDeniedException
import java.util
import java.util.concurrent.atomic.AtomicReference

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{CreateFlag, DelegateToFileSystem, FSDataOutputStream, FsServerDefaults, Options, Path}
import org.apache.hadoop.fs.local.LocalConfigKeys
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.StreamTest

/**
 * Write protection for the Hadoop FileSystem API (`fs.file.impl`).
 *
 * Extends [[org.apache.spark.DebugFilesystem]] so it replaces DebugFilesystem
 * without losing open-connection tracking. Every write entry point is overridden to deny
 * the operation when the target path is protected; reads are left untouched.
 */
class WriteProtectedLocalFileSystem extends org.apache.spark.DebugFilesystem {

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    if (WriteProtectedPaths.isProtected(f)) WriteProtectedPaths.deny("create", f)
    super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress)
  }

  override def create(
      f: Path,
      permission: FsPermission,
      flags: util.EnumSet[CreateFlag],
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable,
      checksumOpt: Options.ChecksumOpt): FSDataOutputStream = {
    if (WriteProtectedPaths.isProtected(f)) WriteProtectedPaths.deny("create", f)
    super.create(
      f, permission, flags, bufferSize, replication, blockSize, progress, checksumOpt)
  }

  override def createNonRecursive(
      f: Path,
      permission: FsPermission,
      flags: util.EnumSet[CreateFlag],
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    if (WriteProtectedPaths.isProtected(f)) WriteProtectedPaths.deny("createNonRecursive", f)
    super.createNonRecursive(f, permission, flags, bufferSize, replication, blockSize, progress)
  }

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = {
    if (WriteProtectedPaths.isProtected(f)) WriteProtectedPaths.deny("append", f)
    super.append(f, bufferSize, progress)
  }

  override def rename(src: Path, dst: Path): Boolean = {
    // A rename out of a protected path is also a write to that path -- check both ends.
    if (WriteProtectedPaths.isProtected(src)) WriteProtectedPaths.deny("rename from", src)
    if (WriteProtectedPaths.isProtected(dst)) WriteProtectedPaths.deny("rename to", dst)
    super.rename(src, dst)
  }

  override def delete(f: Path, recursive: Boolean): Boolean = {
    if (WriteProtectedPaths.isProtected(f)) WriteProtectedPaths.deny("delete", f)
    super.delete(f, recursive)
  }

  override def mkdirs(f: Path, permission: FsPermission): Boolean = {
    if (WriteProtectedPaths.isProtected(f)) WriteProtectedPaths.deny("mkdirs", f)
    super.mkdirs(f, permission)
  }

  override def truncate(f: Path, newLength: Long): Boolean = {
    if (WriteProtectedPaths.isProtected(f)) WriteProtectedPaths.deny("truncate", f)
    super.truncate(f, newLength)
  }

  override def setTimes(f: Path, mtime: Long, atime: Long): Unit = {
    if (WriteProtectedPaths.isProtected(f)) WriteProtectedPaths.deny("setTimes", f)
    super.setTimes(f, mtime, atime)
  }
}

/**
 * Write protection for the Hadoop FileContext/AbstractFileSystem API
 * (`fs.AbstractFileSystem.file.impl`).
 *
 * Needed because FileContextBasedCheckpointFileManager goes through FileContext, which
 * bypasses the FileSystem-level write methods overridden in [[WriteProtectedLocalFileSystem]].
 */
class WriteProtectedAbstractFileSystem(uri: URI, conf: Configuration)
    extends DelegateToFileSystem(
      uri,
      new WriteProtectedLocalFileSystem,
      conf,
      "file",
      false) {

  override def getUriDefaultPort(): Int = -1
  override def getServerDefaults(): FsServerDefaults = LocalConfigKeys.getServerDefaults
  override def isValidName(src: String): Boolean = true

  override def createInternal(
      f: Path,
      flag: util.EnumSet[CreateFlag],
      absolutePermission: FsPermission,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable,
      checksumOpt: Options.ChecksumOpt,
      createParent: Boolean): FSDataOutputStream = {
    if (WriteProtectedPaths.isProtected(f)) WriteProtectedPaths.deny("create", f)
    super.createInternal(
      f, flag, absolutePermission, bufferSize, replication, blockSize,
      progress, checksumOpt, createParent)
  }

  override def mkdir(
      dir: Path, permission: FsPermission, createParent: Boolean): Unit = {
    if (WriteProtectedPaths.isProtected(dir)) WriteProtectedPaths.deny("mkdir", dir)
    super.mkdir(dir, permission, createParent)
  }

  override def delete(f: Path, recursive: Boolean): Boolean = {
    if (WriteProtectedPaths.isProtected(f)) WriteProtectedPaths.deny("delete", f)
    super.delete(f, recursive)
  }

  override def renameInternal(src: Path, dst: Path): Unit = {
    // A rename out of a protected path is also a write to that path -- check both ends.
    if (WriteProtectedPaths.isProtected(src)) WriteProtectedPaths.deny("rename from", src)
    if (WriteProtectedPaths.isProtected(dst)) WriteProtectedPaths.deny("rename to", dst)
    super.renameInternal(src, dst)
  }

  override def truncate(f: Path, newLength: Long): Boolean = {
    if (WriteProtectedPaths.isProtected(f)) WriteProtectedPaths.deny("truncate", f)
    super.truncate(f, newLength)
  }

  override def setTimes(f: Path, mtime: Long, atime: Long): Unit = {
    if (WriteProtectedPaths.isProtected(f)) WriteProtectedPaths.deny("setTimes", f)
    super.setTimes(f, mtime, atime)
  }
}

/**
 * Shared state for write-protected paths. Used by both [[WriteProtectedLocalFileSystem]]
 * (FileSystem API) and [[WriteProtectedAbstractFileSystem]] (FileContext API).
 *
 * State is JVM-global: parallel suites in the same JVM would interfere. Tests in this
 * package run serially via [[StateDataSourceTestBase]], which clears state in `afterEach`.
 *
 * Path normalization: prefixes are stored with a trailing "/", and matches use
 * `pathStr.startsWith(prefix)`. Operations on the protected directory itself
 * (without trailing "/") are NOT matched -- only operations on its descendants.
 */
object WriteProtectedPaths {
  // Single AtomicReference so snapshot/restore are atomic; readers do one volatile load.
  private val protectedPrefixes =
    new AtomicReference[Set[String]](Set.empty[String])

  private def normalize(path: String): String =
    if (path.endsWith("/")) path else path + "/"

  private[state] def deny(op: String, p: Path): Nothing = {
    throw new AccessDeniedException(
      s"Write-protected path: $op attempted on path: $p. " +
      "This path is protected to simulate a read-only cloud object store checkpoint. " +
      "If you are seeing this in a test, it means the data source code path " +
      "is attempting a write that would fail on a read-only cloud object store path.")
  }

  def protectPath(path: String): Unit = {
    val normalized = normalize(path)
    protectedPrefixes.updateAndGet(_ + normalized)
  }

  def unprotectPath(path: String): Unit = {
    val normalized = normalize(path)
    protectedPrefixes.updateAndGet(_ - normalized)
  }

  def isProtected(p: Path): Boolean = {
    val current = protectedPrefixes.get()
    if (current.isEmpty) return false
    val pathStr = p.toUri.getPath
    current.exists(pathStr.startsWith)
  }

  def clearAll(): Unit = protectedPrefixes.set(Set.empty)

  /** Snapshot current prefixes and atomically clear -- single store update. */
  def snapshotAndClear(): java.util.Set[String] =
    new util.HashSet(protectedPrefixes.getAndSet(Set.empty).asJava)

  /**
   * Re-add the snapshot's entries. Non-destructive: prefixes added concurrently by other
   * code while the snapshot was inactive are preserved.
   */
  def restore(snapshot: java.util.Set[String]): Unit = {
    val toAdd = snapshot.asScala.toSet
    protectedPrefixes.updateAndGet(_ ++ toAdd)
  }
}

/**
 * Mix-in that installs [[WriteProtectedLocalFileSystem]] and auto-protects every
 * [[withTempDir]] from writes. Tests that perform writes outside of [[testStream]]
 * (e.g. provider API calls, log purging, direct file manipulation) must wrap them in
 * [[withWritableCheckpoint]] to temporarily suspend protection.
 *
 * Mix this into any test base whose reads should never write to the checkpoint path
 * (state data source, ...). The framework is JVM-global and assumes a single suite runs
 * at a time within a JVM.
 */
trait WriteProtectedCheckpointTestMixin extends StreamTest with BeforeAndAfterEach {

  // Set the write-protected filesystem via sparkConf so it is applied before the
  // SparkContext is created. Setting it later (e.g., on the session) is too late --
  // the Hadoop FileSystem cache may already hold a DebugFilesystem from startup.
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.hadoop.fs.file.impl",
        classOf[WriteProtectedLocalFileSystem].getCanonicalName)
      .set("spark.hadoop.fs.file.impl.disable.cache", "true")
      .set("spark.hadoop.fs.AbstractFileSystem.file.impl",
        classOf[WriteProtectedAbstractFileSystem].getCanonicalName)
      .set("spark.hadoop.fs.AbstractFileSystem.file.impl.disable.cache", "true")
  }

  override def afterEach(): Unit = {
    // Clear protections so teardown writes (state-store maintenance, temp-dir cleanup)
    // are not falsely flagged.
    WriteProtectedPaths.clearAll()
    super.afterEach()
  }

  /**
   * Override withTempDir to automatically protect the directory from writes.
   * [[testStream]] temporarily suspends protections for streaming query writes.
   * Everything else (data source reads) is blocked from writing.
   */
  override def withTempDir(f: java.io.File => Unit): Unit = {
    super.withTempDir { dir =>
      // Protect both absolute and canonical paths since data sources resolve paths
      // via makeQualified which may canonicalize.
      WriteProtectedPaths.protectPath(dir.getAbsolutePath)
      WriteProtectedPaths.protectPath(dir.getCanonicalPath)
      try {
        f(dir)
      } finally {
        WriteProtectedPaths.unprotectPath(dir.getAbsolutePath)
        WriteProtectedPaths.unprotectPath(dir.getCanonicalPath)
      }
    }
  }

  /**
   * Suspends write protection for a block. Use this for test setup code that writes
   * to the checkpoint outside of [[testStream]] (e.g. provider API calls, log purging,
   * writeStream.start(), legacy behavior tests).
   */
  protected def withWritableCheckpoint[T](body: => T): T = {
    val saved = WriteProtectedPaths.snapshotAndClear()
    try {
      body
    } finally {
      WriteProtectedPaths.restore(saved)
    }
  }

  /**
   * Assert that a write-protection-related exception exists in the cause chain.
   *
   * Accepted shapes:
   *   - AccessDeniedException: thrown directly by [[WriteProtectedLocalFileSystem]] /
   *     [[WriteProtectedAbstractFileSystem]].
   *   - "FAILED_TO_GET_CHANGELOG_WRITER" error class (CANNOT_LOAD_STATE_STORE in
   *     [[org.apache.spark.sql.errors.QueryExecutionErrors]]): RocksDB wraps the
   *     underlying AccessDeniedException when changelog-writer creation fails.
   */
  protected def assertWriteProtectionFailure(e: Throwable): Unit = {
    def matches(t: Throwable): Boolean = {
      val msg = Option(t.getMessage).getOrElse("")
      t.isInstanceOf[java.nio.file.AccessDeniedException] ||
        msg.contains("Write-protected") ||
        msg.contains("FAILED_TO_GET_CHANGELOG_WRITER")
    }
    val found = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).exists(matches)
    assert(found,
      s"Expected write-protection failure in cause chain, got: ${e.getClass}: ${e.getMessage}")
  }

  /**
   * Override testStream to temporarily suspend all protections during streaming query
   * execution. Streaming queries write checkpoints, state, and metadata. After the
   * query completes, protections are restored so subsequent reads are checked.
   */
  override def testStream(
      _stream: org.apache.spark.sql.Dataset[_],
      outputMode: org.apache.spark.sql.streaming.OutputMode =
        org.apache.spark.sql.streaming.OutputMode.Append,
      extraOptions: Map[String, String] = Map.empty,
      sink: org.apache.spark.sql.execution.streaming.sources.MemorySink =
        new org.apache.spark.sql.execution.streaming.sources.MemorySink())(
      actions: StreamAction*): Unit = {
    val saved = WriteProtectedPaths.snapshotAndClear()
    try {
      super.testStream(_stream, outputMode, extraOptions, sink)(actions: _*)
    } finally {
      WriteProtectedPaths.restore(saved)
    }
  }
}
