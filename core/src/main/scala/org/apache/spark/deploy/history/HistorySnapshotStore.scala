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

package org.apache.spark.deploy.history

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream, EOFException, IOException}
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.{Properties, UUID}

import scala.collection.Iterator
import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.FsHistoryProvider.InvalidHistorySnapshotException
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.History._
import org.apache.spark.status._
import org.apache.spark.status.api.v1
import org.apache.spark.status.protobuf.KVStoreProtobufSerializer
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.KVStore

private[spark] object HistorySnapshotStore extends Logging {

  private val SNAPSHOT_SCHEMA_VERSION = 1
  private val MANIFEST_FILE_PREFIX = "manifest-"
  private val SNAPSHOT_DIR_PREFIX = "snapshot-"

  private val STORE_ENTITY_CLASSES = Seq(
    classOf[org.apache.spark.status.ApplicationInfoWrapper],
    classOf[ApplicationEnvironmentInfoWrapper],
    classOf[AppSummary],
    classOf[ExecutorSummaryWrapper],
    classOf[JobDataWrapper],
    classOf[StageDataWrapper],
    classOf[ExecutorStageSummaryWrapper],
    classOf[SpeculationStageSummaryWrapper],
    classOf[PoolData],
    classOf[ProcessSummaryWrapper],
    classOf[ResourceProfileWrapper],
    classOf[RDDStorageInfoWrapper],
    classOf[RDDOperationGraphWrapper],
    classOf[StreamBlockData],
    classOf[TaskDataWrapper],
    classOf[CachedQuantile])

  private val SQL_ENTITY_CLASS_NAMES = Seq(
    "org.apache.spark.sql.execution.ui.SQLExecutionUIData",
    "org.apache.spark.sql.execution.ui.SparkPlanGraphWrapper",
    "org.apache.spark.sql.streaming.ui.StreamingQueryData",
    "org.apache.spark.sql.streaming.ui.StreamingQueryProgressWrapper")

  private val serializer = new KVStoreProtobufSerializer()

  private case class SnapshotEntry(className: String, fileName: String)

  /**
   * Returns whether an application attempt is complete enough to publish a history snapshot.
   *
   * Snapshots are only written for completed attempts so the published data is stable and can be
   * used by the History Server without replaying the event log.
   */
  def shouldWriteSnapshot(appInfo: v1.ApplicationInfo): Boolean = {
    appInfo != null && appInfo.attempts.lastOption.exists(_.completed)
  }

  /**
   * Writes a versioned history snapshot for an application attempt and returns the manifest path.
   *
   * The snapshot is published under the configured snapshot root using a fresh manifest and data
   * directory so readers can safely continue using the previous published snapshot while a new one
   * is being written.
   */
  def writeSnapshot(
      conf: SparkConf,
      store: KVStore,
      appId: String,
      attemptId: Option[String]): Option[Path] = {
    if (!isEnabled(conf)) {
      return None
    }

    val rootPath = snapshotDir(conf, appId, attemptId).getOrElse {
      return None
    }
    val fs = rootPath.getFileSystem(SparkHadoopUtil.get.newConfiguration(conf))
    fs.mkdirs(rootPath)
    val snapshotId = newSnapshotId()
    val dataDir = new Path(rootPath, s"$SNAPSHOT_DIR_PREFIX$snapshotId")
    val manifestPath = new Path(rootPath, s"$MANIFEST_FILE_PREFIX$snapshotId.properties")

    try {
      fs.mkdirs(dataDir)
      val entries = snapshotEntityClasses.zipWithIndex.flatMap { case (klass, idx) =>
        val fileName = f"$idx%03d-${klass.getSimpleName}.pb"
        val path = new Path(dataDir, fileName)
        if (writeEntityFile(store, klass, fs, path, appId) > 0L) {
          Some(SnapshotEntry(klass.getName, fileName))
        } else {
          fs.delete(path, false)
          None
        }
      }

      val manifest = new Properties()
      manifest.setProperty("schemaVersion", SNAPSHOT_SCHEMA_VERSION.toString)
      manifest.setProperty("snapshotDir", dataDir.getName)
      manifest.setProperty("classNames", entries.map(_.className).mkString(","))
      entries.foreach { entry =>
        manifest.setProperty(s"${entry.className}.file", entry.fileName)
      }

      Utils.tryWithResource(fs.create(manifestPath, false)) { out =>
        manifest.store(out, "spark history snapshot")
      }
      // Only older manifests are deleted, so readers always see either the old snapshot or the
      // newly published one.
      cleanupStaleSnapshots(fs, rootPath, manifestPath)
      Some(manifestPath)
    } catch {
      case NonFatal(e) =>
        deletePathQuietly(fs, manifestPath, recursive = false)
        deletePathQuietly(fs, dataDir, recursive = true)
        throw e
    }
  }

  /**
   * Returns the latest published history snapshot manifest for an application attempt, if any.
   *
   * When multiple manifests exist, the latest versioned manifest is treated as the active
   * published snapshot.
   */
  def findSnapshot(
      conf: SparkConf,
      appId: String,
      attemptId: Option[String]): Option[Path] = {
    if (!isEnabled(conf)) {
      return None
    }

    snapshotDir(conf, appId, attemptId).flatMap { dir =>
      val fs = dir.getFileSystem(SparkHadoopUtil.get.newConfiguration(conf))
      try {
        latestManifest(fs, dir)
      } catch {
        case NonFatal(e) =>
          logWarning(s"Failed to check history snapshot manifest in $dir.", e)
          None
      }
    }
  }

  /**
   * Returns the active manifest path for an application attempt.
   *
   * If no snapshot has been published yet, this returns the path pattern where a versioned
   * manifest would be created so callers can still log the intended destination.
   */
  def manifestPath(
      conf: SparkConf,
      appId: String,
      attemptId: Option[String]): Option[Path] = {
    findSnapshot(conf, appId, attemptId).orElse {
      snapshotDir(conf, appId, attemptId).map(new Path(_, s"$MANIFEST_FILE_PREFIX*.properties"))
    }
  }

  /**
   * Loads a published history snapshot into the provided KVStore.
   *
   * The manifest controls which entity files are restored and where the snapshot data directory is
   * located relative to the manifest path.
   */
  def restoreSnapshot(conf: SparkConf, store: KVStore, manifestPath: Path): Unit = {
    val fs = manifestPath.getFileSystem(SparkHadoopUtil.get.newConfiguration(conf))
    val manifest = loadManifest(fs, manifestPath)
    val schemaVersion = manifest.getProperty("schemaVersion", "-1").toInt
    require(schemaVersion == SNAPSHOT_SCHEMA_VERSION,
      s"Unsupported history snapshot version $schemaVersion at $manifestPath")

    val dataRoot = snapshotDataRoot(manifestPath, manifest)
    manifestEntries(manifestPath, manifest).foreach { entry =>
      loadSnapshotClass(manifestPath, entry.className).foreach { klass =>
        readEntityFile(store, klass, fs, new Path(dataRoot, entry.fileName))
      }
    }
  }

  /**
   * Returns the total on-storage size of a published history snapshot, including its manifest.
   *
   * This is used by History Server disk-store allocation so the target local store can reserve
   * enough space before restoring a snapshot, and reports unreadable snapshot metadata as an
   * invalid snapshot.
   */
  def snapshotSize(conf: SparkConf, manifestPath: Path): Long = {
    try {
      val fs = manifestPath.getFileSystem(SparkHadoopUtil.get.newConfiguration(conf))
      val manifest = loadManifest(fs, manifestPath)
      val root = snapshotDataRoot(manifestPath, manifest)
      val manifestSize = fs.getFileStatus(manifestPath).getLen
      manifestEntries(manifestPath, manifest)
        .foldLeft(manifestSize) { (total, entry) =>
          total + fs.getFileStatus(new Path(root, entry.fileName)).getLen
        }
    } catch {
      case e: Exception =>
        throw InvalidHistorySnapshotException(manifestPath, e)
    }
  }

  /**
   * Deletes a published history snapshot after it is determined to be unreadable or invalid.
   *
   * Any failure while deleting is logged and suppressed so snapshot invalidation does not mask the
   * original restore failure.
   */
  def invalidateSnapshot(
      conf: SparkConf,
      appId: String,
      manifestPath: Path): Unit = {
    val fs = manifestPath.getFileSystem(SparkHadoopUtil.get.newConfiguration(conf))
    Utils.tryLogNonFatalError {
      deleteSnapshotArtifacts(fs, manifestPath)
      logInfo(
        s"Deleted invalid history snapshot $manifestPath for appId: $appId.")
    }
  }

  private[spark] def isEnabled(conf: SparkConf): Boolean = {
    conf.get(SNAPSHOT_ENABLED) && conf.get(SNAPSHOT_PATH).nonEmpty
  }

  /** Returns the entity classes that should be materialized into every history snapshot. */
  private def snapshotEntityClasses: Seq[Class[_]] = {
    STORE_ENTITY_CLASSES ++ SQL_ENTITY_CLASS_NAMES.flatMap(loadOptionalClass)
  }

  /** Loads an optional snapshot class and skips it when the corresponding module is absent. */
  private def loadOptionalClass(className: String): Option[Class[_]] = {
    try {
      Some(Utils.classForName(className, initialize = false))
    } catch {
      case _: ClassNotFoundException =>
        None
    }
  }

  private def snapshotDir(
      conf: SparkConf,
      appId: String,
      attemptId: Option[String]): Option[Path] = {
    conf.get(SNAPSHOT_PATH).map { root =>
      new Path(new Path(root, encode(appId)), encode(attemptId.getOrElse("_default_")))
    }
  }

  private def encode(value: String): String = {
    URLEncoder.encode(value, StandardCharsets.UTF_8.name())
  }

  /** Reads and parses a snapshot manifest file. */
  private def loadManifest(fs: FileSystem, manifestPath: Path): Properties = {
    val properties = new Properties()
    Utils.tryWithResource(fs.open(manifestPath)) { in =>
      properties.load(in)
    }
    properties
  }

  /** Parses the manifest into concrete class-to-file entries and validates the required mapping. */
  private def manifestEntries(manifestPath: Path, manifest: Properties): Seq[SnapshotEntry] = {
    manifest.getProperty("classNames", "")
      .split(",")
      .iterator
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { className =>
        val fileName = Option(manifest.getProperty(s"$className.file")).getOrElse {
          throw new IOException(
            s"Missing snapshot file mapping for $className in history snapshot $manifestPath")
        }
        SnapshotEntry(className, fileName)
      }
      .toSeq
  }

  /** Resolves the snapshot data directory referenced by a manifest. */
  private def snapshotDataRoot(manifestPath: Path, manifest: Properties): Path = {
    Option(manifest.getProperty("snapshotDir")).map { dir =>
      new Path(manifestPath.getParent, dir)
    }.getOrElse {
      throw new IOException(
        s"Missing snapshotDir in history snapshot manifest $manifestPath")
    }
  }

  /** Loads a snapshot entity class, tolerating optional SQL classes that are absent at restore. */
  private def loadSnapshotClass(manifestPath: Path, className: String): Option[Class[_]] = {
    try {
      Some(Utils.classForName(className, initialize = false))
    } catch {
      case e: ClassNotFoundException if SQL_ENTITY_CLASS_NAMES.contains(className) =>
        logWarning(
          s"Ignoring optional history snapshot class $className from $manifestPath because " +
            "it is not available on the classpath.")
        None
      case e: ClassNotFoundException =>
        throw new IOException(
          s"Required history snapshot class $className from $manifestPath could not be loaded.",
          e)
    }
  }

  /** Returns the newest published manifest under a snapshot root, ordered by manifest name. */
  private def latestManifest(fs: FileSystem, rootPath: Path): Option[Path] = {
    val statuses = Option(fs.listStatus(rootPath)).getOrElse(Array.empty)
    val manifests = statuses.map(_.getPath).filter(isVersionedManifest).sortBy(_.getName)
    manifests.lastOption
  }

  private def isVersionedManifest(path: Path): Boolean = {
    val name = path.getName
    name.startsWith(MANIFEST_FILE_PREFIX) &&
      name.endsWith(".properties")
  }

  /**
   * Creates a lexicographically sortable snapshot id so newer manifests compare after older ones.
   */
  private def newSnapshotId(): String = {
    f"${System.currentTimeMillis()}%020d-${UUID.randomUUID().toString}"
  }

  /** Selects older manifests that can be deleted once a newer manifest has been published. */
  private[history] def staleManifestsForCleanup(
      paths: Seq[Path],
      keepManifestPath: Path): Seq[Path] = {
    paths
      .filter(isVersionedManifest)
      .filterNot(_ == keepManifestPath)
      .filter { path =>
        isVersionedManifest(keepManifestPath) && path.getName < keepManifestPath.getName
      }
      .sortBy(_.getName)
  }

  /** Removes stale manifests and data only when this writer still owns the latest publication. */
  private def cleanupStaleSnapshots(
      fs: FileSystem,
      rootPath: Path,
      keepManifestPath: Path): Unit = {
    try {
      // A concurrent writer may have published a newer manifest after this write completed.
      // In that case, avoid deleting anything and let the newer publication own cleanup.
      latestManifest(fs, rootPath).foreach { currentManifest =>
        if (currentManifest != keepManifestPath) {
          logWarning(
            s"Skipping cleanup for history snapshot root $rootPath because another manifest " +
              s"became active while publishing $keepManifestPath.")
          return
        }
      }
      val manifests = Option(fs.listStatus(rootPath)).getOrElse(Array.empty).map(_.getPath).toSeq
      staleManifestsForCleanup(manifests, keepManifestPath)
        .foreach(deleteSnapshotArtifacts(fs, _))
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to clean up stale history snapshots under $rootPath.", e)
    }
  }

  /** Deletes a manifest and its associated snapshot artifacts, tolerating partial corruption. */
  private def deleteSnapshotArtifacts(fs: FileSystem, manifestPath: Path): Unit = {
    val manifest = try {
      Some(loadManifest(fs, manifestPath))
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to load history snapshot manifest at $manifestPath for deletion.", e)
        None
    }

    manifest.foreach { properties =>
      val dataRoot = snapshotDataRoot(manifestPath, properties)
      deletePathQuietly(fs, dataRoot, recursive = true)
    }
    deletePathQuietly(fs, manifestPath, recursive = false)
    val rootPath = manifestPath.getParent
    if (fs.exists(rootPath) && fs.listStatus(rootPath).isEmpty) {
      Utils.tryLogNonFatalError {
        fs.delete(rootPath, true)
      }
    }
  }

  private def deletePathQuietly(fs: FileSystem, path: Path, recursive: Boolean): Unit = {
    Utils.tryLogNonFatalError {
      if (fs.exists(path)) {
        fs.delete(path, recursive)
      }
    }
  }

  /** Writes all records for one entity type into a single snapshot file. */
  private def writeEntityFile(
      store: KVStore,
      klass: Class[_],
      fs: FileSystem,
      path: Path,
      appId: String): Long = {
    singletonEntity(store, klass, appId)
      .map { value =>
        writeValuesFile(fs, path, Iterator.single(value))
      }
      .getOrElse {
        Utils.tryWithResource(store.view(klass.asInstanceOf[Class[AnyRef]]).closeableIterator()) {
          it =>
            writeValuesFile(fs, path,
              Iterator.continually(it).takeWhile(_.hasNext).map(_.next().asInstanceOf[AnyRef]))
        }
      }
  }

  /** Serializes a stream of KVStore values using length-delimited protobuf records. */
  private def writeValuesFile(
      fs: FileSystem,
      path: Path,
      values: Iterator[AnyRef]): Long = {
    var count = 0L
    Utils.tryWithResource(new DataOutputStream(new BufferedOutputStream(fs.create(path, true)))) {
      out =>
        while (values.hasNext) {
          val bytes = serializer.serialize(values.next())
          out.writeInt(bytes.length)
          out.write(bytes)
          count += 1
        }
    }
    count
  }

  /** Returns singleton snapshot entities whose keys are not discovered through a KVStore view. */
  private def singletonEntity(store: KVStore, klass: Class[_], appId: String): Option[AnyRef] = {
    def read(key: Any): Option[AnyRef] = {
      try {
        Some(store.read(klass.asInstanceOf[Class[AnyRef]], key))
      } catch {
        case _: NoSuchElementException => None
      }
    }

    if (klass == classOf[org.apache.spark.status.ApplicationInfoWrapper]) {
      read(appId)
    } else if (klass == classOf[ApplicationEnvironmentInfoWrapper]) {
      read(classOf[ApplicationEnvironmentInfoWrapper].getName)
    } else if (klass == classOf[AppSummary]) {
      read(classOf[AppSummary].getName)
    } else {
      None
    }
  }

  /** Restores one entity file by replaying its length-delimited protobuf records into the store. */
  private def readEntityFile(
      store: KVStore,
      klass: Class[_],
      fs: FileSystem,
      path: Path): Unit = {
    Utils.tryWithResource(new DataInputStream(new BufferedInputStream(fs.open(path)))) { in =>
      var done = false
      while (!done) {
        val size = try {
          in.readInt()
        } catch {
          case _: EOFException =>
            done = true
            -1
        }
        if (!done) {
          if (size < 0) {
            throw new IOException(s"Negative record size $size in snapshot file $path")
          }
          val bytes = new Array[Byte](size)
          in.readFully(bytes)
          store.write(serializer.deserialize(bytes, klass))
        }
      }
    }
  }
}
