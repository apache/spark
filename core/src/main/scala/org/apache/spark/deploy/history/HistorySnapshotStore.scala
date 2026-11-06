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

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream, IOException}
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.{Properties, UUID}

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.internal.config.History._
import org.apache.spark.internal.config.Status._
import org.apache.spark.status._
import org.apache.spark.status.api.v1
import org.apache.spark.status.protobuf.KVStoreProtobufSerializer
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.KVStore

/**
 * Writes and restores protobuf snapshots of an application's status store under
 * `spark.history.snapshot.path`, so the History Server can load a completed application without
 * replaying its event log. Snapshot files use the same permissions as event logs.
 */
private[spark] class HistorySnapshotStore(conf: SparkConf, hadoopConf: Configuration)
  extends Logging {

  import HistorySnapshotStore._

  def this(conf: SparkConf) = this(conf, SparkHadoopUtil.get.newConfiguration(conf))

  private val configuredRoot: Option[Path] =
    if (isEnabled(conf)) conf.get(SNAPSHOT_PATH).map(new Path(_)) else None

  // Resolved lazily so a misconfigured snapshot path does not affect application startup.
  private lazy val fs: FileSystem = configuredRoot.get.getFileSystem(hadoopConf)

  private lazy val rootPath: Option[Path] = configuredRoot.map(fs.makeQualified)

  private val serializer = new KVStoreProtobufSerializer()

  // Only read by the History Server, so an invalid value does not affect application startup.
  private lazy val maxRecordSize = conf.get(SNAPSHOT_MAX_RECORD_SIZE)

  /**
   * Writes a versioned history snapshot for an application attempt and returns the manifest path.
   *
   * The snapshot is published under the configured snapshot root using a fresh manifest and data
   * directory so readers can safely continue using the previous published snapshot while a new one
   * is being written.
   */
  def writeSnapshot(store: KVStore, appId: String, attemptId: Option[String]): Option[Path] = {
    snapshotDir(appId, attemptId).map { attemptDir =>
      mkdirs(attemptDir.getParent)
      mkdirs(attemptDir)
      val snapshotId = newSnapshotId()
      val dataDir = new Path(attemptDir, s"$SNAPSHOT_DIR_PREFIX$snapshotId")
      val manifestPath = new Path(attemptDir, s"$MANIFEST_FILE_PREFIX$snapshotId.properties")

      try {
        mkdirs(dataDir)
        val entries = writeEntityFiles(store, dataDir, appId)

        val manifest = new Properties()
        manifest.setProperty("schemaVersion", SNAPSHOT_SCHEMA_VERSION.toString)
        manifest.setProperty("snapshotDir", dataDir.getName)
        manifest.setProperty("classNames", entries.map(_.className).mkString(","))
        retentionLimits(conf).foreach { case (key, limit) =>
          manifest.setProperty(RETENTION_PREFIX + key, limit.toString)
        }
        entries.foreach { entry =>
          manifest.setProperty(s"${entry.className}.file", entry.fileName)
        }

        Utils.tryWithResource(create(manifestPath, overwrite = false)) { out =>
          manifest.store(out, "spark history snapshot")
        }
        // Only older manifests are deleted, so readers always see either the old snapshot or the
        // newly published one.
        cleanupStaleSnapshots(attemptDir, manifestPath)
        manifestPath
      } catch {
        case NonFatal(e) =>
          deletePathQuietly(manifestPath, recursive = false)
          deletePathQuietly(dataDir, recursive = true)
          throw e
      }
    }
  }

  /**
   * Returns the latest published history snapshot manifest for an application attempt, if any.
   *
   * When multiple manifests exist, the latest versioned manifest is treated as the active
   * published snapshot.
   */
  def findSnapshot(appId: String, attemptId: Option[String]): Option[Path] = {
    try {
      snapshotDir(appId, attemptId).flatMap(latestManifest)
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to look up history snapshot for ${MDC(APP_ID, appId)}.", e)
        None
    }
  }

  /**
   * Loads a published history snapshot into the provided KVStore.
   *
   * The manifest controls which entity files are restored and where the snapshot data directory is
   * located relative to the manifest path.
   */
  def restoreSnapshot(store: KVStore, manifestPath: Path): Unit = {
    val manifest = loadManifest(manifestPath)
    val schemaVersion = manifest.getProperty("schemaVersion", "-1").toInt
    require(schemaVersion == SNAPSHOT_SCHEMA_VERSION,
      s"Unsupported history snapshot version $schemaVersion at $manifestPath")
    checkRetention(manifestPath, manifest)

    val dataRoot = snapshotDataRoot(manifestPath, manifest)
    manifestEntries(manifestPath, manifest).foreach { entry =>
      loadSnapshotClass(manifestPath, entry.className).foreach { klass =>
        readEntityFile(store, klass, new Path(dataRoot, entry.fileName))
      }
    }
  }

  /**
   * Returns the total on-storage size of a published history snapshot, including its manifest.
   *
   * This is used by History Server disk-store allocation so the target local store can reserve
   * enough space before restoring a snapshot.
   */
  def snapshotSize(manifestPath: Path): Long = {
    val manifest = loadManifest(manifestPath)
    val root = snapshotDataRoot(manifestPath, manifest)
    manifestEntries(manifestPath, manifest)
      .foldLeft(fs.getFileStatus(manifestPath).getLen) { (total, entry) =>
        total + fs.getFileStatus(new Path(root, entry.fileName)).getLen
      }
  }

  /**
   * Deletes all snapshots of an application attempt. The History Server calls this when it cleans
   * up the attempt's event log, so snapshots do not outlive their event logs.
   */
  def deleteSnapshots(appId: String, attemptId: Option[String]): Unit = {
    Utils.tryLogNonFatalError {
      snapshotDir(appId, attemptId).foreach { attemptDir =>
        deletePathQuietly(attemptDir, recursive = true)
        val appDir = attemptDir.getParent
        if (fs.exists(appDir) && fs.listStatus(appDir).isEmpty) {
          fs.delete(appDir, false)
        }
      }
    }
  }

  /**
   * Returns the snapshot directory of an application attempt. The IDs come from event logs, and
   * Hadoop normalizes `.` and `..` components, so the result is checked to stay within the root.
   */
  private def snapshotDir(appId: String, attemptId: Option[String]): Option[Path] = {
    rootPath.map { root =>
      val dir = new Path(new Path(root, encode(appId)), encode(attemptId.getOrElse("_default_")))
      if (dir.getParent == null || dir.getParent.getParent != root) {
        throw new IllegalArgumentException(
          s"Snapshot path for app $appId / $attemptId escapes the snapshot directory")
      }
      dir
    }
  }

  /**
   * Checks that the snapshot retains at least as much UI data as this History Server keeps when
   * replaying the event log. The snapshot holds the application's already pruned status store,
   * so it cannot provide entries that the application's retention limits removed.
   */
  private def checkRetention(manifestPath: Path, manifest: Properties): Unit = {
    retentionLimits(conf).foreach { case (key, limit) =>
      Option(manifest.getProperty(RETENTION_PREFIX + key)).map(_.toLong).foreach { captured =>
        if (captured < limit) {
          throw new IOException(s"History snapshot $manifestPath was written with " +
            s"$key=$captured, which retains less than this server's $key=$limit")
        }
      }
    }
  }

  /** Reads and parses a snapshot manifest file. */
  private def loadManifest(manifestPath: Path): Properties = {
    val properties = new Properties()
    Utils.tryWithResource(fs.open(manifestPath)) { in =>
      properties.load(in)
    }
    properties
  }

  /** Returns the newest published manifest under a snapshot root, ordered by manifest name. */
  private def latestManifest(rootPath: Path): Option[Path] = {
    if (!fs.exists(rootPath)) {
      None
    } else {
      fs.listStatus(rootPath).map(_.getPath).filter(isVersionedManifest).sortBy(_.getName)
        .lastOption
    }
  }

  /** Removes stale manifests and data only when this writer still owns the latest publication. */
  private def cleanupStaleSnapshots(rootPath: Path, keepManifestPath: Path): Unit = {
    try {
      // A concurrent writer may have published a newer manifest after this write completed.
      // In that case, avoid deleting anything and let the newer publication own cleanup.
      if (latestManifest(rootPath).exists(_ != keepManifestPath)) {
        logWarning(log"Skipping cleanup for history snapshot root ${MDC(PATH, rootPath)} " +
          log"because another manifest became active while publishing " +
          log"${MDC(PATHS, keepManifestPath)}.")
      } else {
        val manifests = fs.listStatus(rootPath).map(_.getPath).toSeq
        staleManifestsForCleanup(manifests, keepManifestPath).foreach(deleteSnapshot)
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to clean up stale history snapshots under ${MDC(PATH, rootPath)}.",
          e)
    }
  }

  /** Deletes a manifest and its associated snapshot artifacts, tolerating partial corruption. */
  private def deleteSnapshot(manifestPath: Path): Unit = {
    try {
      deletePathQuietly(snapshotDataRoot(manifestPath, loadManifest(manifestPath)),
        recursive = true)
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to load history snapshot manifest at " +
          log"${MDC(PATH, manifestPath)} for deletion.", e)
    }
    deletePathQuietly(manifestPath, recursive = false)
  }

  private def mkdirs(dir: Path): Unit = {
    if (!FileSystem.mkdirs(fs, dir, EventLogFileWriter.LOG_FOLDER_PERMISSIONS)) {
      throw new IOException(s"Failed to create history snapshot directory $dir")
    }
  }

  private def create(path: Path, overwrite: Boolean): FSDataOutputStream = {
    val out = fs.create(path, overwrite)
    try {
      fs.setPermission(path, EventLogFileWriter.LOG_FILE_PERMISSIONS)
    } catch {
      case NonFatal(e) =>
        Utils.closeQuietly(out)
        throw e
    }
    out
  }

  private def deletePathQuietly(path: Path, recursive: Boolean): Unit = {
    Utils.tryLogNonFatalError {
      if (fs.exists(path)) {
        fs.delete(path, recursive)
      }
    }
  }

  /** Writes one file per entity type into the snapshot data directory. */
  private def writeEntityFiles(
      store: KVStore,
      dataDir: Path,
      appId: String): Seq[SnapshotEntry] = {
    snapshotEntityClasses.zipWithIndex.flatMap { case (klass, idx) =>
      val fileName = f"$idx%03d-${klass.getSimpleName}.pb"
      val path = new Path(dataDir, fileName)
      if (writeEntityFile(store, klass, path, appId) > 0L) {
        Some(SnapshotEntry(klass.getName, fileName))
      } else {
        fs.delete(path, false)
        None
      }
    }
  }

  /** Writes all records for one entity type into a single snapshot file. */
  private def writeEntityFile(
      store: KVStore,
      klass: Class[_],
      path: Path,
      appId: String): Long = {
    singletonEntity(store, klass, appId)
      .map { value =>
        writeValuesFile(path, Iterator.single(redact(value)))
      }
      .getOrElse {
        Utils.tryWithResource(store.view(klass.asInstanceOf[Class[AnyRef]]).closeableIterator()) {
          it =>
            writeValuesFile(path,
              Iterator.continually(it).takeWhile(_.hasNext).map(_.next().asInstanceOf[AnyRef]))
        }
      }
  }

  /** Serializes a stream of KVStore values using length-delimited protobuf records. */
  private def writeValuesFile(path: Path, values: Iterator[AnyRef]): Long = {
    var count = 0L
    Utils.tryWithResource(
        new DataOutputStream(new BufferedOutputStream(create(path, overwrite = true)))) { out =>
      while (values.hasNext) {
        val bytes = serializer.serialize(values.next())
        out.writeInt(bytes.length)
        out.write(bytes)
        count += 1
      }
    }
    count
  }

  /**
   * Redacts the application environment like the event log does. The live store keeps it
   * unredacted, but snapshots are written to shared storage and served by the History Server.
   */
  private def redact(value: AnyRef): AnyRef = value match {
    case env: ApplicationEnvironmentInfoWrapper =>
      val info = env.info
      new ApplicationEnvironmentInfoWrapper(new v1.ApplicationEnvironmentInfo(
        info.runtime,
        Utils.redact(conf, info.sparkProperties),
        Utils.redact(conf, info.hadoopProperties),
        Utils.redact(conf, info.systemProperties),
        Utils.redact(conf, info.metricsProperties),
        info.classpathEntries,
        info.resourceProfiles))
    case other => other
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

  /**
   * Restores one entity file by replaying its length-delimited protobuf records into the store.
   * Record sizes are checked against the remaining file length and the maximum record size before
   * allocating, so a corrupt size fails with an IOException rather than an OutOfMemoryError.
   */
  private def readEntityFile(store: KVStore, klass: Class[_], path: Path): Unit = {
    var remaining = fs.getFileStatus(path).getLen
    Utils.tryWithResource(new DataInputStream(new BufferedInputStream(fs.open(path)))) { in =>
      while (remaining > 0) {
        val size = in.readInt()
        remaining -= Integer.BYTES
        if (size < 0 || size > remaining) {
          throw new IOException(s"Invalid record size $size in snapshot file $path")
        }
        if (size > maxRecordSize) {
          throw new IOException(s"Record size $size in snapshot file $path exceeds " +
            s"${SNAPSHOT_MAX_RECORD_SIZE.key}=$maxRecordSize")
        }
        val bytes = new Array[Byte](size)
        in.readFully(bytes)
        remaining -= size
        store.write(serializer.deserialize(bytes, klass))
      }
    }
  }
}

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

  private val RETENTION_PREFIX = "retention."

  // Limits on how much UI data the status listeners retain. The SQL limits are looked up by key
  // because the SQL module may be absent; StaticSQLConf registers the streaming ones.
  private val CORE_RETENTION_CONFIGS: Seq[ConfigEntry[_]] = Seq(
    MAX_RETAINED_JOBS,
    MAX_RETAINED_STAGES,
    MAX_RETAINED_TASKS_PER_STAGE,
    MAX_RETAINED_DEAD_EXECUTORS,
    MAX_RETAINED_ROOT_NODES)

  private val SQL_RETENTION_CONFIG_KEYS = Seq(
    "spark.sql.ui.retainedExecutions",
    "spark.sql.streaming.ui.retainedQueries",
    "spark.sql.streaming.ui.retainedProgressUpdates")

  private lazy val retentionConfigs: Seq[ConfigEntry[_]] = {
    try {
      Utils.classForName("org.apache.spark.sql.internal.StaticSQLConf$", initialize = true)
    } catch {
      case _: ClassNotFoundException =>
    }
    CORE_RETENTION_CONFIGS ++
      SQL_RETENTION_CONFIG_KEYS.flatMap(key => Option(ConfigEntry.findEntry(key)))
  }

  private case class SnapshotEntry(className: String, fileName: String)

  def isEnabled(conf: SparkConf): Boolean = {
    conf.get(SNAPSHOT_ENABLED) && conf.get(SNAPSHOT_PATH).nonEmpty
  }

  /**
   * Returns whether an application attempt is complete enough to publish a history snapshot.
   *
   * Snapshots are only written for completed attempts so the published data is stable and can be
   * used by the History Server without replaying the event log.
   */
  def shouldWriteSnapshot(appInfo: v1.ApplicationInfo): Boolean = {
    appInfo != null && appInfo.attempts.lastOption.exists(_.completed)
  }

  /** Returns the effective UI retention limits of a configuration, keyed by config key. */
  private def retentionLimits(conf: SparkConf): Seq[(String, Long)] = {
    retentionConfigs.map(entry => entry.key -> conf.get(entry).toString.toLong)
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

  private def encode(value: String): String = {
    URLEncoder.encode(value, StandardCharsets.UTF_8)
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
      case _: ClassNotFoundException if SQL_ENTITY_CLASS_NAMES.contains(className) =>
        logWarning(log"Ignoring optional history snapshot class ${MDC(CLASS_NAME, className)} " +
          log"from ${MDC(PATH, manifestPath)} because it is not available on the classpath.")
        None
      case e: ClassNotFoundException =>
        throw new IOException(
          s"Required history snapshot class $className from $manifestPath could not be loaded.",
          e)
    }
  }

  private def isVersionedManifest(path: Path): Boolean = {
    val name = path.getName
    name.startsWith(MANIFEST_FILE_PREFIX) && name.endsWith(".properties")
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
}
