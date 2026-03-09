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
import java.util.Properties

import scala.collection.Iterator
import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.History._
import org.apache.spark.status._
import org.apache.spark.status.protobuf.KVStoreProtobufSerializer
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.KVStore

private[spark] object HistorySnapshotStore extends Logging {

  private val SnapshotSchemaVersion = 1
  private val ManifestFileName = "manifest.properties"

  private val BaseEntityClasses = Seq(
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
    classOf[StreamBlockData])

  private val OptionalEntityClassNames = Seq(
    "org.apache.spark.sql.execution.ui.SQLExecutionUIData",
    "org.apache.spark.sql.execution.ui.SparkPlanGraphWrapper",
    "org.apache.spark.sql.streaming.ui.StreamingQueryData",
    "org.apache.spark.sql.streaming.ui.StreamingQueryProgressWrapper")

  private val TaskEntityClasses = Seq(
    classOf[TaskDataWrapper],
    classOf[CachedQuantile])

  private val serializer = new KVStoreProtobufSerializer()

  private[history] def baseEntityClassNames: Seq[String] = BaseEntityClasses.map(_.getName)

  private[history] def taskEntityClassNames: Seq[String] = TaskEntityClasses.map(_.getName)

  private[history] def optionalEntityClassNames: Seq[String] = OptionalEntityClassNames

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

    val entries = snapshotEntityClasses(conf).zipWithIndex.flatMap { case (klass, idx) =>
      val fileName = f"$idx%03d-${klass.getSimpleName}.pb"
      val path = new Path(rootPath, fileName)
      val count = writeEntityFile(store, klass, fs, path, appId)
      if (count > 0L) {
        Some((klass.getName, fileName, count))
      } else {
        fs.delete(path, false)
        None
      }
    }

    val manifest = new Properties()
    manifest.setProperty("schemaVersion", SnapshotSchemaVersion.toString)
    manifest.setProperty("appId", appId)
    manifest.setProperty("attemptId", attemptId.getOrElse(""))
    manifest.setProperty("includeTasks", conf.get(SNAPSHOT_INCLUDE_TASKS).toString)
    manifest.setProperty("classNames", entries.map(_._1).mkString(","))
    entries.foreach { case (className, fileName, count) =>
      manifest.setProperty(s"$className.file", fileName)
      manifest.setProperty(s"$className.count", count.toString)
    }

    val manifestPath = new Path(rootPath, ManifestFileName)
    Utils.tryWithResource(fs.create(manifestPath, true)) { out =>
      manifest.store(out, "spark history snapshot")
    }
    Some(manifestPath)
  }

  def findSnapshot(
      conf: SparkConf,
      appId: String,
      attemptId: Option[String]): Option[Path] = {
    if (!isEnabled(conf)) {
      return None
    }

    snapshotDir(conf, appId, attemptId).flatMap { dir =>
      val manifest = new Path(dir, ManifestFileName)
      val fs = manifest.getFileSystem(SparkHadoopUtil.get.newConfiguration(conf))
      try {
        if (fs.exists(manifest)) Some(manifest) else None
      } catch {
        case NonFatal(e) =>
          logWarning(s"Failed to check history snapshot manifest at $manifest.", e)
          None
      }
    }
  }

  def manifestPath(
      conf: SparkConf,
      appId: String,
      attemptId: Option[String]): Option[Path] = {
    snapshotDir(conf, appId, attemptId).map(new Path(_, ManifestFileName))
  }

  def restoreSnapshot(conf: SparkConf, store: KVStore, manifestPath: Path): Unit = {
    val fs = manifestPath.getFileSystem(SparkHadoopUtil.get.newConfiguration(conf))
    val manifest = loadManifest(fs, manifestPath)
    val schemaVersion = manifest.getProperty("schemaVersion", "-1").toInt
    require(schemaVersion == SnapshotSchemaVersion,
      s"Unsupported history snapshot version $schemaVersion at $manifestPath")

    val root = manifestPath.getParent
    manifest.getProperty("classNames", "")
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .foreach { className =>
        loadClass(className).foreach { klass =>
          val fileName = manifest.getProperty(s"$className.file")
          if (fileName != null) {
            readEntityFile(store, klass, fs, new Path(root, fileName))
          }
        }
      }
  }

  def snapshotSize(conf: SparkConf, manifestPath: Path): Long = {
    val fs = manifestPath.getFileSystem(SparkHadoopUtil.get.newConfiguration(conf))
    val manifest = loadManifest(fs, manifestPath)
    val root = manifestPath.getParent
    val manifestSize = fs.getFileStatus(manifestPath).getLen
    manifest.getProperty("classNames", "")
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .foldLeft(manifestSize) { (total, className) =>
        Option(manifest.getProperty(s"$className.file")).map { fileName =>
          total + fs.getFileStatus(new Path(root, fileName)).getLen
        }.getOrElse(total)
      }
  }

  private def isEnabled(conf: SparkConf): Boolean = {
    conf.get(SNAPSHOT_ENABLED) && conf.get(SNAPSHOT_PATH).nonEmpty
  }

  private def snapshotEntityClasses(conf: SparkConf): Seq[Class[_]] = {
    val baseClasses = if (conf.get(SNAPSHOT_INCLUDE_TASKS)) {
      BaseEntityClasses ++ TaskEntityClasses
    } else {
      BaseEntityClasses
    }
    baseClasses ++ OptionalEntityClassNames.flatMap(loadClass)
  }

  private def loadClass(className: String): Option[Class[_]] = {
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

  private def loadManifest(fs: FileSystem, manifestPath: Path): Properties = {
    val properties = new Properties()
    Utils.tryWithResource(fs.open(manifestPath)) { in =>
      properties.load(in)
    }
    properties
  }

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

  def writeSnapshotQuietly(
      conf: SparkConf,
      store: KVStore,
      appId: String,
      attemptId: Option[String]): Unit = {
    try {
      writeSnapshot(conf, store, appId, attemptId)
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to write history snapshot for $appId/${attemptId.getOrElse("")}.", e)
    }
  }
}
