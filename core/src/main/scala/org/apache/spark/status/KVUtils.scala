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

package org.apache.spark.status

import java.io.File
import java.nio.file.Files

import scala.annotation.meta.getter
import scala.collection.JavaConverters._
import scala.reflect.{classTag, ClassTag}

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.fusesource.leveldbjni.internal.NativeDB
import org.rocksdb.RocksDBException

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.{FsHistoryProvider, FsHistoryProviderMetadata}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.History
import org.apache.spark.internal.config.History.HYBRID_STORE_DISK_BACKEND
import org.apache.spark.internal.config.History.HybridStoreDiskBackend
import org.apache.spark.internal.config.History.HybridStoreDiskBackend._
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore._

private[spark] object KVUtils extends Logging {

  /** Use this to annotate constructor params to be used as KVStore indices. */
  type KVIndexParam = KVIndex @getter

  private def backend(conf: SparkConf) =
    HybridStoreDiskBackend.withName(conf.get(HYBRID_STORE_DISK_BACKEND))

  /**
   * A KVStoreSerializer that provides Scala types serialization too, and uses the same options as
   * the API serializer.
   */
  private[spark] class KVStoreScalaSerializer extends KVStoreSerializer {

    mapper.registerModule(DefaultScalaModule)
    mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT)

  }

  /**
   * Open or create a disk-based KVStore.
   *
   * @param path Location of the store.
   * @param metadata Metadata value to compare to the data in the store. If the store does not
   *                 contain any metadata (e.g. it's a new store), this value is written as
   *                 the store's metadata.
   * @param conf SparkConf use to get `HYBRID_STORE_DISK_BACKEND`
   */
  def open[M: ClassTag](
      path: File,
      metadata: M,
      conf: SparkConf,
      diskBackend: Option[HybridStoreDiskBackend.Value] = None): KVStore = {
    require(metadata != null, "Metadata is required.")

    val db = diskBackend.getOrElse(backend(conf)) match {
      case LEVELDB => new LevelDB(path, new KVStoreScalaSerializer())
      case ROCKSDB => new RocksDB(path, new KVStoreScalaSerializer())
    }
    val dbMeta = db.getMetadata(classTag[M].runtimeClass)
    if (dbMeta == null) {
      db.setMetadata(metadata)
    } else if (dbMeta != metadata) {
      db.close()
      throw new MetadataMismatchException()
    }

    db
  }

  def createKVStore(
      storePath: Option[File],
      diskBackend: HybridStoreDiskBackend.Value,
      conf: SparkConf): KVStore = {
    storePath.map { path =>
      val dir = diskBackend match {
        case LEVELDB => "listing.ldb"
        case ROCKSDB => "listing.rdb"
      }

      val dbPath = Files.createDirectories(new File(path, dir).toPath()).toFile()
      Utils.chmod700(dbPath)

      val metadata = FsHistoryProviderMetadata(
        FsHistoryProvider.CURRENT_LISTING_VERSION,
        AppStatusStore.CURRENT_VERSION,
        conf.get(History.HISTORY_LOG_DIR))

      try {
        open(dbPath, metadata, conf, Some(diskBackend))
      } catch {
        // If there's an error, remove the listing database and any existing UI database
        // from the store directory, since it's extremely likely that they'll all contain
        // incompatible information.
        case _: UnsupportedStoreVersionException | _: MetadataMismatchException =>
          logInfo("Detected incompatible DB versions, deleting...")
          path.listFiles().foreach(Utils.deleteRecursively)
          open(dbPath, metadata, conf, Some(diskBackend))
        case dbExc @ (_: NativeDB.DBException | _: RocksDBException) =>
          // Get rid of the corrupted data and re-create it.
          logWarning(s"Failed to load disk store $dbPath :", dbExc)
          Utils.deleteRecursively(dbPath)
          open(dbPath, metadata, conf, Some(diskBackend))
      }
    }.getOrElse(new InMemoryStore())
  }

  /** Turns a KVStoreView into a Scala sequence, applying a filter. */
  def viewToSeq[T](
      view: KVStoreView[T],
      max: Int)
      (filter: T => Boolean): Seq[T] = {
    val iter = view.closeableIterator()
    try {
      iter.asScala.filter(filter).take(max).toList
    } finally {
      iter.close()
    }
  }

  /** Turns an interval of KVStoreView into a Scala sequence, applying a filter. */
  def viewToSeq[T](
      view: KVStoreView[T],
      from: Int,
      until: Int)(filter: T => Boolean): Seq[T] = {
    Utils.tryWithResource(view.closeableIterator()) { iter =>
      iter.asScala.filter(filter).slice(from, until).toList
    }
  }

  /** Turns a KVStoreView into a Scala sequence. */
  def viewToSeq[T](view: KVStoreView[T]): Seq[T] = {
    Utils.tryWithResource(view.closeableIterator()) { iter =>
      iter.asScala.toList
    }
  }

  /** Counts the number of elements in the KVStoreView which satisfy a predicate. */
  def count[T](view: KVStoreView[T])(countFunc: T => Boolean): Int = {
    Utils.tryWithResource(view.closeableIterator()) { iter =>
      iter.asScala.count(countFunc)
    }
  }

  /** Applies a function f to all values produced by KVStoreView. */
  def foreach[T](view: KVStoreView[T])(foreachFunc: T => Unit): Unit = {
    Utils.tryWithResource(view.closeableIterator()) { iter =>
      iter.asScala.foreach(foreachFunc)
    }
  }

  /** Maps all values of KVStoreView to new values using a transformation function. */
  def mapToSeq[T, B](view: KVStoreView[T])(mapFunc: T => B): Seq[B] = {
    Utils.tryWithResource(view.closeableIterator()) { iter =>
      iter.asScala.map(mapFunc).toList
    }
  }

  def size[T](view: KVStoreView[T]): Int = {
    Utils.tryWithResource(view.closeableIterator()) { iter =>
      iter.asScala.size
    }
  }

  private[spark] class MetadataMismatchException extends Exception

}
