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
package org.apache.spark.shuffle.parquet

import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import scala.util.{Success, Try}

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

import org.apache.spark._
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.parquet.avro.AvroPair
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.Utils

// Returned on shuffle registration, contains schema information for readers/writers
private[spark] class ParquetShuffleHandle[K, V, C](shuffleId: Int,
                                                   numMaps: Int,
                                                   dependency: ShuffleDependency[K, V, C],
                                                   val avroPairSchema: String)
  extends BaseShuffleHandle(shuffleId, numMaps, dependency)

private[spark] object ParquetShuffleManager extends Logging {

  def parquetShuffleCanBeUsed[K, V, C](shuffleId: Int,
                                       numMaps: Int,
                                       dependency: ShuffleDependency[K, V, C]): Option[Schema] = {

    def getSchema(className: String): Option[Schema] = {
      Try(Utils.classForName(className).newInstance().asInstanceOf[IndexedRecord]) match {
        case Success(indexedRecord) => Some(indexedRecord.getSchema)
        case _ => None
      }
    }

    getSchema(dependency.keyClassName) match {
      case None =>
        // Can't use Parquet, the key class has no schema
        None
      case Some(keySchema) =>
        if (dependency.mapSideCombine) {
          dependency.aggregator match {
            case None =>
              throw new AssertionError("Map-Side combine requested but no aggregator defined!")
            case Some(aggregator) =>
              dependency.combinerClassName.map(getSchema)
                .map(schema => AvroPair.makePairSchema(keySchema, schema.get))
          }
        } else {
          // We are *not* doing a map-side combine
          getSchema(dependency.valueClassName) match {
            case None =>
              // We can't use Parquet, the value class has no schema
              None
            case Some(valueSchema) =>
              // Parquet shuffle files will contain key and value class pairs
              Some(AvroPair.makePairSchema(keySchema, valueSchema))
          }
        }
    }
  }
}

private[spark] class ParquetShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {
  private val fileShuffleBlockManager = new FileShuffleBlockResolver(conf)
  private val fallbackManager = ParquetShuffleConfig.getFallbackShuffleManager(conf)
  private val fallbackShuffleIds =
    Collections.newSetFromMap(new ConcurrentHashMap[Int, java.lang.Boolean]())
  private val delegatingShuffleBlockResolver = new ShuffleBlockResolver {

    override def stop(): Unit = {
      fallbackManager.shuffleBlockResolver.stop()
      fileShuffleBlockManager.stop()
    }

    /**
     * Retrieve the data for the specified block. If the data for that block is not available,
     * throws an unspecified exception.
     */
    override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
      if (fallbackShuffleIds.contains(blockId.shuffleId)) {
        fallbackManager.shuffleBlockResolver.getBlockData(blockId)
      } else {
        fileShuffleBlockManager.getBlockData(blockId)
      }
    }
  }

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   */
  override def registerShuffle[K, V, C](shuffleId: Int,
                                        numMaps: Int,
                                        dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    // If Parquet is supported for this shuffle, use it
    ParquetShuffleManager.parquetShuffleCanBeUsed(shuffleId, numMaps, dependency) match {
      case Some(schema) =>
        new ParquetShuffleHandle(shuffleId, numMaps, dependency, schema.toString)
      case _ =>
        // ... otherwise, use the fallback shuffle manager
        fallbackShuffleIds.add(shuffleId)
        fallbackManager.registerShuffle(shuffleId, numMaps, dependency)
    }
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = delegatingShuffleBlockResolver.stop()

  /**
   * Remove a shuffle's metadata from the ShuffleManager.
   * @return true if the metadata removed successfully, otherwise false.
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    if (fallbackShuffleIds.remove(shuffleId)) {
      // Notify the fallback shuffle manager, if it was used for this shuffle
      fallbackManager.unregisterShuffle(shuffleId)
    } else {
      // Otherwise, remove it from the Parquet block resolver
      fileShuffleBlockManager.removeShuffle(shuffleId)
    }
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](handle: ShuffleHandle,
                               mapId: Int,
                               context: TaskContext): ShuffleWriter[K, V] = {
    handle match {
      case parquetHandle: ParquetShuffleHandle[K, V, _] =>
        new ParquetShuffleWriter[K, V](fileShuffleBlockManager, parquetHandle, mapId, context)
      case _ =>
        fallbackManager.getWriter(handle, mapId, context)
    }
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = delegatingShuffleBlockResolver

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  override def getReader[K, C](handle: ShuffleHandle,
                               startPartition: Int,
                               endPartition: Int,
                               context: TaskContext): ShuffleReader[K, C] = {
    handle match {
      case parquetHandle: ParquetShuffleHandle[K, _, C] =>
        new ParquetShuffleReader(parquetHandle, startPartition, endPartition, context)
      case _ =>
        fallbackManager.getReader(handle, startPartition, endPartition, context)
    }
  }
}
