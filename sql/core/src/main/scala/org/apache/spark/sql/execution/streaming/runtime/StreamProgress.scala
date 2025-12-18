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

package org.apache.spark.sql.execution.streaming.runtime

import scala.collection.immutable

import org.apache.spark.sql.connector.read.streaming.{Offset => OffsetV2, SparkDataStream}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeq, OffsetSeqBase, OffsetSeqLog, OffsetSeqMetadata, OffsetSeqMetadataBase, OffsetSeqMetadataV2}

/**
 * A helper class that looks like a Map[Source, Offset].
 */
class StreamProgress(
    val baseMap: immutable.Map[SparkDataStream, OffsetV2] =
        new immutable.HashMap[SparkDataStream, OffsetV2])
  extends scala.collection.immutable.Map[SparkDataStream, OffsetV2] {

  /**
   * Unified method to convert StreamProgress to appropriate OffsetSeq format.
   * Handles both VERSION_1 (OffsetSeq) and VERSION_2 (OffsetMap) based on metadata version.
   */
  def toOffsets(
      sources: Seq[SparkDataStream],
      sourceIdMap: Map[String, SparkDataStream],
      metadata: OffsetSeqMetadataBase): OffsetSeqBase = {
    metadata.version match {
      case OffsetSeqLog.VERSION_1 =>
        toOffsetSeq(sources, metadata)
      case OffsetSeqLog.VERSION_2 =>
        toOffsetMap(sourceIdMap, metadata)
      case v =>
        throw QueryExecutionErrors.logVersionGreaterThanSupported(v, OffsetSeqLog.MAX_VERSION)
    }
  }

  def toOffsetSeq(
      source: Seq[SparkDataStream],
      metadata: OffsetSeqMetadataBase): OffsetSeq = {
    OffsetSeq(source.map(get), Some(metadata.asInstanceOf[OffsetSeqMetadata]))
  }

  private def toOffsetMap(
      sourceIdMap: Map[String, SparkDataStream],
      metadata: OffsetSeqMetadataBase): OffsetMap = {
    // Compute reverse mapping only when needed
    val sourceToIdMap = sourceIdMap.map(_.swap)
    val offsetsMap = baseMap.map { case (source, offset) =>
      val sourceId = sourceToIdMap.getOrElse(source,
        throw new IllegalArgumentException(s"Source $source not found in sourceToIdMap"))
      sourceId -> Some(offset)
    }
    // OffsetMap requires OffsetSeqMetadataV2
    OffsetMap(offsetsMap, metadata.asInstanceOf[OffsetSeqMetadataV2])
  }

  override def toString: String =
    baseMap.map { case (k, v) => s"$k: $v"}.mkString("{", ",", "}")

  override def updated[B1 >: OffsetV2](key: SparkDataStream, value: B1): Map[SparkDataStream, B1] =
    baseMap + (key -> value)

  override def get(key: SparkDataStream): Option[OffsetV2] = baseMap.get(key)

  override def iterator: Iterator[(SparkDataStream, OffsetV2)] = baseMap.iterator

  override def removed(key: SparkDataStream): Map[SparkDataStream, OffsetV2] = baseMap - key

  def ++(updates: IterableOnce[(SparkDataStream, OffsetV2)]): StreamProgress = {
    new StreamProgress(baseMap ++ updates)
  }
}
