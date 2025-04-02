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

import scala.collection.immutable

import org.apache.spark.sql.connector.read.streaming.{Offset => OffsetV2, SparkDataStream}

/**
 * A helper class that looks like a Map[Source, Offset].
 */
class StreamProgress(
    val baseMap: immutable.Map[SparkDataStream, OffsetV2] =
        new immutable.HashMap[SparkDataStream, OffsetV2])
  extends scala.collection.immutable.Map[SparkDataStream, OffsetV2] {

  def toOffsetSeq(source: Seq[SparkDataStream], metadata: OffsetSeqMetadata): OffsetSeq = {
    OffsetSeq(source.map(get), Some(metadata))
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
