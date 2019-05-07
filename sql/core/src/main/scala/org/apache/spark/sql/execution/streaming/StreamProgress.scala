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

import scala.collection.{immutable, GenTraversableOnce}

import org.apache.spark.sql.sources.v2.reader.streaming.SparkDataStream

/**
 * A helper class that looks like a Map[Source, Offset].
 */
class StreamProgress(
    val baseMap: immutable.Map[SparkDataStream, Offset] =
        new immutable.HashMap[SparkDataStream, Offset])
  extends scala.collection.immutable.Map[SparkDataStream, Offset] {

  def toOffsetSeq(source: Seq[SparkDataStream], metadata: OffsetSeqMetadata): OffsetSeq = {
    OffsetSeq(source.map(get), Some(metadata))
  }

  override def toString: String =
    baseMap.map { case (k, v) => s"$k: $v"}.mkString("{", ",", "}")

  override def +[B1 >: Offset](kv: (SparkDataStream, B1)): Map[SparkDataStream, B1] = {
    baseMap + kv
  }

  override def get(key: SparkDataStream): Option[Offset] = baseMap.get(key)

  override def iterator: Iterator[(SparkDataStream, Offset)] = baseMap.iterator

  override def -(key: SparkDataStream): Map[SparkDataStream, Offset] = baseMap - key

  def ++(updates: GenTraversableOnce[(SparkDataStream, Offset)]): StreamProgress = {
    new StreamProgress(baseMap ++ updates)
  }
}
