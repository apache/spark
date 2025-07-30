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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connector.read.streaming.{Offset => OffsetV2, SparkDataStream}
import org.apache.spark.sql.types.StructType

/**
 * A source of continually arriving data for a streaming query. A [[Source]] must have a
 * monotonically increasing notion of progress that can be represented as an [[Offset]]. Spark
 * will regularly query each [[Source]] to see if any more data is available.
 *
 * Note that, we extends `SparkDataStream` here, to make the v1 streaming source API be compatible
 * with data source v2.
 */
trait Source extends SparkDataStream {

  /** Returns the schema of the data from this source */
  def schema: StructType

  /**
   * Returns the maximum available offset for this source.
   * Returns `None` if this source has never received any data.
   */
  def getOffset: Option[Offset]

  /**
   * Returns the data that is between the offsets (`start`, `end`]. When `start` is `None`,
   * then the batch should begin with the first record. This method must always return the
   * same data for a particular `start` and `end` pair; even after the Source has been restarted
   * on a different node.
   *
   * Higher layers will always call this method with a value of `start` greater than or equal
   * to the last value passed to `commit` and a value of `end` less than or equal to the
   * last value returned by `getOffset`
   *
   * It is possible for the [[Offset]] type to be a [[SerializedOffset]] when it was
   * obtained from the log. Moreover, [[StreamExecution]] only compares the [[Offset]]
   * JSON representation to determine if the two objects are equal. This could have
   * ramifications when upgrading [[Offset]] JSON formats i.e., two equivalent [[Offset]]
   * objects could differ between version. Consequently, [[StreamExecution]] may call
   * this method with two such equivalent [[Offset]] objects. In which case, the [[Source]]
   * should return an empty [[DataFrame]]
   */
  def getBatch(start: Option[Offset], end: Offset): DataFrame

  /**
   * Informs the source that Spark has completed processing all data for offsets less than or
   * equal to `end` and will only request offsets greater than `end` in the future.
   */
  def commit(end: Offset) : Unit = {}

  override def initialOffset(): OffsetV2 = {
    throw new IllegalStateException("should not be called.")
  }

  override def deserializeOffset(json: String): OffsetV2 = {
    throw new IllegalStateException("should not be called.")
  }

  override def commit(end: OffsetV2): Unit = {
    throw new IllegalStateException("should not be called.")
  }
}
