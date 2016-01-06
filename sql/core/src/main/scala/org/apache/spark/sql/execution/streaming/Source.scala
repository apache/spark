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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * A source of continually arriving data for a streaming query. A [[Source]] must have a
 * monotonically increasing notion of progress that can be represented as an [[Offset]]. Spark
 * will regularly query each [[Source]] for is current [[Offset]] in order to determine when new
 * data has arrived.
 */
trait Source  {

  /** Returns the schema of the data from this source */
  def schema: StructType

  /**
   * Returns the maximum offset that can be retrieved from the source.  This function will be called
   * only before attempting to start a new batch, in order to determine if new data is available.
   * Therefore it is acceptable to perform potentially expensive work when this function is called
   * that are required in-order to correctly replay sections of data when `getSlice` is called.  For
   * example, a [[Source]] might append to a write-ahead log in order to record what data is present
   * at a given [[Offset]].
   */
  def getCurrentOffset: Offset

  /**
   * Returns the data between the `start` and `end` offsets.  This function must always return
   * the same set of data for any given pair of offsets in order to guarantee exactly-once semantics
   * in the presence of failures.
   *
   * When `start` is [[None]], the stream should be replayed from the beginning. `getSlice` will
   * never be called with an `end` that is greater than the result of `getCurrentOffset`.
   */
  def getSlice(sqlContext: SQLContext, start: Option[Offset], end: Offset): RDD[InternalRow]
}
