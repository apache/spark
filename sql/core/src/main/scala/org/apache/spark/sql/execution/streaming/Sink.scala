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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * An interface for systems that can collect the results of a streaming query.
 *
 * When new data is produced by a query, a [[Sink]] must be able to transactionally collect the
 * data and update the [[StreamProgress]]. In the case of a failure, the sink will be recreated
 * and must be able to return the [[StreamProgress]] for all of the data that is made durable.
 * This contract allows Spark to process data with exactly-once semantics, even in the case
 * of failures that require the computation to be restarted.
 */
trait Sink {
  /**
   * Returns the [[StreamProgress]] for all data that is currently present in the sink. This
   * function will be called by Spark when restarting a stream in order to determine at which point
   * in streamed input data computation should be resumed from.
   */
  def currentProgress: StreamProgress

  /**
   * Accepts a new batch of data as well as a [[StreamProgress]] that denotes how far in the input
   * data computation has progressed to.  When computation restarts after a failure, it is important
   * that a [[Sink]] returns the same [[StreamProgress]] as the most recent batch of data that
   * has been persisted durrably.  Note that this does not necessarily have to be the
   * [[StreamProgress]] for the most recent batch of data that was given to the sink.  For example,
   * it is valid to buffer data before persisting, as long as the [[StreamProgress]] is stored
   * transactionally as data is eventually persisted.
   */
  def addBatch(currentState: StreamProgress, rdd: RDD[InternalRow]): Unit
}
