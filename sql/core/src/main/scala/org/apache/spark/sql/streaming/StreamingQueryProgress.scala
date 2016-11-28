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

package org.apache.spark.sql.streaming

import java.{util => ju}
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.jute.compiler.JLong
import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * Statistics about updates made to a stateful operators in a [[StreamingQuery]] in a trigger.
 */
@Experimental
class StateOperatorProgress private[sql](
    val numEntries: Long,
    val numUpdated: Long) {
  private[sql] def jsonValue: JValue = {
    ("numEntries" -> JInt(numEntries)) ~
    ("numUpdated" -> JInt(numUpdated))
  }
}

/**
 * :: Experimental ::
 * Used to report statistics about progress that has been made in the execution of a
 * [[StreamingQuery]]. Each event relates to processing done for a single trigger of the streaming
 * query. Events are emitted even when no new data is available to be processed.
 *
 * @param id A unique id of the query.
 * @param name Name of the query. This name is unique across all active queries.
 * @param timestamp Timestamp (ms) of the beginning of the trigger.
 * @param batchId A unique id for the current batch of data being processed.  Note that in the
 *                case of retries after a failure a given batchId my be executed more than once.
 *                Similarly, when there is no data to be processed, the batchId will not be
 *                incremented.
 * @param durationMs The amount of time taken to perform various operations in milliseconds.
 * @param currentWatermark The current event time watermark in milliseconds
 * @param stateOperators Information about operators in the query that store state.
 * @param sources detailed statistics on data being read from each of the streaming sources.
 * @since 2.1.0
 */
@Experimental
class StreamingQueryProgress private[sql](
  val id: UUID,
  val name: String,
  val timestamp: Long,
  val batchId: Long, // TODO: epoch?
  val durationMs: ju.Map[String, java.lang.Long],
  val currentWatermark: Long,
  val stateOperators: Array[StateOperatorProgress],
  val sources: Array[SourceProgress]) {

  /** The aggregate (across all sources) number of records processed in a trigger. */
  def numRecords: Long = sources.map(_.numRecords).sum

  /** The aggregate (across all sources) rate of data arriving. */
  def inputRecordsPerSecond: Double = sources.map(_.inputRecordsPerSecond).sum

  /** The aggregate (across all sources) rate at which Spark is processing data. */
  def processedRecordsPerSecond: Double = sources.map(_.processedRecordsPerSecond).sum

  /** The compact JSON representation of this status. */
  def json: String = compact(render(jsonValue))

  /** The pretty (i.e. indented) JSON representation of this status. */
  def prettyJson: String = pretty(render(jsonValue))

  override def toString: String = prettyJson

  private[sql] def jsonValue: JValue = {
    ("id" -> JString(id.toString)) ~
    ("name" -> JString(name)) ~
    ("timestamp" -> JInt(timestamp)) ~
    ("inputRecordsPerSecond" -> JDouble(inputRecordsPerSecond)) ~
    ("processedRecordsPerSecond" -> JDouble(processedRecordsPerSecond)) ~
    ("durationMs" -> durationMs
        .asScala
        .map { case (k, v) => k -> JInt(v.toLong): JObject }
        .reduce(_ ~ _)) ~
    ("currentWatermark" -> JInt(currentWatermark)) ~
    ("stateOperators" -> JArray(stateOperators.map(_.jsonValue).toList)) ~
    ("sources" -> JArray(sources.map(_.jsonValue).toList))
  }
}
