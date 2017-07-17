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

package org.apache.spark.streaming.dstream

import java.util.Locale

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDDOperationScope
import org.apache.spark.streaming.{Duration, StreamingContext, Time}
import org.apache.spark.streaming.scheduler.RateController
import org.apache.spark.util.Utils

/**
 * This is the abstract base class for all input streams. This class provides methods
 * start() and stop() which are called by Spark Streaming system to start and stop
 * receiving data, respectively.
 * Input streams that can generate RDDs from new data by running a service/thread only on
 * the driver node (that is, without running a receiver on worker nodes), can be
 * implemented by directly inheriting this InputDStream. For example,
 * FileInputDStream, a subclass of InputDStream, monitors a HDFS directory from the driver for
 * new files and generates RDDs with the new files. For implementing input streams
 * that requires running a receiver on the worker nodes, use
 * [[org.apache.spark.streaming.dstream.ReceiverInputDStream]] as the parent class.
 *
 * @param _ssc Streaming context that will execute this input stream
 */
abstract class InputDStream[T: ClassTag](_ssc: StreamingContext)
  extends DStream[T](_ssc) {

  private[streaming] var lastValidTime: Time = null

  ssc.graph.addInputStream(this)

  /** This is an unique identifier for the input stream. */
  val id = ssc.getNewInputStreamId()

  // Keep track of the freshest rate for this stream using the rateEstimator
  protected[streaming] val rateController: Option[RateController] = None

  /** A human-readable name of this InputDStream */
  private[streaming] def name: String = {
    // e.g. FlumePollingDStream -> "Flume polling stream"
    val newName = Utils.getFormattedClassName(this)
      .replaceAll("InputDStream", "Stream")
      .split("(?=[A-Z])")
      .filter(_.nonEmpty)
      .mkString(" ")
      .toLowerCase(Locale.ROOT)
      .capitalize
    s"$newName [$id]"
  }

  /**
   * The base scope associated with the operation that created this DStream.
   *
   * For InputDStreams, we use the name of this DStream as the scope name.
   * If an outer scope is given, we assume that it includes an alternative name for this stream.
   */
  protected[streaming] override val baseScope: Option[String] = {
    val scopeName = Option(ssc.sc.getLocalProperty(SparkContext.RDD_SCOPE_KEY))
      .map { json => RDDOperationScope.fromJson(json).name + s" [$id]" }
      .getOrElse(name.toLowerCase(Locale.ROOT))
    Some(new RDDOperationScope(scopeName).toJson)
  }

  /**
   * Checks whether the 'time' is valid wrt slideDuration for generating RDD.
   * Additionally it also ensures valid times are in strictly increasing order.
   * This ensures that InputDStream.compute() is called strictly on increasing
   * times.
   */
  override private[streaming] def isTimeValid(time: Time): Boolean = {
    if (!super.isTimeValid(time)) {
      false // Time not valid
    } else {
      // Time is valid, but check it is more than lastValidTime
      if (lastValidTime != null && time < lastValidTime) {
        logWarning(s"isTimeValid called with $time whereas the last valid time " +
          s"is $lastValidTime")
      }
      lastValidTime = time
      true
    }
  }

  override def dependencies: List[DStream[_]] = List()

  override def slideDuration: Duration = {
    if (ssc == null) throw new Exception("ssc is null")
    if (ssc.graph.batchDuration == null) throw new Exception("batchDuration is null")
    ssc.graph.batchDuration
  }

  /** Method called to start receiving data. Subclasses must implement this method. */
  def start(): Unit

  /** Method called to stop receiving data. Subclasses must implement this method. */
  def stop(): Unit
}
