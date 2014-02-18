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

import org.apache.spark.streaming.{Time, Duration, StreamingContext}

import scala.reflect.ClassTag

/**
 * This is the abstract base class for all input streams. This class provides methods
 * start() and stop() which is called by Spark Streaming system to start and stop receiving data.
 * Input streams that can generate RDDs from new data by running a service/thread only on
 * the driver node (that is, without running a receiver on worker nodes), can be
 * implemented by directly inheriting this InputDStream. For example,
 * FileInputDStream, a subclass of InputDStream, monitors a HDFS directory from the driver for
 * new files and generates RDDs with the new files. For implementing input streams
 * that requires running a receiver on the worker nodes, use
 * [[org.apache.spark.streaming.dstream.NetworkInputDStream]] as the parent class.
 *
 * @param ssc_ Streaming context that will execute this input stream
 */
abstract class InputDStream[T: ClassTag] (@transient ssc_ : StreamingContext)
  extends DStream[T](ssc_) {

  private[streaming] var lastValidTime: Time = null

  ssc.graph.addInputStream(this)

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
      // Time is valid, but check it it is more than lastValidTime
      if (lastValidTime != null && time < lastValidTime) {
        logWarning("isTimeValid called with " + time + " where as last valid time is " +
          lastValidTime)
      }
      lastValidTime = time
      true
    }
  }

  override def dependencies = List()

  override def slideDuration: Duration = {
    if (ssc == null) throw new Exception("ssc is null")
    if (ssc.graph.batchDuration == null) throw new Exception("batchDuration is null")
    ssc.graph.batchDuration
  }

  /** Method called to start receiving data. Subclasses must implement this method. */
  def start()

  /** Method called to stop receiving data. Subclasses must implement this method. */
  def stop()
}
