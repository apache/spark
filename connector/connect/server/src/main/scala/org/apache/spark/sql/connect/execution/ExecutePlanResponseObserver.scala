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

package org.apache.spark.sql.connect.execution

import scala.collection.mutable.ListBuffer

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto.ExecutePlanResponse
import org.apache.spark.internal.Logging

/**
 * Container for ExecutePlanResponses responses.
 *
 * This StreamObserver is running on the execution thread and saves the responses,
 * it notifies the ExecutePlanResponseSender about available responses.
 *
 * @param responseObserver
 */
private[connect] class ExecutePlanResponseObserver()
  extends StreamObserver[ExecutePlanResponse]
  with Logging {

  // Cached stream state.
  private val responses = new ListBuffer[CachedExecutePlanResponse]()
  private var error: Option[Throwable] = None
  private var completed: Boolean = false
  private var lastIndex: Option[Long] = None // index of last response before completed.
  private var index: Long = 0 // first response will have index 1

  // sender to notify of available responses.
  private var responseSender: Option[ExecutePlanResponseSender] = None

  def onNext(r: ExecutePlanResponse): Unit = synchronized {
    if (lastIndex.nonEmpty) {
      throw new IllegalStateException("Stream onNext can't be called after stream completed")
    }
    index += 1
    responses += CachedExecutePlanResponse(r, index)
    logDebug(s"Saved response with index=$index")
    notifyAll()
  }

  def onError(t: Throwable): Unit = synchronized {
    if (lastIndex.nonEmpty) {
      throw new IllegalStateException("Stream onError can't be called after stream completed")
    }
    error = Some(t)
    lastIndex = Some(index) // no responses to be send after error.
    logDebug(s"Error. Last stream index is $index.")
    notifyAll()
  }

  def onCompleted(): Unit = synchronized {
    if (lastIndex.nonEmpty) {
      throw new IllegalStateException("Stream onCompleted can't be called after stream completed")
    }
    lastIndex = Some(index)
    logDebug(s"Completed. Last stream index is $index.")
    notifyAll()
  }

  /** Set a new response sender. */
  def setExecutePlanResponseSender(newSender: ExecutePlanResponseSender): Unit = synchronized {
    responseSender.foreach(_.detach()) // detach the current sender before attaching new one
    responseSender = Some(newSender)
    notifyAll()
  }

  /** Remove cached responses until index */
  def removeUntilIndex(index: Long): Unit = synchronized {
    while (responses.nonEmpty && responses(0).index <= index) {
      responses.remove(0)
    }
    logDebug(s"Removed saved responses until index $index.")
  }

  /** Get response with a given index in the stream, if set. */
  def getResponse(index: Long): Option[CachedExecutePlanResponse] = synchronized {
    if (responses.nonEmpty && (index - responses(0).index).toInt < responses.size) {
      // Note: index access in ListBuffer is linear; we assume here the buffer is not too long.
      val ret = responses((index - responses(0).index).toInt)
      assert(ret.index == index)
      Some(ret)
    } else {
      None
    }
  }

  /** Get the stream error, if set.  */
  def getError(): Option[Throwable] = synchronized {
    error
  }

  /** If the stream is finished, the index of the last response, otherwise unset. */
  def getLastIndex(): Option[Long] = synchronized {
    lastIndex
  }
}
