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

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto.ExecutePlanResponse

case class CachedExecutePlanResponse(
    r: ExecutePlanResponse, // the actual response
    index: Long // index of the response in the response stream (starting from 1)
)

/**
 * Container for ExecutePlanResponses responses.
 *
 * This StreamObserver is running on the execution thread and saves the responses,
 * it notifies the ExecutePlanResponseSender about available responses.
 *
 * @param responseObserver
 */
private[connect] class ExecutePlanResponseObserver(var responseSender: ExecutePlanResponseSender)
    extends StreamObserver[ExecutePlanResponse] {

  // Cached stream state.
  private val responses = new scala.mutable.ListBuffer[CachedExecutePlanResponse]()
  private var error: Option[Throwable] = None
  private var completed: Boolean = false
  private var lastIndex: Option[Long] = None // index of last response before completed.
  private var index: Long = 0 // first response will have index 1

  def onNext(r: ExecutePlanResponse): Unit = synchronized {
    index += 1
    responses += CachedExecutePlanResponse(r, index)
    notifySender()
  }

  def onError(t: Throwable): Unit = synchronized {
    error = Some(t)
    lastIndex = Some(0) // no responses to be send after error.
    notifySender()
  }

  def onCompleted(): Unit = synchronized {
    lastIndex = Some(index)
    notifySender()
  }

  /** Set a new response sender. */
  def setExecutePlanResponseSender(var newSender) = synchronized {
    responseSender = newSender
    newSender.notify()
  }

  /** Remove cached responses until index */
  def removeUntilIndex(val index: Long) = synchronized {
    while (responses.nonEmpty && a(0).index <= index) {
      a.remove(0)
    }
  }

  def getResponse(val index: Long): Option[CachedExecutePlanResponse] = synchronized {
    if (responses.nonEmpty && index - responses(0).index < responses.size) {
      responses(index - responses(0).index)
    } else {
      None
    }
  }

  def getError() = synchronized {
    error
  }

  def getLastIndex() = synchronized {
    lastIndex
  }

  private def notifySender() = synchronized {
    responseSender.notify()
  }
}

/**
 * ExecutePlanResponseSender sends responses to the GRPC stream.
 * It runs on the RPC thread, and gets notified by ExecutePlanResponseObserver about available
 * responses.
 * It notifies the ExecutePlanResponseObserver back about cached responses that can be removed
 * after being sent out.
 * @param responseObserver the GRPC request StreamObserver
 */
private[connect] class ExecutePlanResponseSender(
  executionObserver: ExecutePlanResponseObserver,
  grpcObserver: StreamObserver[ExecutePlanResponse]) {

  private val signal = new Object

  def notify(): Unit = {
    signal.synchronized {
      signal.notify()
    }
  }

  def run(val lastSentIndex: Long) {
    // register to be notified about available responses.
    executionObserver.setExecutePlanResponseSender(this)

    var currentIndex = lastSentIndex + 1
    var finished = false

    // TODOTODOTODO - unfinished
    // do not want to block on grpcObserver.onXXX, while holding signal that can block exec
    // thread.
    do {
      signal.synchronized {
        error = executionObserver.getError()
        if (error.isEmpty) {
          response = executionObserver.getResponse(currentIndex)
        }

      if (error.isDefined()) {
        grpcObserver.onError(error.get)
        finished = true
      } else if (response.isDefined) {
        grpcObserver.onNext(response.r)
        currentIndex += 1
        if (getLastIndex().forall(currentIndex <= _)) {
          grpcObserver.onCompleted()
          finished = true
        }
      } else {
        // Nothing available, wait to be notified.
        signal.wait()
      }
    } while (!finished)
  }
}