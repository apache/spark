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

package org.apache.spark.sql.pipelines.graph

/**
 * Uncaught exception handler which first calls the delegate and then calls the
 * OnFailure function with the uncaught exception.
 */
class UncaughtExceptionHandler(
    delegate: Option[Thread.UncaughtExceptionHandler],
    onFailure: Throwable => Unit)
    extends Thread.UncaughtExceptionHandler {

  override def uncaughtException(t: Thread, e: Throwable): Unit = {
    try {
      delegate.foreach(_.uncaughtException(t, e))
    } finally {
      onFailure(e)
    }
  }
}

object UncaughtExceptionHandler {

  /**
   * Sets a handler which calls 'onFailure' function with the uncaught exception.
   * If the thread already has a uncaught exception handler, it will be called first
   * before calling the 'onFailure' function.
   */
  def addHandler(thread: Thread, onFailure: Throwable => Unit): Unit = {
    val currentHandler = Option(thread.getUncaughtExceptionHandler)
    thread.setUncaughtExceptionHandler(
      new UncaughtExceptionHandler(currentHandler, onFailure)
    )
  }
}
