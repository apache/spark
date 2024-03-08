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

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.internal.Logging

/**
 * Class to notify of any errors that might have occurred out of band
 */
class ErrorNotifier extends Logging {

  private val error = new AtomicReference[Throwable]

  /** To indicate any errors that have occurred */
  def markError(th: Throwable): Unit = {
    logError("A fatal error has occurred.", th)
    error.set(th)
  }

  /** Get any errors that have occurred */
  def getError(): Option[Throwable] = {
    Option(error.get())
  }

  /** Throw errors that have occurred */
  def throwErrorIfExists(): Unit = {
    getError().foreach({th => throw th})
  }
}
