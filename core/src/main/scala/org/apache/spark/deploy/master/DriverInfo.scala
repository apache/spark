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

package org.apache.spark.deploy.master

import java.util.Date

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.deploy.DriverDescription
import org.apache.spark.util.Utils

private[spark] class DriverInfo(
    val startTime: Long,
    val id: String,
    val desc: DriverDescription,
    val submitDate: Date)
  extends Serializable {

  @transient var state: DriverState.Value = DriverState.SUBMITTED
  /* If we fail when launching the driver, the exception is stored here. */
  @transient var exception: Option[Exception] = None
  /* Most recent worker assigned to this driver */
  @transient var worker: Option[WorkerInfo] = None

  init()

  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    init()
  }

  private def init(): Unit = {
    state = DriverState.SUBMITTED
    worker = None
    exception = None
  }
}
