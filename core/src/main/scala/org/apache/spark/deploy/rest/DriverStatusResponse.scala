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

package org.apache.spark.deploy.rest

/**
 * A response to the [[DriverStatusRequest]] in the REST application submission protocol.
 */
class DriverStatusResponse extends SubmitRestProtocolResponse {
  private val driverId = new SubmitRestProtocolField[String]("driverId")
  private val success = new SubmitRestProtocolField[Boolean]("success")
  // standalone cluster mode only
  private val driverState = new SubmitRestProtocolField[String]("driverState")
  private val workerId = new SubmitRestProtocolField[String]("workerId")
  private val workerHostPort = new SubmitRestProtocolField[String]("workerHostPort")

  def getDriverId: String = driverId.toString
  def getSuccess: String = success.toString
  def getDriverState: String = driverState.toString
  def getWorkerId: String = workerId.toString
  def getWorkerHostPort: String = workerHostPort.toString

  def setDriverId(s: String): this.type = setField(driverId, s)
  def setSuccess(s: String): this.type = setBooleanField(success, s)
  def setDriverState(s: String): this.type = setField(driverState, s)
  def setWorkerId(s: String): this.type = setField(workerId, s)
  def setWorkerHostPort(s: String): this.type = setField(workerHostPort, s)

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(driverId)
    assertFieldIsSet(success)
  }
}
