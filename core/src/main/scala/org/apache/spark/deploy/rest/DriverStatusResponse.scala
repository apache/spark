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

class DriverStatusResponse extends SubmitRestProtocolResponse {
  protected override val action = SubmitRestProtocolAction.DRIVER_STATUS_RESPONSE
  private val driverId = new SubmitRestProtocolField[String]
  private val success = new SubmitRestProtocolField[Boolean]
  private val driverState = new SubmitRestProtocolField[String]
  private val workerId = new SubmitRestProtocolField[String]
  private val workerHostPort = new SubmitRestProtocolField[String]

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

  override def validate(): Unit = {
    super.validate()
    assertFieldIsSet(driverId, "driver_id")
    assertFieldIsSet(success, "success")
  }
}
