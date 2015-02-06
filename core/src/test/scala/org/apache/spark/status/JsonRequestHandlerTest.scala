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
package org.apache.spark.status

import org.apache.spark.JobExecutionStatus
import org.apache.spark.status.api.StageStatus
import org.scalatest.{Matchers, FunSuite}

class JsonRequestHandlerTest extends FunSuite with Matchers {

  test("extract stage status") {
    def f(x: Option[Array[String]]) = RouteUtils.extractParamSet(x, StageStatus.values())
    f(None) should be (Set(StageStatus.values(): _*))
    f(Some(Array[String]())) should be (Set())
    f(Some(Array[String]("failed"))) should be (Set(StageStatus.Failed))
    f(Some(Array[String]("failed,complete"))) should be (Set(StageStatus.Failed, StageStatus.Complete))
    f(Some(Array[String]("failed","complete"))) should be (
      Set(StageStatus.Failed, StageStatus.Complete))
    f(Some(Array[String]("failed","complete", "active"))) should be (
      Set(StageStatus.Failed, StageStatus.Complete, StageStatus.Active))

    intercept[IllegalArgumentException]{f(Some(Array[String]("foobar")))}.getMessage() should be (
      "unknown status: foobar")
  }

  test("extract job status") {
    def f(x: Option[Array[String]]) = RouteUtils.extractParamSet(x, JobExecutionStatus.values())

    f(None) should be (Set(JobExecutionStatus.values(): _*))
    f(Some(Array[String]())) should be (Set())
    f(Some(Array[String]("failed"))) should be (Set(JobExecutionStatus.FAILED))
    f(Some(Array[String]("failed,succeeded"))) should be (Set(JobExecutionStatus.FAILED, JobExecutionStatus.SUCCEEDED))
    f(Some(Array[String]("failed","succeeded"))) should be (
      Set(JobExecutionStatus.FAILED, JobExecutionStatus.SUCCEEDED))
    f(Some(Array[String]("failed","succeeded", "running"))) should be (
      Set(JobExecutionStatus.FAILED, JobExecutionStatus.RUNNING, JobExecutionStatus.SUCCEEDED))

    intercept[IllegalArgumentException]{f(Some(Array[String]("foobar")))}.getMessage() should be (
      "unknown status: foobar")

  }

}
