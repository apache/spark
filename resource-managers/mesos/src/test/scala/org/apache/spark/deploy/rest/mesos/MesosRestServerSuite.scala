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

package org.apache.spark.deploy.rest.mesos

import javax.servlet.http.HttpServletResponse

import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.TestPrematureExit
import org.apache.spark.deploy.mesos.MesosDriverDescription
import org.apache.spark.deploy.rest.{CreateSubmissionRequest, CreateSubmissionResponse, SubmitRestProtocolMessage, SubmitRestProtocolResponse}
import org.apache.spark.internal.config
import org.apache.spark.scheduler.cluster.mesos.{MesosClusterPersistenceEngineFactory, MesosClusterScheduler}

class MesosRestServerSuite extends SparkFunSuite
  with TestPrematureExit with MockitoSugar {

  test("test default driver overhead memory") {
    testOverheadMemory(new SparkConf(), "2000M", 2384)
  }

  test("test driver overhead memory with default overhead factor") {
    testOverheadMemory(new SparkConf(), "5000M", 5500)
  }

  test("test driver overhead memory with overhead factor") {
    val conf = new SparkConf()
    conf.set(config.DRIVER_MEMORY_OVERHEAD_FACTOR.key, "0.2")
    testOverheadMemory(conf, "5000M", 6000)
  }

  test("test configured driver overhead memory") {
    val conf = new SparkConf()
    conf.set(config.DRIVER_MEMORY_OVERHEAD.key, "1000")
    testOverheadMemory(conf, "2000M", 3000)
  }

  def testOverheadMemory(conf: SparkConf, driverMemory: String, expectedResult: Int): Unit = {
    conf.set("spark.master", "testmaster")
    conf.set("spark.app.name", "testapp")
    conf.set(config.DRIVER_MEMORY.key, driverMemory)
    var actualMem = 0
    class TestMesosClusterScheduler extends MesosClusterScheduler(
        mock[MesosClusterPersistenceEngineFactory], conf) {
      override def submitDriver(desc: MesosDriverDescription): CreateSubmissionResponse = {
        actualMem = desc.mem
        mock[CreateSubmissionResponse]
      }
    }

    class TestServlet extends MesosSubmitRequestServlet(new TestMesosClusterScheduler, conf) {
      override def handleSubmit(
          requestMessageJson: String,
          requestMessage: SubmitRestProtocolMessage,
          responseServlet: HttpServletResponse): SubmitRestProtocolResponse = {
        super.handleSubmit(requestMessageJson, requestMessage, responseServlet)
      }

      override def findUnknownFields(
          requestJson: String,
          requestMessage: SubmitRestProtocolMessage): Array[String] = {
        Array()
      }
    }
    val servlet = new TestServlet()
    val request = new CreateSubmissionRequest()
    request.appResource = "testresource"
    request.mainClass = "mainClass"
    request.appArgs = Array("appArgs")
    request.environmentVariables = Map("envVar" -> "envVal")
    request.sparkProperties = conf.getAll.toMap
    servlet.handleSubmit("null", request, null)
    assert(actualMem == expectedResult)
  }
}
