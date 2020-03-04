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

package org.apache.spark.deploy.yarn

import java.io.IOException

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.yarn.api.records.{ContainerLaunchContext, LocalResource}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import org.mockito.Mockito.{mock, when}
import org.scalatest.matchers.must.Matchers.{contain, not}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.network.util.JavaUtils

class ExecutorRunnableSuite extends SparkFunSuite {

  private def createExecutorRunnable(
      sparkConf: SparkConf = new SparkConf(),
      securityManager: SecurityManager = mock(classOf[SecurityManager])): ExecutorRunnable = {
    val executorCores = sparkConf.get(EXECUTOR_CORES)
    new ExecutorRunnable(
      None,
      new YarnConfiguration(),
      sparkConf,
      "yarn",
      "exec-1",
      "localhost",
      1,
      executorCores,
      "application_123_1",
      securityManager,
      Map.empty[String, LocalResource],
      0)
  }

  for (shuffleServerRecoveryDisabled <- Seq(true, false)) {
    test("validate service data when $shuffleServerRecoveryDisabled is " +
      shuffleServerRecoveryDisabled) {
      val sparkConf = new SparkConf()
      sparkConf.set(SHUFFLE_SERVER_RECOVERY_DISABLED, shuffleServerRecoveryDisabled)
      val securityManager = mock(classOf[SecurityManager])
      when(securityManager.getSecretKey()).thenReturn("secret")
      val execRunnable = createExecutorRunnable(sparkConf, securityManager)
      val ctx = Records.newRecord(classOf[ContainerLaunchContext])
        .asInstanceOf[ContainerLaunchContext]
      execRunnable.configureServiceData(ctx)
      val serviceName = sparkConf.get(SHUFFLE_SERVICE_NAME)
      val serviceData = ctx.getServiceData.get(serviceName)
      assert(serviceData != null)
      val payload: String = JavaUtils.bytesToString(serviceData)
      var metaInfo: java.util.Map[String, AnyRef] = null
      val secret = try {
        val mapper = new ObjectMapper
        metaInfo = mapper.readValue(payload,
          new TypeReference[java.util.Map[String, AnyRef]]() {})
        metaInfo.get(ExecutorRunnable.SECRET_KEY).asInstanceOf[String]
      } catch {
        case _: IOException =>
          payload
      }
      assert(secret equals "secret")
      if (shuffleServerRecoveryDisabled) {
        assert(metaInfo != null)
        val metadataStorageVal: Any = metaInfo.get(SHUFFLE_SERVER_RECOVERY_DISABLED.key)
        assert(metadataStorageVal != null && metadataStorageVal.asInstanceOf[Boolean])
      }
    }
  }

  test("if shuffle server recovery is disabled and authentication is disabled, then" +
    " service data should not contain secret") {
    val sparkConf = new SparkConf()
    sparkConf.set(SHUFFLE_SERVER_RECOVERY_DISABLED, true)
    val execRunnable = createExecutorRunnable(sparkConf)
    val ctx = Records.newRecord(classOf[ContainerLaunchContext])
      .asInstanceOf[ContainerLaunchContext]
    execRunnable.configureServiceData(ctx)
    val serviceName = sparkConf.get(SHUFFLE_SERVICE_NAME)
    val serviceData = ctx.getServiceData.get(serviceName)
    assert(serviceData != null)
    val payload: String = JavaUtils.bytesToString(serviceData)
    val mapper = new ObjectMapper
    val metaInfo = mapper.readValue(payload,
        new TypeReference[java.util.Map[String, AnyRef]]() {})
    assert(!metaInfo.containsKey(ExecutorRunnable.SECRET_KEY))
    val metadataStorageVal: Any = metaInfo.get(SHUFFLE_SERVER_RECOVERY_DISABLED.key)
    assert(metadataStorageVal != null && metadataStorageVal.asInstanceOf[Boolean])
  }

  test("SPARK-53209: ActiveProcessorCount not set by default") {
    val sparkConf = new SparkConf()
    val execRunnable = createExecutorRunnable(sparkConf)

    val commands = execRunnable.prepareCommand()
    commands should not contain ("-XX:ActiveProcessorCount=")
  }

  test("SPARK-53209: ActiveProcessorCount should default to 1 when executor cores not configured") {
    val sparkConf = new SparkConf()
        .set("spark.yarn.limitActiveProcessorCount", "true")
    val execRunnable = createExecutorRunnable(sparkConf)

    val commands = execRunnable.prepareCommand()
    commands should contain ("-XX:ActiveProcessorCount=1")
    commands should contain inOrderElementsOf List("--cores", "1")
  }

  test("SPARK-53209: ActiveProcessorCount should respect custom executor core count") {
    val sparkConf = new SparkConf()
      .set("spark.yarn.limitActiveProcessorCount", "true")
      .set(EXECUTOR_CORES, 7)
    val execRunnable = createExecutorRunnable(sparkConf)

    val commands = execRunnable.prepareCommand()
    commands should contain ("-XX:ActiveProcessorCount=7")
    commands should contain inOrderElementsOf List("--cores", "7")
  }
}
