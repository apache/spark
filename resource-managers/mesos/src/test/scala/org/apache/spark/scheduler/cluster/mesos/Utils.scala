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

package org.apache.spark.scheduler.cluster.mesos

import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.mesos.Protos._
import org.apache.mesos.Protos.Value.{Range => MesosRange, Ranges, Scalar}
import org.apache.mesos.SchedulerDriver
import org.apache.mesos.protobuf.ByteString
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{times, verify}
import org.scalatest.Assertions._

import org.apache.spark.deploy.mesos.config.MesosSecretConfig

object Utils {

  val TEST_FRAMEWORK_ID = FrameworkID.newBuilder()
    .setValue("test-framework-id")
    .build()

  val TEST_MASTER_INFO = MasterInfo.newBuilder()
    .setId("test-master")
    .setIp(0)
    .setPort(0)
    .build()

  def createOffer(
                   offerId: String,
                   agentId: String,
                   mem: Int,
                   cpus: Int,
                   ports: Option[(Long, Long)] = None,
                   gpus: Int = 0,
                   attributes: List[Attribute] = List.empty): Offer = {
    val builder = Offer.newBuilder()
    builder.addResourcesBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(mem))
    builder.addResourcesBuilder()
      .setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(cpus))
    ports.foreach { resourcePorts =>
      builder.addResourcesBuilder()
        .setName("ports")
        .setType(Value.Type.RANGES)
        .setRanges(Ranges.newBuilder().addRange(MesosRange.newBuilder()
        .setBegin(resourcePorts._1).setEnd(resourcePorts._2).build()))
    }
    if (gpus > 0) {
      builder.addResourcesBuilder()
        .setName("gpus")
        .setType(Value.Type.SCALAR)
        .setScalar(Scalar.newBuilder().setValue(gpus))
    }
    builder.setId(createOfferId(offerId))
      .setFrameworkId(FrameworkID.newBuilder()
      .setValue("f1"))
      .setSlaveId(SlaveID.newBuilder().setValue(agentId))
      .setHostname(s"host${agentId}")
      .addAllAttributes(attributes.asJava)
      .build()
  }

  def verifyTaskLaunched(driver: SchedulerDriver, offerId: String): List[TaskInfo] = {
    val captor = ArgumentCaptor.forClass(classOf[java.util.Collection[TaskInfo]])
    verify(driver, times(1)).launchTasks(
      meq(Collections.singleton(createOfferId(offerId))),
      captor.capture())
    captor.getValue.asScala.toList
  }

  def verifyTaskNotLaunched(driver: SchedulerDriver, offerId: String): Unit = {
    verify(driver, times(0)).launchTasks(
      meq(Collections.singleton(createOfferId(offerId))),
      any(classOf[java.util.Collection[TaskInfo]]))
  }

  def createOfferId(offerId: String): OfferID = {
    OfferID.newBuilder().setValue(offerId).build()
  }

  def createAgentId(agentId: String): SlaveID = {
    SlaveID.newBuilder().setValue(agentId).build()
  }

  def createExecutorId(executorId: String): ExecutorID = {
    ExecutorID.newBuilder().setValue(executorId).build()
  }

  def createTaskId(taskId: String): TaskID = {
    TaskID.newBuilder().setValue(taskId).build()
  }

  def configEnvBasedRefSecrets(secretConfig: MesosSecretConfig): Map[String, String] = {
    val secretName = "/path/to/secret,/anothersecret"
    val envKey = "SECRET_ENV_KEY,PASSWORD"
    Map(
      secretConfig.SECRET_NAMES.key -> secretName,
      secretConfig.SECRET_ENVKEYS.key -> envKey
    )
  }

  def verifyEnvBasedRefSecrets(launchedTasks: List[TaskInfo]): Unit = {
    val envVars = launchedTasks.head
      .getCommand
      .getEnvironment
      .getVariablesList
      .asScala
    assert(envVars
      .count(!_.getName.startsWith("SPARK_")) == 2) // user-defined secret env vars
    val variableOne = envVars.filter(_.getName == "SECRET_ENV_KEY").head
    assert(variableOne.getSecret.isInitialized)
    assert(variableOne.getSecret.getType == Secret.Type.REFERENCE)
    assert(variableOne.getSecret.getReference.getName == "/path/to/secret")
    assert(variableOne.getType == Environment.Variable.Type.SECRET)
    val variableTwo = envVars.filter(_.getName == "PASSWORD").head
    assert(variableTwo.getSecret.isInitialized)
    assert(variableTwo.getSecret.getType == Secret.Type.REFERENCE)
    assert(variableTwo.getSecret.getReference.getName == "/anothersecret")
    assert(variableTwo.getType == Environment.Variable.Type.SECRET)
  }

  def configEnvBasedValueSecrets(secretConfig: MesosSecretConfig): Map[String, String] = {
    val secretValues = "user,password"
    val envKeys = "USER,PASSWORD"
    Map(
      secretConfig.SECRET_VALUES.key -> secretValues,
      secretConfig.SECRET_ENVKEYS.key -> envKeys
    )
  }

  def verifyEnvBasedValueSecrets(launchedTasks: List[TaskInfo]): Unit = {
    val envVars = launchedTasks.head
      .getCommand
      .getEnvironment
      .getVariablesList
      .asScala
    assert(envVars
      .count(!_.getName.startsWith("SPARK_")) == 2) // user-defined secret env vars
    val variableOne = envVars.filter(_.getName == "USER").head
    assert(variableOne.getSecret.isInitialized)
    assert(variableOne.getSecret.getType == Secret.Type.VALUE)
    assert(variableOne.getSecret.getValue.getData ==
      ByteString.copyFrom("user".getBytes))
    assert(variableOne.getType == Environment.Variable.Type.SECRET)
    val variableTwo = envVars.filter(_.getName == "PASSWORD").head
    assert(variableTwo.getSecret.isInitialized)
    assert(variableTwo.getSecret.getType == Secret.Type.VALUE)
    assert(variableTwo.getSecret.getValue.getData ==
      ByteString.copyFrom("password".getBytes))
    assert(variableTwo.getType == Environment.Variable.Type.SECRET)
  }

  def configFileBasedRefSecrets(secretConfig: MesosSecretConfig): Map[String, String] = {
    val secretName = "/path/to/secret,/anothersecret"
    val secretPath = "/topsecret,/mypassword"
    Map(
      secretConfig.SECRET_NAMES.key -> secretName,
      secretConfig.SECRET_FILENAMES.key -> secretPath
    )
  }

  def verifyFileBasedRefSecrets(launchedTasks: List[TaskInfo]): Unit = {
    val volumes = launchedTasks.head.getContainer.getVolumesList
    assert(volumes.size() == 2)
    val secretVolOne = volumes.get(0)
    assert(secretVolOne.getContainerPath == "/topsecret")
    assert(secretVolOne.getSource.getSecret.getType == Secret.Type.REFERENCE)
    assert(secretVolOne.getSource.getSecret.getReference.getName == "/path/to/secret")
    val secretVolTwo = volumes.get(1)
    assert(secretVolTwo.getContainerPath == "/mypassword")
    assert(secretVolTwo.getSource.getSecret.getType == Secret.Type.REFERENCE)
    assert(secretVolTwo.getSource.getSecret.getReference.getName == "/anothersecret")
  }

  def configFileBasedValueSecrets(secretConfig: MesosSecretConfig): Map[String, String] = {
    val secretValues = "user,password"
    val secretPath = "/whoami,/mypassword"
    Map(
      secretConfig.SECRET_VALUES.key -> secretValues,
      secretConfig.SECRET_FILENAMES.key -> secretPath
    )
  }

  def verifyFileBasedValueSecrets(launchedTasks: List[TaskInfo]): Unit = {
    val volumes = launchedTasks.head.getContainer.getVolumesList
    assert(volumes.size() == 2)
    val secretVolOne = volumes.get(0)
    assert(secretVolOne.getContainerPath == "/whoami")
    assert(secretVolOne.getSource.getSecret.getType == Secret.Type.VALUE)
    assert(secretVolOne.getSource.getSecret.getValue.getData ==
      ByteString.copyFrom("user".getBytes))
    val secretVolTwo = volumes.get(1)
    assert(secretVolTwo.getContainerPath == "/mypassword")
    assert(secretVolTwo.getSource.getSecret.getType == Secret.Type.VALUE)
    assert(secretVolTwo.getSource.getSecret.getValue.getData ==
      ByteString.copyFrom("password".getBytes))
  }

  def createTextAttribute(name: String, value: String): Attribute = {
    Attribute.newBuilder()
      .setName(name)
      .setType(Value.Type.TEXT)
      .setText(Value.Text.newBuilder().setValue(value))
      .build()
  }
}
