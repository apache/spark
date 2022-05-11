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

package org.apache.spark.deploy.security.cloud

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.deploy.security.cloud.TestCloudCredentialsProvider.MOCK_CREDENTIALS_STRING
import org.apache.spark.internal.config.CLOUD_CREDENTIALS_SHARING
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend


class ExceptionThrowingCloudCredentialsProvider extends CloudCredentialsProvider {
  throw new IllegalArgumentException

  def serviceName: String = "throwerror"

  override def obtainCredentials(hadoopConf: Configuration, sparkConf: SparkConf)
  : Option[CloudCredentials] = throw new IllegalArgumentException
}

class TestCloudCredentialsProvider extends CloudCredentialsProvider {

  def serviceName: String = "test-cloud"


  override def obtainCredentials(hadoopConf: Configuration, sparkConf: SparkConf)
  : Option[CloudCredentials] = {
    Some(CloudCredentials(serviceName, MOCK_CREDENTIALS_STRING, Some(10660L)))
  }
}

object TestCloudCredentialsProvider {
  val MOCK_CREDENTIALS_STRING = "MOCK_CREDENTIALS_xyzzy"
}

private class TestCredentialsManagerClusterScheduler(scheduler: TaskSchedulerImpl, sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {
}

private class TestCredentialsManagerClusterManager extends ExternalClusterManager {

  def canCreate(masterURL: String): Boolean = {
    masterURL == "testCredentialsManagerClusterManager"
  }

  def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    new TestCredentialsManagerTaskScheduler(sc)
  }

  def createSchedulerBackend(sc: SparkContext,
                             masterURL: String,
                             scheduler: TaskScheduler): SchedulerBackend = {
    new TestCredentialsManagerSchedulerBackend(scheduler.asInstanceOf[TaskSchedulerImpl], sc)
  }

  def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TestCredentialsManagerTaskScheduler].initialized = true
    backend.asInstanceOf[TestCredentialsManagerSchedulerBackend].initialized = true
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }

}

private class TestCredentialsManagerTaskScheduler(sc: SparkContext) extends TaskSchedulerImpl(sc) {
  var initialized = false
}

private class TestCredentialsManagerSchedulerBackend(scheduler: TaskSchedulerImpl, sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {
  var initialized = false

  override def defaultParallelism(): Int = 1

  var credentialsManager: Option[CloudCredentialsManager] = None

  override protected def createCloudCredentialsManager(): Option[CloudCredentialsManager] = {
    credentialsManager = super.createCloudCredentialsManager()
    credentialsManager
  }

  def getFromConf(key: String): String = {
    conf.get(key)
  }

}

class CloudCredentialsManagerSuite extends SparkFunSuite {
  private val hadoopConf = new Configuration()

  test("default configuration") {
    val manager = new CloudCredentialsManager(new SparkConf(false), hadoopConf, null)
    assert(!manager.isProviderLoaded("throwerror"))
    assert(!manager.isProviderLoaded("test-cloud"))
  }

  test("enable credential provider") {
    val sparkConf = new SparkConf(false)
      .set(CLOUD_CREDENTIALS_SHARING.key, "true")
      .set("spark.security.cloud.credentials.test-cloud.enabled", "true")
      .set("spark.security.cloud.credentials.throwerror.enabled", "false")
    val manager = new CloudCredentialsManager(sparkConf, hadoopConf, null)
    assert(!manager.isProviderLoaded("throwerror"))
    assert(manager.isProviderLoaded("test-cloud"))
  }

  test("get credentials from manager") {
    val conf = new SparkConf()
      .setMaster("testCredentialsManagerClusterManager")
      .set(CLOUD_CREDENTIALS_SHARING.key, "true")
      .set("spark.security.cloud.credentials.test-cloud.enabled", "true")
      .set("spark.security.cloud.credentials.throwerror.enabled", "false")
      .setAppName("testCredentialsManager")
    val sc = new SparkContext(conf)
    val sb = sc.schedulerBackend.asInstanceOf[TestCredentialsManagerSchedulerBackend]

    try {
      sb.start()
      val cm = sb.credentialsManager.get
      assert(!cm.isProviderLoaded("throwerror"))
      assert(cm.isProviderLoaded("test-cloud"))
      val cloudCredentials = sb.getFromConf("spark.security.cloud.credentials.test-cloud")
      assert(cloudCredentials.equals(MOCK_CREDENTIALS_STRING))
    } finally {
      sb.stop()
      sc.stop()
    }
  }

}
