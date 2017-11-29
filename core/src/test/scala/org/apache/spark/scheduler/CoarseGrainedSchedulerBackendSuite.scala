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

package org.apache.spark.scheduler


import scala.collection.{Map, mutable}
import scala.concurrent.Future

import org.mockito.Mockito.{mock, spy, when}

import org.apache.spark._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{KillExecutors, RegisterExecutor, RequestExecutors}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.{RpcUtils, SerializableBuffer}


class CoarseGrainedSchedulerBackendSuite extends SparkFunSuite with LocalSparkContext {

  test("serialized task larger than max RPC message size") {
    val conf = new SparkConf
    conf.set("spark.rpc.message.maxSize", "1")
    conf.set("spark.default.parallelism", "1")
    sc = new SparkContext("local-cluster[2, 1, 1024]", "test", conf)
    val frameSize = RpcUtils.maxMessageSizeBytes(sc.conf)
    val buffer = new SerializableBuffer(java.nio.ByteBuffer.allocate(2 * frameSize))
    val larger = sc.parallelize(Seq(buffer))
    val thrown = intercept[SparkException] {
      larger.collect()
    }
    assert(thrown.getMessage.contains("using broadcast variables for large values"))
    val smaller = sc.parallelize(1 to 4).collect()
    assert(smaller.size === 4)
  }

  test("verify executorAddress") {
    val executorId1 = "1"
    val executorId2 = "2"

    var scheduler: TaskSchedulerImpl = null
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("test")
      .set("spark.dynamicAllocation.testing", "true")
    sc = spy(new SparkContext(conf))
    scheduler = mock(classOf[TaskSchedulerImpl])
    when(sc.taskScheduler).thenReturn(scheduler)
    when(scheduler.sc).thenReturn(sc)
    // Set up a fake backend and cluster manager to simulate killing executors
    val rpcEnv = sc.env.rpcEnv
    val fakeClusterManager = new FakeClusterManager(rpcEnv)
    val fakeClusterManagerRef = rpcEnv.setupEndpoint("fake-cm", fakeClusterManager)
    val fakeSchedulerBackend = new CGFakeSchedulerBackend(scheduler, rpcEnv, fakeClusterManagerRef)
    when(sc.schedulerBackend).thenReturn(fakeSchedulerBackend)

    // Register fake executors with our fake scheduler backend
    // This is necessary because the backend refuses to kill executors it does not know about
    fakeSchedulerBackend.start()
    val dummyExecutorEndpoint1 = new FakeExecutorEndpoint(rpcEnv)
    val dummyExecutorEndpoint2 = new FakeExecutorEndpoint(rpcEnv)
    val dummyExecutorEndpointRef1 = rpcEnv.setupEndpoint("fake-executor-1", dummyExecutorEndpoint1)
    val dummyExecutorEndpointRef2 = rpcEnv.setupEndpoint("fake-executor-2", dummyExecutorEndpoint2)
    assert(dummyExecutorEndpointRef1.address == null)
    fakeSchedulerBackend.driverEndpoint.askSync[Boolean](
      RegisterExecutor(executorId1, dummyExecutorEndpointRef1, "1.2.3.4", 1, Map.empty))
    fakeSchedulerBackend.driverEndpoint.askSync[Boolean](
      RegisterExecutor(executorId2, dummyExecutorEndpointRef2, "1.2.3.5", 2, Map.empty))
    assert(fakeSchedulerBackend.getExecutorIds.sorted == Seq[String](executorId1, executorId2)
      .sorted)
  }
}

/*
 * Dummy RPC endpoint to simulate executors.
 */
private class FakeExecutorEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint {

  override def receive: PartialFunction[Any, Unit] = {
    case _ =>
  }
}

/*
 * Dummy scheduler backend to simulate executor allocation requests to the cluster manager.
 */
private class CGFakeSchedulerBackend(
                                    scheduler: TaskSchedulerImpl,
                                    rpcEnv: RpcEnv,
                                    clusterManagerEndpoint: RpcEndpointRef)
  extends CoarseGrainedSchedulerBackend(scheduler, rpcEnv) {

  protected override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    clusterManagerEndpoint.ask[Boolean](
      RequestExecutors(requestedTotal, localityAwareTasks, hostToLocalTaskCount, Set.empty[String]))
  }

  protected override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    clusterManagerEndpoint.ask[Boolean](KillExecutors(executorIds))
  }
}

/*
 * Dummy cluster manager to simulate responses to executor allocation requests.
 */
private class FakeClusterManager(override val rpcEnv: RpcEnv) extends RpcEndpoint {
  private var targetNumExecutors = 0
  private val executorIdsToKill = new mutable.HashSet[String]

  def getTargetNumExecutors: Int = targetNumExecutors
  def getExecutorIdsToKill: Set[String] = executorIdsToKill.toSet

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestExecutors(requestedTotal, _, _, _) =>
      targetNumExecutors = requestedTotal
      context.reply(true)
    case KillExecutors(executorIds) =>
      executorIdsToKill ++= executorIds
      context.reply(true)
  }
}
