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

import java.io.{IOException, NotSerializableException, ObjectInputStream, ObjectOutputStream}
import java.util.Properties

import scala.collection.mutable

import org.mockito.Matchers._
import org.mockito.Mockito._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RegisterExecutor, ReviveOffers}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.{RpcUtils, SerializableBuffer}

private[spark] class NotSerializablePartitionRDD(
    sc: SparkContext,
    numPartitions: Int) extends RDD[(Int, Int)](sc, Nil) with Serializable {

  override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] =
    throw new RuntimeException("should not be reached")

  override def getPartitions: Array[Partition] = (0 until numPartitions).map(i => new Partition {
    override def index: Int = i

    @throws(classOf[IOException])
    private def writeObject(out: ObjectOutputStream): Unit = {
      throw new NotSerializableException()
    }

    @throws(classOf[IOException])
    private def readObject(in: ObjectInputStream): Unit = {}
  }).toArray

  override def getPreferredLocations(partition: Partition): Seq[String] = Nil

  override def toString: String = "DAGSchedulerSuiteRDD " + id
}

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

  test("Scheduler aborts stages that have unserializable partition") {
    val conf = new SparkConf()
      .setMaster("local-cluster[2, 1, 1024]")
      .setAppName("test")
      .set("spark.dynamicAllocation.testing", "true")
    sc = new SparkContext(conf)
    val myRDD = new NotSerializablePartitionRDD(sc, 2)
    val e = intercept[SparkException] {
      myRDD.count()
    }
    assert(e.getMessage.contains("Failed to serialize task"))
    assertResult(10) {
      sc.parallelize(1 to 10).count()
    }
  }

  test("serialization task errors do not affect each other") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
    sc = new SparkContext(conf)
    val rpcEnv = sc.env.rpcEnv

    val endpointRef = mock(classOf[RpcEndpointRef])
    val mockAddress = mock(classOf[RpcAddress])
    when(endpointRef.address).thenReturn(mockAddress)
    val message = RegisterExecutor("1", endpointRef, "localhost", 4, Map.empty)

    val taskScheduler = mock(classOf[TaskSchedulerImpl])
    when(taskScheduler.CPUS_PER_TASK).thenReturn(1)
    when(taskScheduler.sc).thenReturn(sc)
    when(taskScheduler.mapOutputTracker).thenReturn(sc.env.mapOutputTracker)
    val taskIdToTaskSetManager = new mutable.HashMap[Long, TaskSetManager]
    when(taskScheduler.taskIdToTaskSetManager).thenReturn(taskIdToTaskSetManager)
    val dagScheduler = mock(classOf[DAGScheduler])
    when(taskScheduler.dagScheduler).thenReturn(dagScheduler)
    val taskSet1 = FakeTask.createTaskSet(1)
    val taskSet2 = FakeTask.createTaskSet(1)
    taskSet1.tasks(0) = new NotSerializableFakeTask(1, 0)

    def createTaskDescription(taskId: Long, task: Task[_]): TaskDescription = {
      new TaskDescription(
        taskId = 1L,
        attemptNumber = 0,
        executorId = "1",
        name = "localhost",
        index = 0,
        addedFiles = mutable.Map.empty[String, Long],
        addedJars = mutable.Map.empty[String, Long],
        properties = new Properties(),
        task = task)
    }

    when(taskScheduler.resourceOffers(any[IndexedSeq[WorkerOffer]])).thenReturn(Seq(Seq(
      createTaskDescription(1, taskSet1.tasks.head),
      createTaskDescription(2, taskSet2.tasks.head))))
    taskIdToTaskSetManager(1L) = new TaskSetManager(taskScheduler, taskSet1, 1)
    taskIdToTaskSetManager(2L) = new TaskSetManager(taskScheduler, taskSet2, 1)

    val backend = new CoarseGrainedSchedulerBackend(taskScheduler, rpcEnv)
    backend.start()
    backend.driverEndpoint.askWithRetry[Boolean](message)
    backend.driverEndpoint.askWithRetry[Boolean](ReviveOffers)
    assert(taskIdToTaskSetManager(1L).isZombie === true)
    assert(taskIdToTaskSetManager(2L).isZombie === false)
    backend.stop()
  }
}
