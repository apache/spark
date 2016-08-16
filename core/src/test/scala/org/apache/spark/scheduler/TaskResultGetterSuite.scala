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

import java.io.File
import java.net.URL
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal

import com.google.common.util.concurrent.MoreExecutors
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.{any, anyLong}
import org.mockito.Mockito.{spy, times, verify}
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually._

import org.apache.spark._
import org.apache.spark.storage.TaskResultBlockId
import org.apache.spark.TestUtils.JavaSourceFromString
import org.apache.spark.network.buffer.ChunkedByteBuffer
import org.apache.spark.util.{MutableURLClassLoader, RpcUtils, Utils}

/**
 * Removes the TaskResult from the BlockManager before delegating to a normal TaskResultGetter.
 *
 * Used to test the case where a BlockManager evicts the task result (or dies) before the
 * TaskResult is retrieved.
 */
private class ResultDeletingTaskResultGetter(sparkEnv: SparkEnv, scheduler: TaskSchedulerImpl)
  extends TaskResultGetter(sparkEnv, scheduler) {
  var removedResult = false

  @volatile var removeBlockSuccessfully = false

  override def enqueueSuccessfulTask(
    taskSetManager: TaskSetManager, tid: Long, serializedData: ChunkedByteBuffer) {
    if (!removedResult) {
      // Only remove the result once, since we'd like to test the case where the task eventually
      // succeeds.
      serializer.get().deserialize[TaskResult[_]](serializedData.duplicate()) match {
        case IndirectTaskResult(blockId, size) =>
          sparkEnv.blockManager.master.removeBlock(blockId)
          // removeBlock is asynchronous. Need to wait it's removed successfully
          try {
            eventually(timeout(3 seconds), interval(200 milliseconds)) {
              assert(!sparkEnv.blockManager.master.contains(blockId))
            }
            removeBlockSuccessfully = true
          } catch {
            case NonFatal(e) => removeBlockSuccessfully = false
          }
        case directResult: DirectTaskResult[_] =>
          taskSetManager.abort("Internal error: expect only indirect results")
      }
      removedResult = true
    }
    super.enqueueSuccessfulTask(taskSetManager, tid, serializedData)
  }
}


/**
 * A [[TaskResultGetter]] that stores the [[DirectTaskResult]]s it receives from executors
 * _before_ modifying the results in any way.
 */
private class MyTaskResultGetter(env: SparkEnv, scheduler: TaskSchedulerImpl)
  extends TaskResultGetter(env, scheduler) {

  // Use the current thread so we can access its results synchronously
  protected override val getTaskResultExecutor = MoreExecutors.sameThreadExecutor()

  // DirectTaskResults that we receive from the executors
  private val _taskResults = new ArrayBuffer[DirectTaskResult[_]]

  def taskResults: Seq[DirectTaskResult[_]] = _taskResults

  override def enqueueSuccessfulTask(tsm: TaskSetManager, tid: Long,
    data: ChunkedByteBuffer): Unit = {
    // work on a copy since the super class still needs to use the buffer
    val newBuffer = data.duplicate()
    _taskResults += env.closureSerializer.newInstance().deserialize[DirectTaskResult[_]](newBuffer)
    super.enqueueSuccessfulTask(tsm, tid, data)
  }
}


/**
 * Tests related to handling task results (both direct and indirect).
 */
class TaskResultGetterSuite extends SparkFunSuite with BeforeAndAfter with LocalSparkContext {

  // Set the RPC message size to be as small as possible (it must be an integer, so 1 is as small
  // as we can make it) so the tests don't take too long.
  def conf: SparkConf = new SparkConf().set("spark.rpc.message.maxSize", "1")

  test("handling results smaller than max RPC message size") {
    sc = new SparkContext("local", "test", conf)
    val result = sc.parallelize(Seq(1), 1).map(x => 2 * x).reduce((x, y) => x)
    assert(result === 2)
  }

  test("handling results larger than max RPC message size") {
    sc = new SparkContext("local", "test", conf)
    val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)
    val result =
      sc.parallelize(Seq(1), 1).map(x => 1.to(maxRpcMessageSize).toArray).reduce((x, y) => x)
    assert(result === 1.to(maxRpcMessageSize).toArray)

    val RESULT_BLOCK_ID = TaskResultBlockId(0)
    assert(sc.env.blockManager.master.getLocations(RESULT_BLOCK_ID).size === 0,
      "Expect result to be removed from the block manager.")
  }

  test("task retried if result missing from block manager") {
    // Set the maximum number of task failures to > 0, so that the task set isn't aborted
    // after the result is missing.
    sc = new SparkContext("local[1,2]", "test", conf)
    // If this test hangs, it's probably because no resource offers were made after the task
    // failed.
    val scheduler: TaskSchedulerImpl = sc.taskScheduler match {
      case taskScheduler: TaskSchedulerImpl =>
        taskScheduler
      case _ =>
        assert(false, "Expect local cluster to use TaskSchedulerImpl")
        throw new ClassCastException
    }
    val resultGetter = new ResultDeletingTaskResultGetter(sc.env, scheduler)
    scheduler.taskResultGetter = resultGetter
    val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)
    val result =
      sc.parallelize(Seq(1), 1).map(x => 1.to(maxRpcMessageSize).toArray).reduce((x, y) => x)
    assert(resultGetter.removeBlockSuccessfully)
    assert(result === 1.to(maxRpcMessageSize).toArray)

    // Make sure two tasks were run (one failed one, and a second retried one).
    assert(scheduler.nextTaskId.get() === 2)
  }

  /**
   * Make sure we are using the context classloader when deserializing failed TaskResults instead
   * of the Spark classloader.

   * This test compiles a jar containing an exception and tests that when it is thrown on the
   * executor, enqueueFailedTask can correctly deserialize the failure and identify the thrown
   * exception as the cause.

   * Before this fix, enqueueFailedTask would throw a ClassNotFoundException when deserializing
   * the exception, resulting in an UnknownReason for the TaskEndResult.
   */
  test("failed task deserialized with the correct classloader (SPARK-11195)") {
    // compile a small jar containing an exception that will be thrown on an executor.
    val tempDir = Utils.createTempDir()
    val srcDir = new File(tempDir, "repro/")
    srcDir.mkdirs()
    val excSource = new JavaSourceFromString(new File(srcDir, "MyException").getAbsolutePath,
      """package repro;
        |
        |public class MyException extends Exception {
        |}
      """.stripMargin)
    val excFile = TestUtils.createCompiledClass("MyException", srcDir, excSource, Seq.empty)
    val jarFile = new File(tempDir, "testJar-%s.jar".format(System.currentTimeMillis()))
    TestUtils.createJar(Seq(excFile), jarFile, directoryPrefix = Some("repro"))

    // ensure we reset the classloader after the test completes
    val originalClassLoader = Thread.currentThread.getContextClassLoader
    try {
      // load the exception from the jar
      val loader = new MutableURLClassLoader(new Array[URL](0), originalClassLoader)
      loader.addURL(jarFile.toURI.toURL)
      Thread.currentThread().setContextClassLoader(loader)
      val excClass: Class[_] = Utils.classForName("repro.MyException")

      // NOTE: we must run the cluster with "local" so that the executor can load the compiled
      // jar.
      sc = new SparkContext("local", "test", conf)
      val rdd = sc.parallelize(Seq(1), 1).map { _ =>
        val exc = excClass.newInstance().asInstanceOf[Exception]
        throw exc
      }

      // the driver should not have any problems resolving the exception class and determining
      // why the task failed.
      val exceptionMessage = intercept[SparkException] {
        rdd.collect()
      }.getMessage

      val expectedFailure = """(?s).*Lost task.*: repro.MyException.*""".r
      val unknownFailure = """(?s).*Lost task.*: UnknownReason.*""".r

      assert(expectedFailure.findFirstMatchIn(exceptionMessage).isDefined)
      assert(unknownFailure.findFirstMatchIn(exceptionMessage).isEmpty)
    } finally {
      Thread.currentThread.setContextClassLoader(originalClassLoader)
    }
  }

  test("task result size is set on the driver, not the executors") {
    import InternalAccumulator._

    // Set up custom TaskResultGetter and TaskSchedulerImpl spy
    sc = new SparkContext("local", "test", conf)
    val scheduler = sc.taskScheduler.asInstanceOf[TaskSchedulerImpl]
    val spyScheduler = spy(scheduler)
    val resultGetter = new MyTaskResultGetter(sc.env, spyScheduler)
    val newDAGScheduler = new DAGScheduler(sc, spyScheduler)
    scheduler.taskResultGetter = resultGetter
    sc.dagScheduler = newDAGScheduler
    sc.taskScheduler = spyScheduler
    sc.taskScheduler.setDAGScheduler(newDAGScheduler)

    // Just run 1 task and capture the corresponding DirectTaskResult
    sc.parallelize(1 to 1, 1).count()
    val captor = ArgumentCaptor.forClass(classOf[DirectTaskResult[_]])
    verify(spyScheduler, times(1)).handleSuccessfulTask(any(), anyLong(), captor.capture())

    // When a task finishes, the executor sends a serialized DirectTaskResult to the driver
    // without setting the result size so as to avoid serializing the result again. Instead,
    // the result size is set later in TaskResultGetter on the driver before passing the
    // DirectTaskResult on to TaskSchedulerImpl. In this test, we capture the DirectTaskResult
    // before and after the result size is set.
    assert(resultGetter.taskResults.size === 1)
    val resBefore = resultGetter.taskResults.head
    val resAfter = captor.getValue
    val resSizeBefore = resBefore.accumUpdates.find(_.name == Some(RESULT_SIZE)).map(_.value)
    val resSizeAfter = resAfter.accumUpdates.find(_.name == Some(RESULT_SIZE)).map(_.value)
    assert(resSizeBefore.exists(_ == 0L))
    assert(resSizeAfter.exists(_.toString.toLong > 0L))
  }

}

