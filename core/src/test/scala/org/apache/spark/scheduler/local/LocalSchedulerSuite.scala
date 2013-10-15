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

package org.apache.spark.scheduler.local

import java.util.concurrent.Semaphore
import java.util.concurrent.CountDownLatch

import scala.collection.mutable.HashMap

import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark._


class Lock() {
  var finished = false
  def jobWait() = {
    synchronized {
      while(!finished) {
        this.wait()
      }
    }
  }

  def jobFinished() = {
    synchronized {
      finished = true
      this.notifyAll()
    }
  }
}

object TaskThreadInfo {
  val threadToLock = HashMap[Int, Lock]()
  val threadToRunning = HashMap[Int, Boolean]()
  val threadToStarted = HashMap[Int, CountDownLatch]()
}

/*
 * 1. each thread contains one job.
 * 2. each job contains one stage.
 * 3. each stage only contains one task.
 * 4. each task(launched) must be lanched orderly(using threadToStarted) to make sure
 *    it will get cpu core resource, and will wait to finished after user manually
 *    release "Lock" and then cluster will contain another free cpu cores.
 * 5. each task(pending) must use "sleep" to  make sure it has been added to taskSetManager queue,
 *    thus it will be scheduled later when cluster has free cpu cores.
 */
class LocalSchedulerSuite extends FunSuite with LocalSparkContext with BeforeAndAfterEach {

  override def afterEach() {
    super.afterEach()
    System.clearProperty("spark.scheduler.mode")
  }

  def createThread(threadIndex: Int, poolName: String, sc: SparkContext, sem: Semaphore) {

    TaskThreadInfo.threadToRunning(threadIndex) = false
    val nums = sc.parallelize(threadIndex to threadIndex, 1)
    TaskThreadInfo.threadToLock(threadIndex) = new Lock()
    TaskThreadInfo.threadToStarted(threadIndex) = new CountDownLatch(1)
    new Thread {
      if (poolName != null) {
        sc.setLocalProperty("spark.scheduler.pool", poolName)
      }
      override def run() {
        val ans = nums.map(number => {
          TaskThreadInfo.threadToRunning(number) = true
          TaskThreadInfo.threadToStarted(number).countDown()
          TaskThreadInfo.threadToLock(number).jobWait()
          TaskThreadInfo.threadToRunning(number) = false
          number
        }).collect()
        assert(ans.toList === List(threadIndex))
        sem.release()
      }
    }.start()
  }

  test("Local FIFO scheduler end-to-end test") {
    System.setProperty("spark.scheduler.mode", "FIFO")
    sc = new SparkContext("local[4]", "test")
    val sem = new Semaphore(0)

    createThread(1,null,sc,sem)
    TaskThreadInfo.threadToStarted(1).await()
    createThread(2,null,sc,sem)
    TaskThreadInfo.threadToStarted(2).await()
    createThread(3,null,sc,sem)
    TaskThreadInfo.threadToStarted(3).await()
    createThread(4,null,sc,sem)
    TaskThreadInfo.threadToStarted(4).await()
    // thread 5 and 6 (stage pending)must meet following two points
    // 1. stages (taskSetManager) of jobs in thread 5 and 6 should be add to taskSetManager
    //    queue before executing TaskThreadInfo.threadToLock(1).jobFinished()
    // 2. priority of stage in thread 5 should be prior to priority of stage in thread 6
    // So I just use "sleep" 1s here for each thread.
    // TODO: any better solution?
    createThread(5,null,sc,sem)
    Thread.sleep(1000)
    createThread(6,null,sc,sem)
    Thread.sleep(1000)

    assert(TaskThreadInfo.threadToRunning(1) === true)
    assert(TaskThreadInfo.threadToRunning(2) === true)
    assert(TaskThreadInfo.threadToRunning(3) === true)
    assert(TaskThreadInfo.threadToRunning(4) === true)
    assert(TaskThreadInfo.threadToRunning(5) === false)
    assert(TaskThreadInfo.threadToRunning(6) === false)

    TaskThreadInfo.threadToLock(1).jobFinished()
    TaskThreadInfo.threadToStarted(5).await()

    assert(TaskThreadInfo.threadToRunning(1) === false)
    assert(TaskThreadInfo.threadToRunning(2) === true)
    assert(TaskThreadInfo.threadToRunning(3) === true)
    assert(TaskThreadInfo.threadToRunning(4) === true)
    assert(TaskThreadInfo.threadToRunning(5) === true)
    assert(TaskThreadInfo.threadToRunning(6) === false)

    TaskThreadInfo.threadToLock(3).jobFinished()
    TaskThreadInfo.threadToStarted(6).await()

    assert(TaskThreadInfo.threadToRunning(1) === false)
    assert(TaskThreadInfo.threadToRunning(2) === true)
    assert(TaskThreadInfo.threadToRunning(3) === false)
    assert(TaskThreadInfo.threadToRunning(4) === true)
    assert(TaskThreadInfo.threadToRunning(5) === true)
    assert(TaskThreadInfo.threadToRunning(6) === true)

    TaskThreadInfo.threadToLock(2).jobFinished()
    TaskThreadInfo.threadToLock(4).jobFinished()
    TaskThreadInfo.threadToLock(5).jobFinished()
    TaskThreadInfo.threadToLock(6).jobFinished()
    sem.acquire(6)
  }

  test("Local fair scheduler end-to-end test") {
    System.setProperty("spark.scheduler.mode", "FAIR")
    val xmlPath = getClass.getClassLoader.getResource("fairscheduler.xml").getFile()
    System.setProperty("spark.scheduler.allocation.file", xmlPath)

    sc = new SparkContext("local[8]", "LocalSchedulerSuite")
    val sem = new Semaphore(0)

    createThread(10,"1",sc,sem)
    TaskThreadInfo.threadToStarted(10).await()
    createThread(20,"2",sc,sem)
    TaskThreadInfo.threadToStarted(20).await()
    createThread(30,"3",sc,sem)
    TaskThreadInfo.threadToStarted(30).await()

    assert(TaskThreadInfo.threadToRunning(10) === true)
    assert(TaskThreadInfo.threadToRunning(20) === true)
    assert(TaskThreadInfo.threadToRunning(30) === true)

    createThread(11,"1",sc,sem)
    TaskThreadInfo.threadToStarted(11).await()
    createThread(21,"2",sc,sem)
    TaskThreadInfo.threadToStarted(21).await()
    createThread(31,"3",sc,sem)
    TaskThreadInfo.threadToStarted(31).await()

    assert(TaskThreadInfo.threadToRunning(11) === true)
    assert(TaskThreadInfo.threadToRunning(21) === true)
    assert(TaskThreadInfo.threadToRunning(31) === true)

    createThread(12,"1",sc,sem)
    TaskThreadInfo.threadToStarted(12).await()
    createThread(22,"2",sc,sem)
    TaskThreadInfo.threadToStarted(22).await()
    createThread(32,"3",sc,sem)

    assert(TaskThreadInfo.threadToRunning(12) === true)
    assert(TaskThreadInfo.threadToRunning(22) === true)
    assert(TaskThreadInfo.threadToRunning(32) === false)

    TaskThreadInfo.threadToLock(10).jobFinished()
    TaskThreadInfo.threadToStarted(32).await()

    assert(TaskThreadInfo.threadToRunning(32) === true)

    //1. Similar with above scenario, sleep 1s for stage of 23 and 33 to be added to taskSetManager
    //   queue so that cluster will assign free cpu core to stage 23 after stage 11 finished.
    //2. priority of 23 and 33 will be meaningless as using fair scheduler here.
    createThread(23,"2",sc,sem)
    createThread(33,"3",sc,sem)
    Thread.sleep(1000)

    TaskThreadInfo.threadToLock(11).jobFinished()
    TaskThreadInfo.threadToStarted(23).await()

    assert(TaskThreadInfo.threadToRunning(23) === true)
    assert(TaskThreadInfo.threadToRunning(33) === false)

    TaskThreadInfo.threadToLock(12).jobFinished()
    TaskThreadInfo.threadToStarted(33).await()

    assert(TaskThreadInfo.threadToRunning(33) === true)

    TaskThreadInfo.threadToLock(20).jobFinished()
    TaskThreadInfo.threadToLock(21).jobFinished()
    TaskThreadInfo.threadToLock(22).jobFinished()
    TaskThreadInfo.threadToLock(23).jobFinished()
    TaskThreadInfo.threadToLock(30).jobFinished()
    TaskThreadInfo.threadToLock(31).jobFinished()
    TaskThreadInfo.threadToLock(32).jobFinished()
    TaskThreadInfo.threadToLock(33).jobFinished()

    sem.acquire(11)
  }
}
