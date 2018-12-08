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

package org.apache.spark.streaming.kafka010.mocks

import java.util.concurrent.TimeUnit

import scala.collection.mutable.PriorityQueue

import kafka.utils.Scheduler
import org.apache.kafka.common.utils.Time

/**
 * A mock scheduler that executes tasks synchronously using a mock time instance.
 * Tasks are executed synchronously when the time is advanced.
 * This class is meant to be used in conjunction with MockTime.
 *
 * Example usage
 * <code>
 *   val time = new MockTime
 *   time.scheduler.schedule("a task", println("hello world: " + time.milliseconds), delay = 1000)
 *   time.sleep(1001) // this should cause our scheduled task to fire
 * </code>
 *
 * Incrementing the time to the exact next execution time of a task will result in that task
 * executing (it as if execution itself takes no time).
 */
private[kafka010] class MockScheduler(val time: Time) extends Scheduler {

  /* a priority queue of tasks ordered by next execution time */
  var tasks = new PriorityQueue[MockTask]()

  def isStarted: Boolean = true

  def startup(): Unit = {}

  def shutdown(): Unit = synchronized {
    tasks.foreach(_.fun())
    tasks.clear()
  }

  /**
   * Check for any tasks that need to execute. Since this is a mock scheduler this check only occurs
   * when this method is called and the execution happens synchronously in the calling thread.
   * If you are using the scheduler associated with a MockTime instance this call
   * will be triggered automatically.
   */
  def tick(): Unit = synchronized {
    val now = time.milliseconds
    while(!tasks.isEmpty && tasks.head.nextExecution <= now) {
      /* pop and execute the task with the lowest next execution time */
      val curr = tasks.dequeue
      curr.fun()
      /* if the task is periodic, reschedule it and re-enqueue */
      if(curr.periodic) {
        curr.nextExecution += curr.period
        this.tasks += curr
      }
    }
  }

  def schedule(
      name: String,
      fun: () => Unit,
      delay: Long = 0,
      period: Long = -1,
      unit: TimeUnit = TimeUnit.MILLISECONDS): Unit = synchronized {
    tasks += MockTask(name, fun, time.milliseconds + delay, period = period)
    tick()
  }

}

case class MockTask(
    val name: String,
    val fun: () => Unit,
    var nextExecution: Long,
    val period: Long) extends Ordered[MockTask] {
  def periodic: Boolean = period >= 0
  def compare(t: MockTask): Int = {
    java.lang.Long.compare(t.nextExecution, nextExecution)
  }
}
