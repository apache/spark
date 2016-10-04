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

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.PriorityQueue
import scala.util.Random

import org.apache.spark.SparkConf

case class OfferState(workOffer: WorkerOffer, var cores: Int) {
  // Build a list of tasks to assign to each worker.
  val tasks = new ArrayBuffer[TaskDescription](cores)
}

abstract class TaskAssigner(conf: SparkConf) {

  var offer: Seq[OfferState] = _
  val CPUS_PER_TASK = conf.getInt("spark.task.cpus", 1)

  // The final assigned offer returned to TaskScheduler.
  def tasks(): Seq[ArrayBuffer[TaskDescription]] = offer.map(_.tasks)

  // construct the assigner by the workoffer.
  def construct(workOffer: Seq[WorkerOffer]): Unit = {
    offer = workOffer.map(o => OfferState(o, o.cores))
  }

  // Invoked in each round of Taskset assignment to initialize the internal structure.
  def init(): Unit

  // Indicating whether there is offer available to be used by one round of Taskset assignment.
  def hasNext(): Boolean

  // Next available offer returned to one round of Taskset assignment.
  def getNext(): OfferState

  // Called by the TaskScheduler to indicate whether the current offer is accepted
  // In order to decide whether the current is valid for the next offering.
  def taskAssigned(assigned: Boolean): Unit

  // Release internally maintained resources. Subclass is responsible to
  // release its own private resources.
  def reset: Unit = {
    offer = null
  }
}

class RoundRobinAssigner(conf: SparkConf) extends TaskAssigner(conf) {
  override def construct(workOffer: Seq[WorkerOffer]): Unit = {
    offer = Random.shuffle(workOffer.map(o => OfferState(o, o.cores)))
  }
  var i = 0
  def init(): Unit = {
    i = 0
  }
  def hasNext: Boolean = {
    i < offer.size
  }
  def getNext(): OfferState = {
    offer(i)
  }
  def taskAssigned(assigned: Boolean): Unit = {
    i += 1
  }
  override def reset: Unit = {
    super.reset
    i = 0
  }
}

class BalancedAssigner(conf: SparkConf) extends TaskAssigner(conf) {

  implicit val ord: Ordering[OfferState] = new Ordering[OfferState] {
    def compare(x: OfferState, y: OfferState): Int = {
      return Ordering[Int].compare(x.cores, y.cores)
    }
  }
  var maxHeap: PriorityQueue[OfferState] = _
  var current: OfferState = _
  def init(): Unit = {
    maxHeap = new PriorityQueue[OfferState]()
    offer.filter(_.cores >= CPUS_PER_TASK).foreach(maxHeap.enqueue(_))
  }
  def hasNext: Boolean = {
    maxHeap.size > 0
  }
  def getNext(): OfferState = {
    current = maxHeap.dequeue()
    current
  }

  def taskAssigned(assigned: Boolean): Unit = {
    if (current.cores > CPUS_PER_TASK && assigned) {
      maxHeap.enqueue(current)
    }
  }
  override def reset: Unit = {
    super.reset
    maxHeap = null
    current = null
  }
}

class PackedAssigner(conf: SparkConf) extends TaskAssigner(conf) {

  var sorted: Seq[OfferState] = _
  var i = 0
  var current: OfferState = _
  def init(): Unit = {
    i = 0
    sorted = offer.filter(_.cores >= CPUS_PER_TASK).sortBy(_.cores)
  }

  def hasNext: Boolean = {
    i < sorted.size
  }

  def getNext(): OfferState = {
    current = sorted(i)
    current
  }

  def taskAssigned(assigned: Boolean): Unit = {
    if (current.cores < CPUS_PER_TASK || !assigned) {
      i += 1
    }
  }

  override def reset: Unit = {
    super.reset
    sorted = null
    current = null
    i = 0
  }
}
