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

import org.apache.spark.internal.{config, Logging}
import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

/** Tracking the current state of the workers with available cores and assigned task list. */
class OfferState(val workOffer: WorkerOffer) {
  // The current remaining cores that can be allocated to tasks.
  var coresAvailable: Int = workOffer.cores
  // The list of tasks that are assigned to this worker.
  val tasks = new ArrayBuffer[TaskDescription](coresAvailable)
}

/**
 * TaskAssigner is the base class for all task assigner implementations, and can be
 * extended to implement different task scheduling algorithms.
 * Together with [[org.apache.spark.scheduler.TaskScheduler TaskScheduler]], TaskAssigner
 * is used to assign tasks to workers with available cores. Internally, TaskScheduler, requested
 * to perform task assignment given available workers, first sorts the candidate tasksets,
 * and then for each taskset, it takes a number of rounds to request TaskAssigner for task
 * assignment with different the locality restrictions until there is either no qualified
 * workers or no valid tasks to be assigned.
 *
 * TaskAssigner is responsible to maintain the worker availability state and task assignment
 * information. The contract between [[org.apache.spark.scheduler.TaskScheduler TaskScheduler]]
 * and TaskAssigner is as follows. First, TaskScheduler invokes construct() of TaskAssigner to
 * initialize the its internal worker states at the beginning of resource offering. Before each
 * round of task assignment for a taskset, TaskScheduler invoke the init() of TaskAssigner to
 * initialize the data structure for the round. When performing real task assignment,
 * hasNext()/getNext() is used by TaskScheduler to check the worker availability and retrieve
 * current offering from TaskAssigner. Then offerAccepted is used by TaskScheduler to notify
 * the TaskAssigner so that TaskAssigner can decide whether the current offer is valid or not for
 * the next request. After task assignment is done, TaskScheduler invokes the tasks() to
 * retrieve all the task assignment information, and eventually, invokes reset() method so that
 * TaskAssigner can cleanup its internal maintained resources.
 */

private[scheduler] abstract class TaskAssigner {
  var offer: Seq[OfferState] = _
  var CPUS_PER_TASK = 1

  def withCpuPerTask(CPUS_PER_TASK: Int): Unit = {
    this.CPUS_PER_TASK = CPUS_PER_TASK
  }

  // The final assigned offer returned to TaskScheduler.
  final def tasks: Seq[ArrayBuffer[TaskDescription]] = offer.map(_.tasks)

  // Invoked at the beginning of resource offering to construct the offer with the workoffers.
  def construct(workOffer: Seq[WorkerOffer]): Unit = {
    offer = workOffer.map(o => new OfferState(o))
  }

  // Invoked at each round of Taskset assignment to initialize the internal structure.
  def init(): Unit

  // Whether there is offer available to be used inside of one round of Taskset assignment.
  def hasNext: Boolean

  // Returned the next assigned offer based on the task assignment strategy.
  def getNext(): OfferState

  // Invoked by the TaskScheduler to indicate whether the current offer is accepted or not so that
  // the assigner can decide whether the current worker is valid for the next offering.
  def offerAccepted(assigned: Boolean): Unit

  // Invoked at the end of resource offering to release internally maintained resources.
  // Subclass is responsible to release its own private resources.
  def reset(): Unit = {
    offer = null
  }
}

object TaskAssigner extends Logging {
  private val roundrobin = classOf[RoundRobinAssigner].getCanonicalName
  private val packed = classOf[PackedAssigner].getCanonicalName
  private val balanced = classOf[BalancedAssigner].getCanonicalName
  private val assignerMap: Map[String, String] =
    Map("roundrobin" -> roundrobin,
      "packed" -> packed,
      "balanced" -> balanced)

  def init(conf: SparkConf): TaskAssigner = {
    val assignerName = conf.get(config.SPARK_SCHEDULER_TASK_ASSIGNER.key, "roundrobin")
      .toLowerCase()
    val className = assignerMap.getOrElse(assignerName, roundrobin)
    val CPUS_PER_TASK = conf.getInt("spark.task.cpus", 1)
    val assigner = try {
      logInfo(s"Constructing an assigner as $className")
      Utils.classForName(className).getConstructor()
        .newInstance().asInstanceOf[TaskAssigner]
    } catch {
      case _: Throwable =>
        logInfo(s"$assignerName cannot be constructed, fallback to default $roundrobin.")
        new RoundRobinAssigner()
    }
    assigner.withCpuPerTask(CPUS_PER_TASK)
    assigner
  }
}

/**
 * Assign the task to workers with available cores in roundrobin manner.
 */
class RoundRobinAssigner extends TaskAssigner {
  private var idx = 0

  override def construct(workOffer: Seq[WorkerOffer]): Unit = {
    offer = Random.shuffle(workOffer.map(o => new OfferState(o)))
  }

  override def init(): Unit = {
    idx = 0
  }

  override def hasNext: Boolean = idx < offer.size

  override def getNext(): OfferState = {
    offer(idx)
  }

  override def offerAccepted(assigned: Boolean): Unit = {
    idx += 1
  }

  override def reset(): Unit = {
    super.reset
    idx = 0
  }
}

/**
 * Assign the task to workers with the most available cores.
 */
class BalancedAssigner extends TaskAssigner {
  private var maxHeap: PriorityQueue[OfferState] = _
  private var currentOffer: OfferState = _

  override def construct(workOffer: Seq[WorkerOffer]): Unit = {
    offer = Random.shuffle(workOffer.map(o => new OfferState(o)))
  }

  implicit val ord: Ordering[OfferState] = new Ordering[OfferState] {
    def compare(x: OfferState, y: OfferState): Int = {
      return Ordering[Int].compare(x.coresAvailable, y.coresAvailable)
    }
  }

  override def init(): Unit = {
    maxHeap = new PriorityQueue[OfferState]()
    offer.filter(_.coresAvailable >= CPUS_PER_TASK).foreach(maxHeap.enqueue(_))
  }

  override def hasNext: Boolean = maxHeap.nonEmpty

  override def getNext(): OfferState = {
    currentOffer = maxHeap.dequeue()
    currentOffer
  }

  override def offerAccepted(assigned: Boolean): Unit = {
    if (currentOffer.coresAvailable >= CPUS_PER_TASK && assigned) {
      maxHeap.enqueue(currentOffer)
    }
  }

  override def reset(): Unit = {
    super.reset
    maxHeap = null
    currentOffer = null
  }
}

/**
 * Assign the task to workers with the least available cores. In other words, PackedAssigner tries
 * to schedule tasks to fewer workers.  As a result, there will be idle workers without any tasks
 * assigned if more than required workers are reserved. If the dynamic allocator is enabled,
 * these idle workers will be released by driver. The released resources can then be allocated to
 * other jobs by underling resource manager. This assigner can potentially reduce the resource
 * reservation for a job.
 */
class PackedAssigner extends TaskAssigner {
  private var sorted: Seq[OfferState] = _
  private var idx = 0
  private var currentOffer: OfferState = _

  override def init(): Unit = {
    idx = 0
    sorted = offer.filter(_.coresAvailable >= CPUS_PER_TASK).sortBy(_.coresAvailable)
  }

  override def hasNext: Boolean = idx < sorted.size

  override def getNext(): OfferState = {
    currentOffer = sorted(idx)
    currentOffer
  }

  override def offerAccepted(assigned: Boolean): Unit = {
    if (currentOffer.coresAvailable < CPUS_PER_TASK || !assigned) {
      idx += 1
    }
  }

  override def reset(): Unit = {
    super.reset
    sorted = null
    currentOffer = null
    idx = 0
  }
}
