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

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.util.Utils

/** Tracks the current state of the workers with available cores and assigned task list. */
private[scheduler] class OfferState(val workOffer: WorkerOffer) {
  /** The current remaining cores that can be allocated to tasks. */
  var coresAvailable: Int = workOffer.cores
  /** The list of tasks that are assigned to this WorkerOffer. */
  val tasks = new ArrayBuffer[TaskDescription](coresAvailable)

  def assignTask(task: TaskDescription, cpu: Int): Unit = {
    if (coresAvailable < cpu) {
      throw new SparkException(s"Available cores are less than cpu per task" +
        s" ($coresAvailable < $cpu)")
    }
    tasks += task
    coresAvailable -= cpu
  }
}

/**
 * TaskAssigner is the base class for all task assigner implementations, and can be
 * extended to implement different task scheduling algorithms.
 * Together with [[org.apache.spark.scheduler.TaskScheduler TaskScheduler]], TaskAssigner
 * is used to assign tasks to workers with available cores. Internally, when TaskScheduler
 * performs task assignment given available workers, it first sorts the candidate tasksets,
 * and then for each taskset, it takes multiple rounds to request TaskAssigner for task
 * assignment with different locality restrictions until there is either no qualified
 * workers or no valid tasks to be assigned.
 *
 * TaskAssigner is responsible to maintain the worker availability state and task assignment
 * information. The contract between [[org.apache.spark.scheduler.TaskScheduler TaskScheduler]]
 * and TaskAssigner is as follows.
 *
 * First, TaskScheduler invokes construct() of TaskAssigner to initialize the its internal
 * worker states at the beginning of resource offering.
 *
 * Second, before each round of task assignment for a taskset, TaskScheduler invokes the init()
 * of TaskAssigner to initialize the data structure for the round.
 *
 * Third, when performing real task assignment, hasNext/next() is used by TaskScheduler
 * to check the worker availability and retrieve current offering from TaskAssigner.
 *
 * Fourth, TaskScheduler calls offerAccepted() to notify the TaskAssigner so that
 * TaskAssigner can decide whether the current offer is valid or not for the next request.
 *
 * Fifth, after task assignment is done, TaskScheduler invokes the function tasks to
 * retrieve all the task assignment information.
 */

private[scheduler] sealed abstract class TaskAssigner {
  protected var offer: Seq[OfferState] = _
  protected var cpuPerTask = 1

  protected def withCpuPerTask(cpuPerTask: Int): TaskAssigner = {
    this.cpuPerTask = cpuPerTask
    this
  }

  /** The currently assigned offers. */
  final def tasks: Seq[ArrayBuffer[TaskDescription]] = offer.map(_.tasks)

  /**
   * Invoked at the beginning of resource offering to construct the offer with the workoffers.
   * By default, offers is randomly shuffled to avoid always placing tasks on the same set of
   * workers.
   */
  def construct(workOffer: Seq[WorkerOffer]): Unit = {
    offer = Random.shuffle(workOffer.map(o => new OfferState(o)))
  }

  /** Invoked at each round of Taskset assignment to initialize the internal structure. */
  def init(): Unit

  /**
   * Tests whether there is offer available to be used inside of one round of Taskset assignment.
   * @return  `true` if a subsequent call to `next` will yield an element,
   *          `false` otherwise.
   */
  def hasNext: Boolean

  /**
   * Produces next worker offer based on the task assignment strategy.
   * @return  the next available offer, if `hasNext` is `true`,
   *          undefined behavior otherwise.
   */
  def next(): OfferState

  /**
   * Invoked by the TaskScheduler to indicate whether the current offer is accepted or not so that
   * the assigner can decide whether the current worker is valid for the next offering.
   *
   * @param isAccepted whether TaskScheduler assigns a task to current offer.
   */
  def offerAccepted(isAccepted: Boolean): Unit
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
    val className = {
      val name = assignerMap.get(assignerName.toLowerCase())
      name.getOrElse {
        logWarning(s"$assignerName cannot be constructed, fallback to default $roundrobin.")
        roundrobin
      }
    }
    // The className is valid. No need to catch exceptions.
    logInfo(s"Constructing TaskAssigner as $className")
    Utils.classForName(className).getConstructor().newInstance().asInstanceOf[TaskAssigner]
      .withCpuPerTask(cpuPerTask = conf.getInt("spark.task.cpus", 1))
  }
}

/**
 * Assigns the task to workers with available cores in a roundrobin manner.
 */
class RoundRobinAssigner extends TaskAssigner {
  private var currentOfferIndex = 0

  override def init(): Unit = {
    currentOfferIndex = 0
  }

  override def hasNext: Boolean = currentOfferIndex < offer.size

  override def next(): OfferState = {
    offer(currentOfferIndex)
  }

  override def offerAccepted(isAccepted: Boolean): Unit = {
    currentOfferIndex += 1
  }
}

/**
 * Assigns the task to workers with the most available cores. In other words, BalancedAssigner tries
 * to distribute the task across workers in a balanced way. Potentially, it may alleviate the
 * workers' memory pressure as less tasks running on the same workers, which also indicates that
 * the task itself can make use of more computation resources, e.g., hyper-thread, across clusters.
 */
class BalancedAssigner extends TaskAssigner {
  implicit val ord: Ordering[OfferState] = new Ordering[OfferState] {
    def compare(x: OfferState, y: OfferState): Int = {
      return Ordering[Int].compare(x.coresAvailable, y.coresAvailable)
    }
  }
  private val maxHeap: PriorityQueue[OfferState] = new PriorityQueue[OfferState]()
  private var currentOffer: OfferState = _

  override def init(): Unit = {
    maxHeap.clear()
    offer.filter(_.coresAvailable >= cpuPerTask).foreach(maxHeap.enqueue(_))
  }

  override def hasNext: Boolean = maxHeap.nonEmpty

  override def next(): OfferState = {
    currentOffer = maxHeap.dequeue()
    currentOffer
  }

  override def offerAccepted(isAccepted: Boolean): Unit = {
    if (currentOffer.coresAvailable >= cpuPerTask && isAccepted) {
      maxHeap.enqueue(currentOffer)
    }
  }
}

/**
 * Assigns the task to workers with the least available cores. In other words, PackedAssigner tries
 * to schedule tasks to fewer workers.  As a result, there will be idle workers without any tasks
 * assigned if more than required workers are reserved. If the dynamic allocator is enabled,
 * these idle workers will be released by driver. The released resources can then be allocated to
 * other jobs by underling resource manager. This assigner can potentially reduce the resource
 * reservation for jobs, which over allocate resources than they need.
 */
class PackedAssigner extends TaskAssigner {
  private var sortedOffer: Seq[OfferState] = _
  private var currentOfferIndex = 0
  private var currentOffer: OfferState = _

  override def init(): Unit = {
    currentOfferIndex = 0
    sortedOffer = offer.filter(_.coresAvailable >= cpuPerTask).sortBy(_.coresAvailable)
  }

  override def hasNext: Boolean = currentOfferIndex < sortedOffer.size

  override def next(): OfferState = {
    currentOffer = sortedOffer(currentOfferIndex)
    currentOffer
  }

  override def offerAccepted(isAccepted: Boolean): Unit = {
    if (currentOffer.coresAvailable < cpuPerTask || !isAccepted) {
      currentOfferIndex += 1
    }
  }
}
