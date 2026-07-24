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

import scala.collection.mutable
import scala.util.Random

/**
 * Decides the order in which the executor [[WorkerOffer]]s of a single resource-offer round are
 * considered when [[TaskSchedulerImpl]] launches tasks, and whether an offer should be revisited
 * after a task has (or has not) been launched on it. Different strategies produce different task
 * placement, e.g. concentrating tasks onto few executors versus spreading them evenly.
 *
 * Within [[TaskSchedulerImpl.resourceOffers]] the scheduler drives an assigner like this, once
 * per locality level of each task set:
 * {{{
 *   while (assigner.hasNext) {
 *     val i = assigner.next()         // index into the offers returned by prepare()
 *     val launched = tryToLaunchOn(i) // attempt to schedule a task on offers(i)
 *     assigner.taskLaunched(launched)
 *   }
 * }}}
 * A single instance is created per resource-offer round, and the strategy is selected via
 * [[internal.config.TASK_ASSIGNMENT_STRATEGY]].
 */
private[spark] trait TaskAssignmentStrategy {

  /**
   * Called once at the start of a resource-offer round. All subsequent indices produced
   * by `next()` refer to this input sequence.
   */
  def prepare(offers: Seq[WorkerOffer]): Unit

  /** Returns true while there is another offer index to visit in the current pass. */
  def hasNext: Boolean

  /** Returns the index of the offer to try next. */
  def next(): Int

  /**
   * Reports whether a task was launched on the offer returned by the most recent `next()`. Let
   * the strategy decide whether to advance past the current offer or revisit it.
   */
  def taskLaunched(launched: Boolean): Unit = {}
}

/**
 * @param index The index of the `prepare` input Seq[WorkerOffer]
 */
private case class WorkerOfferAndIndex(offer: WorkerOffer, index: Int)

/**
 * Visits every offer once, in the order they were prepared.
 */
private[spark] class SimpleAssignmentStrategy extends TaskAssignmentStrategy {
  private var preparedOffer: Seq[WorkerOffer] = _
  private var index = 0

  override def prepare(offers: Seq[WorkerOffer]): Unit = {
    preparedOffer = offers
  }

  override def hasNext: Boolean = index < preparedOffer.length

  override def next(): Int = {
    val originalIndex = index
    index = index + 1
    originalIndex
  }
}

/**
 * Visits every offer once,  but shuffles the offers in `prepare`.
 */
private[spark] class RoundRobinAssignmentStrategy extends TaskAssignmentStrategy {
  private var preparedOffer: IndexedSeq[WorkerOfferAndIndex] = _
  private var index = 0

  override def prepare(offers: Seq[WorkerOffer]): Unit = {
    val offerAndIndex = offers.indices.map(index => WorkerOfferAndIndex(offers(index), index))
    preparedOffer = Random.shuffle(offerAndIndex)
  }

  override def hasNext: Boolean = index < preparedOffer.length

  override def next(): Int = {
    val originalIndex = preparedOffer(index).index
    index = index + 1
    originalIndex
  }
}

/**
 * Orders offers by ascending free cores and keeps launching tasks on the current offer
 * until it can no longer accept one, only then advancing to the next. This concentrates a task
 * set onto as few executors as possible, which can help downstream executor decommissioning,
 * dynamic allocation reclaim idle executors.
 */
private[spark] class BinPackAssignmentStrategy extends TaskAssignmentStrategy {
  private var sortedOffer: IndexedSeq[WorkerOfferAndIndex] = _
  private var index = 0

  override def prepare(offers: Seq[WorkerOffer]): Unit = {
    sortedOffer = offers.indices.map(index => WorkerOfferAndIndex(offers(index), index))
      .sortBy(_.offer.cores)
  }

  override def hasNext: Boolean = index < sortedOffer.size

  override def next(): Int = sortedOffer(index).index

  override def taskLaunched(launched: Boolean): Unit = {
    // move to next work offer if current can not assign task
    if (!launched) {
      index = index + 1
    }
  }
}

/**
 * Keeps the offers in a priority queue keyed by free cores and always tries the offer with the
 * most free cores next, re-enqueuing it after a successful launch. This spreads a task set
 * as evenly as possible across the available executors to favor parallelism.
 */
private[spark] class BalanceAssignmentStrategy extends TaskAssignmentStrategy {
  private var preparedOffers: mutable.PriorityQueue[WorkerOfferAndIndex] = _
  private var currentOfferAndIndex: WorkerOfferAndIndex = _

  override def prepare(offers: Seq[WorkerOffer]): Unit = {
    implicit val ord: Ordering[WorkerOfferAndIndex] =
      (x: WorkerOfferAndIndex, y: WorkerOfferAndIndex) => {
        Ordering[Int].compare(x.offer.cores, y.offer.cores)
      }
    preparedOffers = new mutable.PriorityQueue[WorkerOfferAndIndex]()
    offers.indices.foreach { index =>
      preparedOffers.enqueue(WorkerOfferAndIndex(offers(index), index))
    }
  }

  override def hasNext: Boolean = preparedOffers.nonEmpty

  override def next(): Int = {
    val offerAndIndex = preparedOffers.dequeue()
    currentOfferAndIndex = offerAndIndex
    offerAndIndex.index
  }

  override def taskLaunched(launched: Boolean): Unit = {
    if (launched) {
      preparedOffers.enqueue(currentOfferAndIndex)
    }
  }
}

private[spark] object TaskAssignmentStrategy {
  private val ROUND_ROBIN = "roundrobin"
  private val BIN_PACK = "binpack"
  private val BALANCE = "balance"
  private val NONE = "none"

  def create(taskAssignmentStrategy: String): TaskAssignmentStrategy = {
    taskAssignmentStrategy match {
      case ROUND_ROBIN => new RoundRobinAssignmentStrategy
      case BIN_PACK => new BinPackAssignmentStrategy
      case BALANCE => new BalanceAssignmentStrategy
      case NONE => new SimpleAssignmentStrategy
      case unknown =>
        throw new IllegalArgumentException("Do not recognize task assigner: " + unknown)
    }
  }
}
