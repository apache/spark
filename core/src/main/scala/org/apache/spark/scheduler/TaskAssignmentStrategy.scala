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
 * Lifecycle: a single instance is created per [[TaskSchedulerImpl.resourceOffers]] round and shared
 * across all task sets and locality levels of that round. [[prepare]] is called once at the start
 * of the round with the offers and the live `availableCpus` array; [[reset]] is called at the start
 * of every `resourceOfferSingleTaskSet` invocation (i.e. per task set x locality level x scheduling
 * pass) to restart the iteration cursor. The scheduler drives the assignment strategy like this:
 * {{{
 *   assignmentStrategy.prepare(offers, availableCpus)  // once per resource-offer round
 *   for (taskSet <- sortedTaskSets; locality <- localityLevels) {
 *     assignmentStrategy.reset()                        // once per resourceOfferSingleTaskSet pass
 *     while (assignmentStrategy.hasNext) {
 *       val i = assignmentStrategy.next()               // index into the prepare() offers
 *       val launched = tryToLaunchOn(i)                 // attempt to schedule a task on offers(i)
 *       assignmentStrategy.taskLaunched(launched)
 *     }
 *   }
 * }}}
 * `availableCpus` is drained in place by the scheduler as tasks launch (and reverted on the barrier
 * partial-launch path), so strategies that order by free cores read the live values on each
 * [[reset]] rather than a stale snapshot. The strategy is selected via
 * [[internal.config.TASK_ASSIGNMENT_STRATEGY]].
 */
private[spark] trait TaskAssignmentStrategy {

  /**
   * Called once at the start of a resource-offer round. All subsequent indices produced by
   * `next()` refer to `offers`. `availableCpus` is the live per-offer free-cpu array (index `i`
   * corresponds to `offers(i)`) that the scheduler mutates as the round proceeds; strategies that
   * order by free cores may hold a reference to it and read the current values in [[reset]].
   */
  def prepare(offers: IndexedSeq[WorkerOffer], availableCpus: Array[BigDecimal]): Unit

  /**
   * Called at the start of every `resourceOfferSingleTaskSet` invocation to restart the iteration.
   * Strategies that order by free cores recompute their order here from the live `availableCpus`.
   */
  def reset(): Unit

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
 * Visits every offer once, in the order they were prepared.
 */
private[spark] class SimpleAssignmentStrategy extends TaskAssignmentStrategy {
  private var preparedOffer: IndexedSeq[WorkerOffer] = _
  private var index = 0

  override def prepare(offers: IndexedSeq[WorkerOffer], availableCpus: Array[BigDecimal]): Unit = {
    preparedOffer = offers
  }

  override def reset(): Unit = {
    index = 0
  }

  override def hasNext: Boolean = index < preparedOffer.length

  override def next(): Int = {
    val originalIndex = index
    index = index + 1
    originalIndex
  }
}

/**
 * Visits every offer once, but shuffles the offers once in `prepare` so that tasks are not always
 * placed on the same executors. The shuffled order is fixed for the whole round and reused across
 * all task sets and locality levels.
 */
private[spark] class RoundRobinAssignmentStrategy extends TaskAssignmentStrategy {
  private var shuffledIndices: IndexedSeq[Int] = _
  private var index = 0

  override def prepare(offers: IndexedSeq[WorkerOffer], availableCpus: Array[BigDecimal]): Unit = {
    shuffledIndices = Random.shuffle(offers.indices.toIndexedSeq)
  }

  override def reset(): Unit = {
    index = 0
  }

  override def hasNext: Boolean = index < shuffledIndices.length

  override def next(): Int = {
    val originalIndex = shuffledIndices(index)
    index = index + 1
    originalIndex
  }
}

/**
 * Concentrates a task set onto as few executors as possible, which can help dynamic allocation
 * reclaim idle executors and speed up executor decommissioning. Offers are visited best-fit first:
 * the executor with the fewest free cores (the "fullest" one) is tried first, with executor id as a
 * deterministic tie-breaker when two offers have the same number of free cores. The strategy keeps
 * launching tasks on the current offer until it can no longer accept one, only then advancing to
 * the next. Ordering by the live `availableCpus` (recomputed on every [[reset]]) means executors
 * that already carry work sort ahead of idle ones, so packing keeps filling the busiest executors
 * and lets the rest idle out; the executor-id tie-break keeps the target deterministic and
 * convergent across resource-offer rounds when free-core counts are equal.
 */
private[spark] class BinPackAssignmentStrategy extends TaskAssignmentStrategy {
  private var offers: IndexedSeq[WorkerOffer] = _
  private var availableCpus: Array[BigDecimal] = _
  private var orderedIndices: IndexedSeq[Int] = _
  private var index = 0

  override def prepare(offers: IndexedSeq[WorkerOffer], availableCpus: Array[BigDecimal]): Unit = {
    this.offers = offers
    this.availableCpus = availableCpus
  }

  override def reset(): Unit = {
    // Recompute the visit order from the live free-core counts (they change as tasks launch and
    // are reverted on the barrier partial-launch path). Fewest free cores first packs the fullest
    // executors, and executor id breaks ties so the order is stable across rounds.
    orderedIndices =
      offers.indices.sortBy(i => (availableCpus(i), offers(i).executorId)).toIndexedSeq
    index = 0
  }

  override def hasNext: Boolean = index < orderedIndices.size

  override def next(): Int = orderedIndices(index)

  override def taskLaunched(launched: Boolean): Unit = {
    // Move to the next work offer only when the current one can no longer accept a task.
    if (!launched) {
      index = index + 1
    }
  }
}

/**
 * Spreads a task set as evenly as possible across the available executors to favor parallelism.
 * Offers are kept in a priority queue keyed by the live free cores (with executor id as a
 * deterministic tie-breaker), and the offer with the most free cores is always tried next,
 * re-enqueued after a successful launch.
 */
private[spark] class BalanceAssignmentStrategy extends TaskAssignmentStrategy {
  private var offers: IndexedSeq[WorkerOffer] = _
  private var availableCpus: Array[BigDecimal] = _
  private var preparedOffers: mutable.PriorityQueue[Int] = _
  private var currentIndex: Int = _

  override def prepare(offers: IndexedSeq[WorkerOffer], availableCpus: Array[BigDecimal]): Unit = {
    this.offers = offers
    this.availableCpus = availableCpus
  }

  override def reset(): Unit = {
    // Order by the live free cores (descending) with executor id as a deterministic tie-breaker.
    // The comparator reads the live availableCpus array; only the just-dequeued offer's value is
    // mutated by the scheduler before it is re-enqueued, so the heap stays valid.
    implicit val ord: Ordering[Int] = new Ordering[Int] {
      override def compare(x: Int, y: Int): Int = {
        val byCpus = availableCpus(x).compare(availableCpus(y))
        if (byCpus != 0) {
          byCpus
        } else {
          // PriorityQueue dequeues the max, so reverse the id order to make the smaller executor
          // id win the tie.
          offers(y).executorId.compareTo(offers(x).executorId)
        }
      }
    }
    preparedOffers = new mutable.PriorityQueue[Int]()
    offers.indices.foreach(index => preparedOffers.enqueue(index))
  }

  override def hasNext: Boolean = preparedOffers.nonEmpty

  override def next(): Int = {
    currentIndex = preparedOffers.dequeue()
    currentIndex
  }

  override def taskLaunched(launched: Boolean): Unit = {
    if (launched) {
      preparedOffers.enqueue(currentIndex)
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
        throw new IllegalArgumentException("Do not recognize task assignment strategy: " + unknown)
    }
  }
}
