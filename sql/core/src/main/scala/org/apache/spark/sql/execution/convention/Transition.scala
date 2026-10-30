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

package org.apache.spark.sql.execution.convention

import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.execution.SparkPlan

/**
 * :: DeveloperApi :: Converts the output of a plan from one [[ConventionType]] to another,
 * usually by wrapping the plan with a transition operator. Row-to-batch operators should extend
 * [[org.apache.spark.sql.execution.RowToColumnarTransition]], and batch-to-row operators should
 * extend [[org.apache.spark.sql.execution.ColumnarToRowTransition]]. Direct batch-to-batch
 * transitions are not supported.
 */
@DeveloperApi
abstract class Transition {
  def apply(plan: SparkPlan): SparkPlan

  /** Cost used to pick the cheapest transition path. */
  def cost: Int = 1
}

object Transition {

  /** A no-op transition between compatible types. */
  val empty: Transition = new Transition {
    override def apply(plan: SparkPlan): SparkPlan = plan
    override def cost: Int = 0
    override def toString: String = "Transition.empty"
  }

  def apply(f: SparkPlan => SparkPlan): Transition = new Transition {
    override def apply(plan: SparkPlan): SparkPlan = f(plan)
  }
}

/**
 * :: DeveloperApi :: A graph of registered [[ConventionType]]s and [[Transition]]s between them.
 * Each instance includes Spark's built-in types.
 */
@DeveloperApi
trait TransitionGraph {
  /** Registers a type and its transitions. Referenced types must already be registered. */
  def register(t: ConventionType): Unit

  /** Adds a transition between registered types. Duplicate transitions are rejected. */
  def addEdge(from: ConventionType, to: ConventionType, transition: Transition): Unit

  def registeredTypes: Seq[ConventionType]

  /**
   * The cheapest sequence of transitions from `from` to `to`, applied in order. Empty when
   * `from == to`, `None` if `to` is unreachable.
   */
  def findPath(from: ConventionType, to: ConventionType): Option[Seq[Transition]]

  def findPathOrThrow(from: ConventionType, to: ConventionType): Seq[Transition] = {
    findPath(from, to).getOrElse {
      throw SparkException.internalError(
        s"No transition from $from to $to. Registered types: ${registeredTypes.mkString(", ")}")
    }
  }

  /** Inserts the cheapest transition path between the two layouts. */
  def insertTransitions(plan: SparkPlan, from: ConventionType, to: ConventionType): SparkPlan = {
    findPathOrThrow(from, to).foldLeft(plan)((p, t) => t.apply(p))
  }
}

/**
 * :: DeveloperApi :: Creates transition graphs containing Spark's built-in types.
 */
@DeveloperApi
object TransitionGraph {
  def apply(): TransitionGraph = {
    val graph = new TransitionGraphImpl
    // Spark's built-in types.
    graph.register(RowType.SparkRowType)
    graph.register(BatchType.SparkBatchType)
    graph.register(BatchType.ArrowBatchType)
    graph
  }

  private class TransitionGraphImpl extends TransitionGraph {
    private val vertices = mutable.LinkedHashSet[ConventionType]()
    private val edges =
      mutable.LinkedHashMap[ConventionType, mutable.LinkedHashMap[ConventionType, Transition]]()
    private val pathCache =
      mutable.HashMap[(ConventionType, ConventionType), Option[Seq[Transition]]]()

    /** Registers a type and its transitions. Referenced types must already be registered. */
    override def register(t: ConventionType): Unit = synchronized {
      addVertex(t)
      t.registerTransitions(this)
    }

    private[convention] def addVertex(t: ConventionType): Unit = synchronized {
      require(t != RowType.None && t != BatchType.None, s"Invalid convention type $t")
      require(!vertices.contains(t), s"Convention type $t is already registered")
      vertices += t
      pathCache.clear()
    }

    /** Adds a transition between registered types. Duplicate transitions are rejected. */
    override def addEdge(from: ConventionType, to: ConventionType, transition: Transition): Unit =
      synchronized {
        require(from != to, s"Transition from $from to itself")
        require(
          from != RowType.None && from != BatchType.None && to != RowType.None &&
            to != BatchType.None,
          s"Transition from $from to $to")
        require(
          !(from.isInstanceOf[BatchType] && to.isInstanceOf[BatchType]),
          "Direct batch-to-batch transitions are not supported")
        require(vertices.contains(from), s"Convention type $from is not registered")
        require(vertices.contains(to), s"Convention type $to is not registered")
        val out = edges.getOrElseUpdate(from, mutable.LinkedHashMap())
        require(!out.contains(to), s"Transition from $from to $to is already registered")
        out(to) = transition
        pathCache.clear()
      }

    override def registeredTypes: Seq[ConventionType] = synchronized(vertices.toSeq)

    /**
     * The cheapest sequence of transitions from `from` to `to`, applied in order. Empty when
     * `from == to`, `None` if `to` is unreachable.
     */
    override def findPath(from: ConventionType, to: ConventionType): Option[Seq[Transition]] =
      synchronized {
        require(vertices.contains(from), s"Convention type $from is not registered")
        require(vertices.contains(to), s"Convention type $to is not registered")
        pathCache.getOrElseUpdate((from, to), dijkstra(from, to))
      }

    private def dijkstra(from: ConventionType, to: ConventionType): Option[Seq[Transition]] = {
      val dist = mutable.HashMap[ConventionType, Int](from -> 0)
      val prev = mutable.HashMap[ConventionType, (ConventionType, Transition)]()
      val done = mutable.HashSet[ConventionType]()
      var current: Option[ConventionType] = Some(from)
      while (current.isDefined && current.get != to) {
        val u = current.get
        done += u
        edges
          .get(u)
          .foreach(_.foreach { case (v, t) =>
            val d = dist(u) + t.cost
            if (!done.contains(v) && dist.get(v).forall(d < _)) {
              dist(v) = d
              prev(v) = (u, t)
            }
          })
        current = dist.iterator
          .filter { case (v, _) => !done.contains(v) }
          .minByOption(_._2)
          .map(_._1)
      }
      if (current.isEmpty) {
        None
      } else {
        val path = mutable.ArrayBuffer[Transition]()
        var v = to
        while (v != from) {
          val (u, t) = prev(v)
          path.prepend(t)
          v = u
        }
        Some(path.toSeq)
      }
    }
  }
}
