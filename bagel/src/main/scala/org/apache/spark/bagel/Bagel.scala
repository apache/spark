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

package org.apache.spark.bagel

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object Bagel extends Logging {
  val DEFAULT_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK

  /**
   * Runs a Bagel program.
   * @param sc [[org.apache.spark.SparkContext]] to use for the program.
   * @param vertices vertices of the graph represented as an RDD of (Key, Vertex) pairs. Often the Key will be
   *                 the vertex id.
   * @param messages initial set of messages represented as an RDD of (Key, Message) pairs. Often this will be an
   *                 empty array, i.e. sc.parallelize(Array[K, Message]()).
   * @param combiner [[org.apache.spark.bagel.Combiner]] combines multiple individual messages to a given vertex into one
   *                message before sending (which often involves network I/O).
   * @param aggregator [[org.apache.spark.bagel.Aggregator]] performs a reduce across all vertices after each superstep,
   *                  and provides the result to each vertex in the next superstep.
   * @param partitioner [[org.apache.spark.Partitioner]] partitions values by key
   * @param numPartitions number of partitions across which to split the graph.
   *                      Default is the default parallelism of the SparkContext
   * @param storageLevel [[org.apache.spark.storage.StorageLevel]] to use for caching of intermediate RDDs in each superstep.
   *                    Defaults to caching in memory.
   * @param compute function that takes a Vertex, optional set of (possibly combined) messages to the Vertex,
   *                optional Aggregator and the current superstep,
   *                and returns a set of (Vertex, outgoing Messages) pairs
   * @tparam K key
   * @tparam V vertex type
   * @tparam M message type
   * @tparam C combiner
   * @tparam A aggregator
   * @return an RDD of (K, V) pairs representing the graph after completion of the program
   */
  def run[K: Manifest, V <: Vertex : Manifest, M <: Message[K] : Manifest,
          C: Manifest, A: Manifest](
    sc: SparkContext,
    vertices: RDD[(K, V)],
    messages: RDD[(K, M)],
    combiner: Combiner[M, C],
    aggregator: Option[Aggregator[V, A]],
    partitioner: Partitioner,
    numPartitions: Int,
    storageLevel: StorageLevel = DEFAULT_STORAGE_LEVEL
  )(
    compute: (V, Option[C], Option[A], Int) => (V, Array[M])
  ): RDD[(K, V)] = {
    val splits = if (numPartitions != 0) numPartitions else sc.defaultParallelism

    var superstep = 0
    var verts = vertices
    var msgs = messages
    var noActivity = false
    do {
      logInfo("Starting superstep "+superstep+".")
      val startTime = System.currentTimeMillis

      val aggregated = agg(verts, aggregator)
      val combinedMsgs = msgs.combineByKey(
        combiner.createCombiner _, combiner.mergeMsg _, combiner.mergeCombiners _, partitioner)
      val grouped = combinedMsgs.groupWith(verts)
      val superstep_ = superstep  // Create a read-only copy of superstep for capture in closure
      val (processed, numMsgs, numActiveVerts) =
        comp[K, V, M, C](sc, grouped, compute(_, _, aggregated, superstep_), storageLevel)

      val timeTaken = System.currentTimeMillis - startTime
      logInfo("Superstep %d took %d s".format(superstep, timeTaken / 1000))

      verts = processed.mapValues { case (vert, msgs) => vert }
      msgs = processed.flatMap {
        case (id, (vert, msgs)) => msgs.map(m => (m.targetId, m))
      }
      superstep += 1

      noActivity = numMsgs == 0 && numActiveVerts == 0
    } while (!noActivity)

    verts
  }

  /** Runs a Bagel program with no [[org.apache.spark.bagel.Aggregator]] and the default storage level */
  def run[K: Manifest, V <: Vertex : Manifest, M <: Message[K] : Manifest, C: Manifest](
    sc: SparkContext,
    vertices: RDD[(K, V)],
    messages: RDD[(K, M)],
    combiner: Combiner[M, C],
    partitioner: Partitioner,
    numPartitions: Int
  )(
    compute: (V, Option[C], Int) => (V, Array[M])
  ): RDD[(K, V)] = run(sc, vertices, messages, combiner, numPartitions, DEFAULT_STORAGE_LEVEL)(compute)

  /** Runs a Bagel program with no [[org.apache.spark.bagel.Aggregator]] */
  def run[K: Manifest, V <: Vertex : Manifest, M <: Message[K] : Manifest, C: Manifest](
    sc: SparkContext,
    vertices: RDD[(K, V)],
    messages: RDD[(K, M)],
    combiner: Combiner[M, C],
    partitioner: Partitioner,
    numPartitions: Int,
    storageLevel: StorageLevel
  )(
    compute: (V, Option[C], Int) => (V, Array[M])
  ): RDD[(K, V)] = {
    run[K, V, M, C, Nothing](
      sc, vertices, messages, combiner, None, partitioner, numPartitions, storageLevel)(
      addAggregatorArg[K, V, M, C](compute))
  }

  /**
   * Runs a Bagel program with no [[org.apache.spark.bagel.Aggregator]], default [[org.apache.spark.HashPartitioner]]
   * and default storage level
   */
  def run[K: Manifest, V <: Vertex : Manifest, M <: Message[K] : Manifest, C: Manifest](
    sc: SparkContext,
    vertices: RDD[(K, V)],
    messages: RDD[(K, M)],
    combiner: Combiner[M, C],
    numPartitions: Int
  )(
    compute: (V, Option[C], Int) => (V, Array[M])
  ): RDD[(K, V)] = run(sc, vertices, messages, combiner, numPartitions, DEFAULT_STORAGE_LEVEL)(compute)

  /** Runs a Bagel program with no [[org.apache.spark.bagel.Aggregator]] and the default [[org.apache.spark.HashPartitioner]]*/
  def run[K: Manifest, V <: Vertex : Manifest, M <: Message[K] : Manifest, C: Manifest](
    sc: SparkContext,
    vertices: RDD[(K, V)],
    messages: RDD[(K, M)],
    combiner: Combiner[M, C],
    numPartitions: Int,
    storageLevel: StorageLevel
  )(
    compute: (V, Option[C], Int) => (V, Array[M])
  ): RDD[(K, V)] = {
    val part = new HashPartitioner(numPartitions)
    run[K, V, M, C, Nothing](
      sc, vertices, messages, combiner, None, part, numPartitions, storageLevel)(
      addAggregatorArg[K, V, M, C](compute))
  }

  /**
   * Runs a Bagel program with no [[org.apache.spark.bagel.Aggregator]], default [[org.apache.spark.HashPartitioner]],
   * [[org.apache.spark.bagel.DefaultCombiner]] and the default storage level
   */
  def run[K: Manifest, V <: Vertex : Manifest, M <: Message[K] : Manifest](
    sc: SparkContext,
    vertices: RDD[(K, V)],
    messages: RDD[(K, M)],
    numPartitions: Int
  )(
    compute: (V, Option[Array[M]], Int) => (V, Array[M])
  ): RDD[(K, V)] = run(sc, vertices, messages, numPartitions, DEFAULT_STORAGE_LEVEL)(compute)

  /**
   * Runs a Bagel program with no [[org.apache.spark.bagel.Aggregator]], the default [[org.apache.spark.HashPartitioner]]
   * and [[org.apache.spark.bagel.DefaultCombiner]]
   */
  def run[K: Manifest, V <: Vertex : Manifest, M <: Message[K] : Manifest](
    sc: SparkContext,
    vertices: RDD[(K, V)],
    messages: RDD[(K, M)],
    numPartitions: Int,
    storageLevel: StorageLevel
   )(
    compute: (V, Option[Array[M]], Int) => (V, Array[M])
   ): RDD[(K, V)] = {
    val part = new HashPartitioner(numPartitions)
    run[K, V, M, Array[M], Nothing](
      sc, vertices, messages, new DefaultCombiner(), None, part, numPartitions, storageLevel)(
      addAggregatorArg[K, V, M, Array[M]](compute))
  }

  /**
   * Aggregates the given vertices using the given aggregator, if it
   * is specified.
   */
  private def agg[K, V <: Vertex, A: Manifest](
    verts: RDD[(K, V)],
    aggregator: Option[Aggregator[V, A]]
  ): Option[A] = aggregator match {
    case Some(a) =>
      Some(verts.map {
        case (id, vert) => a.createAggregator(vert)
      }.reduce(a.mergeAggregators(_, _)))
    case None => None
  }

  /**
   * Processes the given vertex-message RDD using the compute
   * function. Returns the processed RDD, the number of messages
   * created, and the number of active vertices.
   */
  private def comp[K: Manifest, V <: Vertex, M <: Message[K], C](
    sc: SparkContext,
    grouped: RDD[(K, (Seq[C], Seq[V]))],
    compute: (V, Option[C]) => (V, Array[M]),
    storageLevel: StorageLevel
  ): (RDD[(K, (V, Array[M]))], Int, Int) = {
    var numMsgs = sc.accumulator(0)
    var numActiveVerts = sc.accumulator(0)
    val processed = grouped.flatMapValues {
      case (_, vs) if vs.size == 0 => None
      case (c, vs) =>
        val (newVert, newMsgs) =
          compute(vs(0), c match {
            case Seq(comb) => Some(comb)
            case Seq() => None
          })

        numMsgs += newMsgs.size
        if (newVert.active)
          numActiveVerts += 1

        Some((newVert, newMsgs))
    }.persist(storageLevel)

    // Force evaluation of processed RDD for accurate performance measurements
    processed.foreach(x => {})

    (processed, numMsgs.value, numActiveVerts.value)
  }

  /**
   * Converts a compute function that doesn't take an aggregator to
   * one that does, so it can be passed to Bagel.run.
   */
  private def addAggregatorArg[K: Manifest, V <: Vertex : Manifest, M <: Message[K] : Manifest, C](
    compute: (V, Option[C], Int) => (V, Array[M])
  ): (V, Option[C], Option[Nothing], Int) => (V, Array[M]) = {
    (vert: V, msgs: Option[C], aggregated: Option[Nothing], superstep: Int) =>
      compute(vert, msgs, superstep)
  }
}

trait Combiner[M, C] {
  def createCombiner(msg: M): C
  def mergeMsg(combiner: C, msg: M): C
  def mergeCombiners(a: C, b: C): C
}

trait Aggregator[V, A] {
  def createAggregator(vert: V): A
  def mergeAggregators(a: A, b: A): A
}

/** Default combiner that simply appends messages together (i.e. performs no aggregation) */
class DefaultCombiner[M: Manifest] extends Combiner[M, Array[M]] with Serializable {
  def createCombiner(msg: M): Array[M] =
    Array(msg)
  def mergeMsg(combiner: Array[M], msg: M): Array[M] =
    combiner :+ msg
  def mergeCombiners(a: Array[M], b: Array[M]): Array[M] =
    a ++ b
}

/**
 * Represents a Bagel vertex.
 *
 * Subclasses may store state along with each vertex and must
 * inherit from java.io.Serializable or scala.Serializable.
 */
trait Vertex {
  def active: Boolean
}

/**
 * Represents a Bagel message to a target vertex.
 *
 * Subclasses may contain a payload to deliver to the target vertex
 * and must inherit from java.io.Serializable or scala.Serializable.
 */
trait Message[K] {
  def targetId: K
}
