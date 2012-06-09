package spark.bagel

import spark._
import spark.SparkContext._

import scala.collection.mutable.ArrayBuffer

object Bagel extends Logging {
  def run[K : Manifest, V <: Vertex : Manifest, M <: Message[K] : Manifest,
          C : Manifest, A : Manifest](
    sc: SparkContext,
    vertices: RDD[(K, V)],
    messages: RDD[(K, M)],
    combiner: Combiner[M, C],
    aggregator: Option[Aggregator[V, A]],
    partitioner: Partitioner,
    numSplits: Int
  )(
    compute: (V, Option[C], Option[A], Int) => (V, Array[M])
  ): RDD[(K, V)] = {
    val splits = if (numSplits != 0) numSplits else sc.defaultParallelism

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
      val (processed, numMsgs, numActiveVerts) =
        comp[K, V, M, C](sc, grouped, compute(_, _, aggregated, superstep))

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

  def run[K : Manifest, V <: Vertex : Manifest, M <: Message[K] : Manifest,
          C : Manifest](
    sc: SparkContext,
    vertices: RDD[(K, V)],
    messages: RDD[(K, M)],
    combiner: Combiner[M, C],
    partitioner: Partitioner,
    numSplits: Int
  )(
    compute: (V, Option[C], Int) => (V, Array[M])
  ): RDD[(K, V)] = {
    run[K, V, M, C, Nothing](
      sc, vertices, messages, combiner, None, partitioner, numSplits)(
      addAggregatorArg[K, V, M, C](compute))
  }

  def run[K : Manifest, V <: Vertex : Manifest, M <: Message[K] : Manifest,
          C : Manifest](
    sc: SparkContext,
    vertices: RDD[(K, V)],
    messages: RDD[(K, M)],
    combiner: Combiner[M, C],
    numSplits: Int
  )(
    compute: (V, Option[C], Int) => (V, Array[M])
  ): RDD[(K, V)] = {
    val part = new HashPartitioner(numSplits)
    run[K, V, M, C, Nothing](
      sc, vertices, messages, combiner, None, part, numSplits)(
      addAggregatorArg[K, V, M, C](compute))
  }

  def run[K : Manifest, V <: Vertex : Manifest, M <: Message[K] : Manifest](
    sc: SparkContext,
    vertices: RDD[(K, V)],
    messages: RDD[(K, M)],
    numSplits: Int
  )(
    compute: (V, Option[Array[M]], Int) => (V, Array[M])
  ): RDD[(K, V)] = {
    val part = new HashPartitioner(numSplits)
    run[K, V, M, Array[M], Nothing](
      sc, vertices, messages, new DefaultCombiner(), None, part, numSplits)(
      addAggregatorArg[K, V, M, Array[M]](compute))
  }

  /**
   * Aggregates the given vertices using the given aggregator, if it
   * is specified.
   */
  private def agg[K, V <: Vertex, A : Manifest](
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
  private def comp[K : Manifest, V <: Vertex, M <: Message[K], C](
    sc: SparkContext,
    grouped: RDD[(K, (Seq[C], Seq[V]))],
    compute: (V, Option[C]) => (V, Array[M])
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
    }.cache

    // Force evaluation of processed RDD for accurate performance measurements
    processed.foreach(x => {})

    (processed, numMsgs.value, numActiveVerts.value)
  }

  /**
   * Converts a compute function that doesn't take an aggregator to
   * one that does, so it can be passed to Bagel.run.
   */
  private def addAggregatorArg[
    K : Manifest, V <: Vertex : Manifest, M <: Message[K] : Manifest, C
  ](
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

class DefaultCombiner[M : Manifest] extends Combiner[M, Array[M]] with Serializable {
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
