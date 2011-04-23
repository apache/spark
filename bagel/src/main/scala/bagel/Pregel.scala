package bagel

import spark._
import spark.SparkContext._

import scala.collection.mutable.ArrayBuffer

object Pregel extends Logging {
  def run[V <: Vertex : Manifest, M <: Message : Manifest, C : Manifest, A : Manifest](
    sc: SparkContext,
    verts: RDD[(String, V)],
    msgs: RDD[(String, M)]
  )(
    combiner: Combiner[M, C] = new DefaultCombiner[M],
    aggregator: Aggregator[V, A] = new NullAggregator[V],
    superstep: Int = 0,
    numSplits: Int = sc.numCores
  )(
    compute: (V, Option[C], A, Int) => (V, Iterable[M])
  ): RDD[V] = {

    logInfo("Starting superstep "+superstep+".")
    val startTime = System.currentTimeMillis

    val aggregated = agg(verts, aggregator)
    val combinedMsgs = msgs.combineByKey(combiner.createCombiner, combiner.mergeMsg, combiner.mergeCombiners, numSplits)
    val grouped = verts.groupWith(combinedMsgs)
    val (processed, numMsgs, numActiveVerts) = comp[V, M, C](sc, grouped, compute(_, _, aggregated, superstep))

    val timeTaken = System.currentTimeMillis - startTime
    logInfo("Superstep %d took %d s".format(superstep, timeTaken / 1000))

    // Check stopping condition and iterate
    val noActivity = numMsgs == 0 && numActiveVerts == 0
    if (noActivity) {
      processed.map { case (id, (vert, msgs)) => vert }
    } else {
      val newVerts = processed.mapValues { case (vert, msgs) => vert }
      val newMsgs = processed.flatMap {
        case (id, (vert, msgs)) => msgs.map(m => (m.targetId, m))
      }
      run(sc, newVerts, newMsgs)(combiner, aggregator, superstep + 1, numSplits)(compute)
    }
  }

  /**
   * Aggregates the given vertices using the given aggregator, or does
   * nothing if it is a NullAggregator.
   */
  def agg[V <: Vertex, A : Manifest](verts: RDD[(String, V)], aggregator: Aggregator[V, A]): A = aggregator match {
    case _: NullAggregator[_] =>
      None
    case _ =>
      verts.map {
        case (id, vert) => aggregator.createAggregator(vert)
      }.reduce(aggregator.mergeAggregators(_, _))
  }

  /**
   * Processes the given vertex-message RDD using the compute
   * function. Returns the processed RDD, the number of messages
   * created, and the number of active vertices.
   */
  def comp[V <: Vertex, M <: Message, C](sc: SparkContext, grouped: RDD[(String, (Seq[V], Seq[C]))], compute: (V, Option[C]) => (V, Iterable[M])): (RDD[(String, (V, Iterable[M]))], Int, Int) = {
    var numMsgs = sc.accumulator(0)
    var numActiveVerts = sc.accumulator(0)
    val processed = grouped.flatMapValues {
      case (Seq(), _) => None
      case (Seq(v), c) =>
          val (newVert, newMsgs) =
            compute(v, c match {
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
   * one that does, so it can be passed to Pregel.run.
   */
  implicit def addAggregatorArg[
    V <: Vertex : Manifest, M <: Message : Manifest, C
  ](
    compute: (V, Option[C], Int) => (V, Iterable[M])
  ): (V, Option[C], Option[Nothing], Int) => (V, Iterable[M]) = {
    (vert: V, messages: Option[C], aggregator: Option[Nothing], superstep: Int) => compute(vert, messages, superstep)
  }
}

// TODO: Simplify Combiner interface and make it more OO.
trait Combiner[M, C] {
  def createCombiner(msg: M): C
  def mergeMsg(combiner: C, msg: M): C
  def mergeCombiners(a: C, b: C): C
}

trait Aggregator[V, A] {
  def createAggregator(vert: V): A
  def mergeAggregators(a: A, b: A): A
}

@serializable
class DefaultCombiner[M] extends Combiner[M, ArrayBuffer[M]] {
  def createCombiner(msg: M): ArrayBuffer[M] =
    ArrayBuffer(msg)
  def mergeMsg(combiner: ArrayBuffer[M], msg: M): ArrayBuffer[M] =
    combiner += msg
  def mergeCombiners(a: ArrayBuffer[M], b: ArrayBuffer[M]): ArrayBuffer[M] =
    a ++= b
}

@serializable
class NullAggregator[V] extends Aggregator[V, Option[Nothing]] {
  def createAggregator(vert: V): Option[Nothing] = None
  def mergeAggregators(a: Option[Nothing], b: Option[Nothing]): Option[Nothing] = None
}

/**
 * Represents a Pregel vertex.
 *
 * Subclasses may store state along with each vertex and must be
 * annotated with @serializable.
 */
trait Vertex {
  def id: String
  def active: Boolean
}

/**
 * Represents a Pregel message to a target vertex.
 *
 * Subclasses may contain a payload to deliver to the target vertex
 * and must be annotated with @serializable.
 */
trait Message {
  def targetId: String
}

/**
 * Represents a directed edge between two vertices.
 *
 * Subclasses may store state along each edge and must be annotated
 * with @serializable.
 */
trait Edge {
  def targetId: String
}
