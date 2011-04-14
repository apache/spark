package bagel

import spark._
import spark.SparkContext._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

object Pregel extends Logging {
  /**
   * Runs a Pregel job on the given vertices consisting of the
   * specified compute function.
   *
   * Before beginning the first superstep, the given messages are sent
   * to their destination vertices.
   *
   * During the job, the specified combiner functions are applied to
   * messages as they travel between vertices.
   *
   * The job halts and returns the resulting set of vertices when no
   * messages are being sent between vertices and all vertices have
   * voted to halt by setting their state to inactive.
   */
  def run[V <: Vertex : Manifest, M <: Message : Manifest, C](
    sc: SparkContext,
    verts: RDD[(String, V)],
    msgs: RDD[(String, M)],
    createCombiner: M => C,
    mergeMsg: (C, M) => C,
    mergeCombiners: (C, C) => C,
    numSplits: Int,
    superstep: Int = 0
  )(compute: (V, Option[C], Int) => (V, Iterable[M])): RDD[V] = {

    logInfo("Starting superstep "+superstep+".")
    val startTime = System.currentTimeMillis

    // Bring together vertices and messages
    val combinedMsgs = msgs.combineByKey(createCombiner, mergeMsg, mergeCombiners, numSplits)
    val grouped = verts.groupWith(combinedMsgs)

    // Run compute on each vertex
    var numMsgs = sc.accumulator(0)
    var numActiveVerts = sc.accumulator(0)
    val processed = grouped.flatMapValues {
      case (Seq(), _) => None
      case (Seq(v), c) =>
          val (newVert, newMsgs) =
            compute(v, c match {
              case Seq(comb) => Some(comb)
              case Seq() => None
            }, superstep)

          numMsgs += newMsgs.size
          if (newVert.active)
            numActiveVerts += 1

          Some((newVert, newMsgs))
    }.cache

    // Force evaluation of processed RDD for accurate performance measurements
    processed.foreach(x => {})

    val timeTaken = System.currentTimeMillis - startTime
    logInfo("Superstep %d took %d s".format(superstep, timeTaken / 1000))

    // Check stopping condition and iterate
    val noActivity = numMsgs.value == 0 && numActiveVerts.value == 0
    if (noActivity) {
      processed.map { case (id, (vert, msgs)) => vert }
    } else {
      val newVerts = processed.mapValues { case (vert, msgs) => vert }
      val newMsgs = processed.flatMap {
        case (id, (vert, msgs)) => msgs.map(m => (m.targetId, m))
      }
      run(sc, newVerts, newMsgs, createCombiner, mergeMsg, mergeCombiners, numSplits, superstep + 1)(compute)
    }
  }
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
