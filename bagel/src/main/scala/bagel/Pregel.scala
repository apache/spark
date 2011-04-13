package bagel

import spark._
import spark.SparkContext._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

object Pregel extends Logging {
  /**
   * Runs a Pregel job on the given vertices, running the specified
   * compute function on each vertex in every superstep. Before
   * beginning the first superstep, sends the given messages to their
   * destination vertices. In the join stage, launches splits
   * separate tasks (where splits is manually specified to work
   * around a bug in Spark).
   *
   * Halts when no more messages are being sent between vertices, and
   * all vertices have voted to halt by setting their state to
   * Inactive.
   */
  def run[V <: Vertex : Manifest, M <: Message : Manifest, C](sc: SparkContext, verts: RDD[(String, V)], msgs: RDD[(String, M)], splits: Int, messageCombiner: (C, M) => C, defaultCombined: () => C, mergeCombined: (C, C) => C, superstep: Int = 0)(compute: (V, C, Int) => (V, Iterable[M])): RDD[V] = {
    println("Starting superstep "+superstep+".")
    val startTime = System.currentTimeMillis

    // Bring together vertices and messages
    println("Joining vertices and messages...")
    val combinedMsgs = msgs.combineByKey({x => messageCombiner(defaultCombined(), x)}, messageCombiner, mergeCombined, splits)
    println("verts.splits.size = " + verts.splits.size)
    println("combinedMsgs.splits.size = " + combinedMsgs.splits.size)
    println("verts.partitioner = " + verts.partitioner)
    println("combinedMsgs.partitioner = " + combinedMsgs.partitioner)
    val joined = verts.groupWith(combinedMsgs)
    println("joined.splits.size = " + joined.splits.size)
    println("joined.partitioner = " + joined.partitioner)
    //val joined = graph.groupByKeyAsymmetrical(messageCombiner, defaultCombined, mergeCombined, splits)
    println("Done joining vertices and messages.")

    // Run compute on each vertex
    println("Running compute on each vertex...")
    var messageCount = sc.accumulator(0)
    var activeVertexCount = sc.accumulator(0)
    val processed = joined.flatMapValues {
      case (Seq(), _) => None
      case (Seq(v), Seq(comb)) =>
          val (newVertex, newMessages) = compute(v, comb, superstep)
          messageCount += newMessages.size
          if (newVertex.active)
            activeVertexCount += 1
          Some((newVertex, newMessages))
          //val result = ArrayBuffer[(String, Either[V, M])]((newVertex.id, Left(newVertex)))
          //result ++= newMessages.map(m => (m.targetId, Right(m)))
      case (Seq(v), Seq()) =>
          val (newVertex, newMessages) = compute(v, defaultCombined(), superstep)
          messageCount += newMessages.size
          if (newVertex.active)
            activeVertexCount += 1
          Some((newVertex, newMessages))
    }.cache
    //MATEI: Added this
    processed.foreach(x => {})
    println("Done running compute on each vertex.")

    println("Checking stopping condition...")
    val stop = messageCount.value == 0 && activeVertexCount.value == 0

    val timeTaken = System.currentTimeMillis - startTime
    println("Superstep %d took %d s".format(superstep, timeTaken / 1000))

    val newVerts = processed.mapValues(_._1)
    val newMsgs = processed.flatMap(x => x._2._2.map(m => (m.targetId, m)))

    if (superstep >= 10)
      processed.map { _._2._1 }
    else
      run(sc, newVerts, newMsgs, splits, messageCombiner, defaultCombined, mergeCombined, superstep + 1)(compute)
  }
}

/**
 * Represents a Pregel vertex. Must be subclassed to store state
 * along with each vertex. Must be annotated with @serializable.
 */
trait Vertex {
  def id: String
  def active: Boolean
}

/**
 * Represents a Pregel message to a target vertex. Must be
 * subclassed to contain a payload. Must be annotated with @serializable.
 */
trait Message {
  def targetId: String
}

/**
 * Represents a directed edge between two vertices. Owned by the
 * source vertex, and contains the ID of the target vertex. Must
 * be subclassed to store state along with each edge. Must be annotated with @serializable.
 */
trait Edge {
  def targetId: String
}
