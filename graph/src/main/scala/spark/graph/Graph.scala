package spark.graph

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import com.esotericsoftware.kryo._

import it.unimi.dsi.fastutil.ints.IntArrayList

import spark.{ClosureCleaner, HashPartitioner, KryoRegistrator, SparkContext, RDD}
import spark.SparkContext._
import spark.storage.StorageLevel



class Vertex[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) VD] {
  var id: Vid = _
  var data: VD = _

  def this(id: Int, data: VD) { this(); this.id = id; this.data = data; }
}


class Edge[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED] {
  var src: Vid = _
  var dst: Vid = _
  var data: ED = _

  def this(src: Vid, dst: Vid, data: ED) {
    this(); this.src = src; this.dst = dst; this.data = data;
  }
}


class EdgeWithVertices[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) VD,
                       @specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED] {

  var src: Vertex[VD] = _
  var dst: Vertex[VD] = _
  var data: ED = _

  def otherVertex(vid: Vid): Vertex[VD] = if (src.id == vid) dst else src

  def vertex(vid: Vid): Vertex[VD] = if (src.id == vid) src else dst
}


/**
 * A Graph RDD that supports computation on graphs.
 */
class Graph[VD: Manifest, ED: Manifest](
  private val _vertices: RDD[Vertex[VD]],
  private val _edges: RDD[Edge[ED]]) {

  import Graph.EdgePartition

  var numEdgePartitions = 5
  var numVertexPartitions = 5

  private val eTable: RDD[(Pid, EdgePartition[ED])] = Graph.createETable(
    _edges, numEdgePartitions)

  private val vTable: RDD[(Vid, (VD, Array[Pid]))] = Graph.createVTable(
    _vertices, eTable, numVertexPartitions)

  def edges: RDD[Edge[ED]] = eTable.mapPartitions { iter => iter.next._2.iterator }

  def edgesWithVertices: RDD[EdgeWithVertices[VD, ED]] = new EdgeWithVerticesRDD(vTable, eTable)

}


object Graph {

  /**
   * A partition of edges. This is created so we can store edge data in columnar format so it is
   * more efficient to store the data in memory.
   */
  private[graph]
  class EdgePartition[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED: Manifest] {
    val srcIds: IntArrayList = new IntArrayList
    val dstIds: IntArrayList = new IntArrayList
    // TODO: Specialize data.
    val data: ArrayBuffer[ED] = new ArrayBuffer[ED]

    /** Add a new edge to the partition. */
    def add(src: Vid, dst: Vid, d: ED) {
      srcIds.add(src)
      dstIds.add(dst)
      data += d
    }

    def trim() {
      srcIds.trim()
      dstIds.trim()
    }

    def size: Int = srcIds.size

    def iterator = new Iterator[Edge[ED]] {
      private var edge = new Edge[ED]
      private var pos = 0

      override def hasNext: Boolean = pos < size

      override def next(): Edge[ED] = {
        edge.src = srcIds.get(pos)
        edge.dst = dstIds.get(pos)
        edge.data = data(pos)
        pos += 1
        edge
      }
    }
  }

  private[graph]
  def createVTable[VD: Manifest, ED: Manifest](
      vertices: RDD[Vertex[VD]],
      eTable: RDD[(Pid, EdgePartition[ED])],
      numPartitions: Int) = {
    val partitioner = new HashPartitioner(numPartitions)

    // A key-value RDD. The key is a vertex id, and the value is a list of
    // partitions that contains edges referencing the vertex.
    val vid2pid : RDD[(Int, Seq[Pid])] = eTable.mapPartitions { iter =>
      val (pid, edgePartition) = iter.next()
      val vSet = new it.unimi.dsi.fastutil.ints.IntOpenHashSet
      var i = 0
      while (i < edgePartition.srcIds.size) {
        vSet.add(edgePartition.srcIds.getInt(i))
        vSet.add(edgePartition.dstIds.getInt(i))
        i += 1
      }
      vSet.iterator.map { vid => (vid.intValue, pid) }
    }.groupByKey(partitioner)

    vertices
      .map { v => (v.id, v.data) }
      .partitionBy(partitioner)
      .leftOuterJoin(vid2pid)
      .mapValues {
        case (vdata, None)       => (vdata, Array.empty[Pid])
        case (vdata, Some(pids)) => (vdata, pids.toArray)
      }
      .cache()
  }

  /**
   * Create the edge table RDD, which is much more efficient for Java heap storage than the
   * normal edges data structure (RDD[(Vid, Vid, ED)]).
   *
   * The edge table contains multiple partitions, and each partition contains only one RDD
   * key-value pair: the key is the partition id, and the value is an EdgePartition object
   * containing all the edges in a partition.
   */
  private[graph]
  def createETable[ED: Manifest](edges: RDD[Edge[ED]], numPartitions: Int)
  : RDD[(Pid, EdgePartition[ED])] = {

    edges.map { e =>
      // Random partitioning based on the source vertex id.
      (math.abs(e.src) % numPartitions, (e.src, e.dst, e.data))
    }
    .partitionBy(new HashPartitioner(numPartitions))
    .mapPartitionsWithIndex({ (pid, iter) =>
      val edgePartition = new Graph.EdgePartition[ED]
      iter.foreach { case (_, (src, dst, data)) => edgePartition.add(src, dst, data) }
      Iterator((pid, edgePartition))
    }, preservesPartitioning = true)
    .cache()
  }
}

