package spark.graph

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import it.unimi.dsi.fastutil.ints.IntArrayList

import spark.{ClosureCleaner, HashPartitioner, SparkContext, RDD}
import spark.SparkContext._
import spark.graph.Graph.EdgePartition
import spark.storage.StorageLevel


case class Vertex[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) VD] (
  var id: Vid = 0,
  var data: VD = nullValue[VD])


case class Edge[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED] (
  var src: Vid = 0,
  var dst: Vid = 0,
  var data: ED = nullValue[ED])


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
  val rawVertices: RDD[Vertex[VD]],
  val rawEdges: RDD[Edge[ED]]) {

  var numEdgePartitions = 5
  var numVertexPartitions = 5

  val vertexPartitioner = new HashPartitioner(numVertexPartitions)

  val edgePartitioner = new HashPartitioner(numEdgePartitions)

  lazy val eTable: RDD[(Pid, EdgePartition[ED])] = Graph.createETable(
    rawEdges, numEdgePartitions)

  lazy val vTable: RDD[(Vid, (VD, Array[Pid]))] = Graph.createVTable(
    rawVertices, eTable, numVertexPartitions)

  def vertices(): RDD[Vertex[VD]] = vTable.map { case(vid, (data, pids)) => new Vertex(vid, data) }

  def edges(): RDD[Edge[ED]] = eTable.mapPartitions { iter => iter.next._2.iterator }

  def edgesWithVertices(): RDD[EdgeWithVertices[VD, ED]] = {
    (new EdgeWithVerticesRDD(vTable, eTable)).mapPartitions { case(vmap, iter) => iter }
  }

  def mapPartitions[U: ClassManifest](
    f: (VertexHashMap, Iterator[EdgeWithVertices[VD, ED]]) => Iterator[U],
    preservesPartitioning: Boolean = false): RDD[U] = {
    (new EdgeWithVerticesRDD(vTable, eTable)).mapPartitions({ part =>
       val (vmap, iter) = part.next()
       iter.mapPartitions(f)
    }, preservesPartitioning)
  }

}


object Graph {

  /**
   * A partition of edges in 3 large columnar arrays.
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

  /**
   * Load a graph from a text file.
   */
  def textFile[ED: Manifest](sc: SparkContext,
    fname: String, edgeParser: Array[String] => ED) = {

    // Parse the edge data table
    val edges = sc.textFile(fname).map(
      line => {
        val lineArray = line.split("\\s+")
        if(lineArray.length < 2) {
          println("Invalid line: " + line)
          assert(false)
        }
        val source = lineArray(0)
        val target = lineArray(1)
        val tail = lineArray.drop(2)
        val edata = edgeParser(tail)
        Edge(source.trim.toInt, target.trim.toInt, edata)
      }).cache

    // Parse the vertex data table
    val vertices = edges.flatMap {
      case (source, target, _) => List((source, 1), (target, 1))
    }.reduceByKey(_ + _).map(pair => Vertex(piar._1, pair._2))

    val graph = new Graph[Int, ED](vertices, edges)

    println("Loaded graph:" +
      "\n\t#edges:    " + graph.numEdges +
      "\n\t#vertices: " + graph.numVertices)
    graph
  }


}

