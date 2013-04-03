package spark.graph

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import it.unimi.dsi.fastutil.ints.IntArrayList

import spark.{ClosureCleaner, HashPartitioner, RDD}
import spark.SparkContext
import spark.SparkContext._
import spark.graph.Graph.EdgePartition
import spark.storage.StorageLevel


case class Vertex[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) VD] (
  var id: Vid = 0,
  var data: VD = nullValue[VD]) {

  def this(tuple: Tuple2[Vid, VD]) = this(tuple._1, tuple._2)
}


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
class Graph[VD: ClassManifest, ED: ClassManifest] protected (
  _rawVertices: RDD[Vertex[VD]],
  _rawEdges: RDD[Edge[ED]],
  _rawVTable: RDD[(Vid, (VD, Array[Pid]))],
  _rawETable: RDD[(Pid, EdgePartition[ED])]) {

  def this(vertices: RDD[Vertex[VD]], edges: RDD[Edge[ED]]) = this(vertices, edges, null, null)

  protected var _cached = false

  def cache(): Graph[VD, ED] = {
    eTable.cache()
    vTable.cache()
    _cached = true
    this
  }

  /// Todo:  Should theses be set on construction and passed onto derived graphs?
  var numEdgePartitions = 5
  var numVertexPartitions = 5

  /// Todo:  Should these be passed onto derived graphs?
  protected val vertexPartitioner = new HashPartitioner(numVertexPartitions)
  protected val edgePartitioner = new HashPartitioner(numEdgePartitions)



  lazy val numEdges = edges.count()
  lazy val numVertices = vertices.count()
  lazy val inDegrees = edges.map{ case Edge(src, target, _) => (target, 1) }.reduceByKey(_+_)
  lazy val outDegrees = edges.map{ case Edge(src, target, _) => (src, 1) }.reduceByKey(_+_)


  protected lazy val eTable: RDD[(Pid, EdgePartition[ED])] = {
    if (_rawETable == null) {
      Graph.createETable(_rawEdges, numEdgePartitions)
    } else {
      _rawETable
    }
  }

  protected lazy val vTable: RDD[(Vid, (VD, Array[Pid]))] = {
    if (_rawVTable == null) {
      Graph.createVTable(_rawVertices, eTable, numVertexPartitions)
    } else {
      _rawVTable
    }
  }

  def vertices: RDD[Vertex[VD]] = {
    if (!_cached && _rawVertices != null) {
      _rawVertices
    } else {
      vTable.map { case(vid, (data, pids)) => new Vertex(vid, data) }
    }
  }

  def edges: RDD[Edge[ED]] = {
    if (!_cached && _rawEdges != null) {
      _rawEdges
    } else {
      eTable.mapPartitions { iter => iter.next._2.iterator }
    }
  }

  def edgesWithVertices: RDD[EdgeWithVertices[VD, ED]] = {
    (new EdgeWithVerticesRDD[VD, ED](vTable, eTable)).mapPartitions { part => part.next._2 }
  }

  def mapVertices[VD2: ClassManifest](f: (Vertex[VD]) => Vertex[VD2]) = {
    ClosureCleaner.clean(f)
    new Graph(vertices.map(f), edges)
  }

  def mapEdges[ED2: ClassManifest](f: (Edge[ED]) => Edge[ED2]) = {
    ClosureCleaner.clean(f)
    new Graph(vertices, edges.map(f))
  }

  def updateVertices[U: ClassManifest, VD2: ClassManifest](
      updates: RDD[(Vid, U)],
      updateFunc: (Vertex[VD], Seq[U]) => VD2)
    : Graph[VD2, ED] = {

    ClosureCleaner.clean(updateFunc)

    val joined: RDD[(Vid, ((VD, Array[Pid]), Option[Seq[U]]))] =
      vTable.leftOuterJoin(updates.groupByKey(vertexPartitioner))

    val newVTable = joined.mapPartitions({ iter =>
      iter.map { case (vid, ((vdata, pids), updates)) =>
        val u = if (updates.isDefined) updates.get else Seq.empty
        val newVdata = updateFunc(Vertex(vid, vdata), u)
        (vid, (newVdata, pids))
      }
    }, preservesPartitioning = true).cache()

    new Graph(null, null, newVTable, eTable)
  }

  def mapPartitions[U: ClassManifest](
    f: (VertexHashMap[VD], Iterator[EdgeWithVertices[VD, ED]]) => Iterator[U],
    preservesPartitioning: Boolean = false): RDD[U] = {
    (new EdgeWithVerticesRDD(vTable, eTable)).mapPartitions({ part =>
       val (vmap, iter) = part.next()
       f(vmap, iter)
    }, preservesPartitioning)
  }

}


object Graph {

  /**
   * Load an edge list from file initializing the Graph RDD
   */
  def textFile[ED: ClassManifest](sc: SparkContext,
    fname: String, edgeParser: Array[String] => ED) = {

    // Parse the edge data table
    val edges = sc.textFile(fname).map { line =>
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
    }.cache()

    // Parse the vertex data table
    val vertices = edges.flatMap { edge => List((edge.src, 1), (edge.dst, 1)) }
      .reduceByKey(_ + _)
      .map(new Vertex(_))

    val graph = new Graph[Int, ED](vertices, edges)
    graph.cache()

    println("Loaded graph:" +
      "\n\t#edges:    " + graph.edges.count +
      "\n\t#vertices: " + graph.vertices.count)

    graph
  }


  /**
   * A partition of edges in 3 large columnar arrays.
   */
  private[graph]
  class EdgePartition[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED:ClassManifest]
  {
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

      override def hasNext: Boolean = pos < EdgePartition.this.size

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
  def createVTable[VD: ClassManifest, ED: ClassManifest](
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
  def createETable[ED: ClassManifest](edges: RDD[Edge[ED]], numPartitions: Int)
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
      case Edge(source, target, _) => List((source, 1), (target, 1))
    }.reduceByKey(_ + _).map(pair => Vertex(pair._1, pair._2))

    val graph = new Graph[Int, ED](vertices, edges)

    println("Loaded graph:" +
      "\n\t#edges:    " + graph.numEdges +
      "\n\t#vertices: " + graph.numVertices)
    graph
  }


}

