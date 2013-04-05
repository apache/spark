package spark.graph

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import it.unimi.dsi.fastutil.ints.IntArrayList

import spark.{ClosureCleaner, HashPartitioner, Partitioner, RDD}
import spark.SparkContext
import spark.SparkContext._
import spark.graph.Graph.EdgePartition
import spark.storage.StorageLevel


case class Vertex[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) VD] (
  var id: Vid = 0,
  var data: VD = nullValue[VD]) {

  def this(tuple: Tuple2[Vid, VD]) = this(tuple._1, tuple._2)

  def tuple = (id, data)
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

  def relativeDirection(vid: Vid): EdgeDirection = {
    if (vid == src.id) EdgeDirection.Out else EdgeDirection.In
  }
}


/**
 * A Graph RDD that supports computation on graphs.
 */
class Graph[VD: ClassManifest, ED: ClassManifest] protected (
  val numVertexPartitions: Int,
  val numEdgePartitions: Int,
  _rawVertices: RDD[Vertex[VD]],
  _rawEdges: RDD[Edge[ED]],
  _rawVTable: RDD[(Vid, (VD, Array[Pid]))],
  _rawETable: RDD[(Pid, EdgePartition[ED])]) {

  def this(vertices: RDD[Vertex[VD]], edges: RDD[Edge[ED]]) = {
    this(vertices.partitions.size, edges.partitions.size, vertices, edges, null, null)
  }

  def withPartitioner(numVertexPartitions: Int, numEdgePartitions: Int): Graph[VD, ED] = {
    if (_cached) {
      (new Graph(numVertexPartitions, numEdgePartitions, null, null, _rawVTable, _rawETable))
        .cache()
    } else {
      new Graph(numVertexPartitions, numEdgePartitions, _rawVertices, _rawEdges, null, null)
    }
  }

  def withVertexPartitioner(numVertexPartitions: Int) = {
    withPartitioner(numVertexPartitions, numEdgePartitions)
  }

  def withEdgePartitioner(numEdgePartitions: Int) = {
    withPartitioner(numVertexPartitions, numEdgePartitions)
  }

  protected var _cached = false

  def cache(): Graph[VD, ED] = {
    eTable.cache()
    vTable.cache()
    _cached = true
    this
  }

  /** Return a RDD of vertices. */
  def vertices: RDD[Vertex[VD]] = {
    if (!_cached && _rawVertices != null) {
      _rawVertices
    } else {
      vTable.map { case(vid, (data, pids)) => new Vertex(vid, data) }
    }
  }

  /** Return a RDD of edges. */
  def edges: RDD[Edge[ED]] = {
    if (!_cached && _rawEdges != null) {
      _rawEdges
    } else {
      eTable.mapPartitions { iter => iter.next._2.iterator }
    }
  }

  /** Return a RDD that brings edges with its source and destination vertices together. */
  def edgesWithVertices: RDD[EdgeWithVertices[VD, ED]] = {
    (new EdgeWithVerticesRDD(vTableReplicated, eTable)).mapPartitions { part => part.next._2 }
  }

  lazy val numEdges: Long = edges.count()

  lazy val numVertices: Long = vertices.count()

  lazy val inDegrees = mapReduceNeighborhood[Vid]((vid, edge) => 1, _+_, 0, EdgeDirection.In)

  lazy val outDegrees = mapReduceNeighborhood[Vid]((vid,edge) => 1, _+_, 0, EdgeDirection.Out)

  lazy val degrees = mapReduceNeighborhood[Vid]((vid,edge) => 1, _+_, 0, EdgeDirection.Both)

  /** Return a new graph with its edge directions reversed. */
  lazy val reverse: Graph[VD,ED] = {
    newGraph(vertices, edges.map{ case Edge(s, t, e) => Edge(t, s, e) })
  }

  def collectNeighborIds(edgeDirection: EdgeDirection) : RDD[(Vid, Array[Vid])] = {
    mapReduceNeighborhood[Array[Vid]](
      (vid, edge) => Array(edge.otherVertex(vid).id),
      (a, b) => a ++ b,
      Array.empty[Vid],
      edgeDirection)
  }

  def mapVertices[VD2: ClassManifest](f: Vertex[VD] => Vertex[VD2]): Graph[VD2, ED] = {
    newGraph(vertices.map(f), edges)
  }

  def mapEdges[ED2: ClassManifest](f: Edge[ED] => Edge[ED2]): Graph[VD, ED2] = {
    newGraph(vertices, edges.map(f))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Lower level transformation methods
  //////////////////////////////////////////////////////////////////////////////////////////////////

  def mapReduceNeighborhood[VD2: ClassManifest](
    mapFunc: (Vid, EdgeWithVertices[VD, ED]) => VD2,
    reduceFunc: (VD2, VD2) => VD2,
    default: VD2,
    gatherDirection: EdgeDirection): RDD[(Vid, VD2)] = {

    ClosureCleaner.clean(mapFunc)
    ClosureCleaner.clean(reduceFunc)

    val newVTable = vTableReplicated.mapPartitions({ part =>
        part.map { v => (v._1, MutableTuple2(v._2, default)) }
      }, preservesPartitioning = true)

    (new EdgeWithVerticesRDD[MutableTuple2[VD, VD2], ED](newVTable, eTable))
      .mapPartitions { part =>
        val (vmap, edges) = part.next()
        val edgeSansAcc = new EdgeWithVertices[VD, ED]()
        edgeSansAcc.src = new Vertex[VD]
        edgeSansAcc.dst = new Vertex[VD]
        edges.foreach { e: EdgeWithVertices[MutableTuple2[VD, VD2], ED] =>
          edgeSansAcc.data = e.data
          edgeSansAcc.src.data = e.src.data._1
          edgeSansAcc.dst.data = e.dst.data._1
          edgeSansAcc.src.id = e.src.id
          edgeSansAcc.dst.id = e.dst.id
          if (gatherDirection == EdgeDirection.In || gatherDirection == EdgeDirection.Both) {
            e.dst.data._2 = reduceFunc(e.dst.data._2, mapFunc(edgeSansAcc.dst.id, edgeSansAcc))
          }
          if (gatherDirection == EdgeDirection.Out || gatherDirection == EdgeDirection.Both) {
            e.src.data._2 = reduceFunc(e.src.data._2, mapFunc(edgeSansAcc.src.id, edgeSansAcc))
          }
        }
        vmap.int2ObjectEntrySet().fastIterator().map{ entry =>
          (entry.getIntKey(), entry.getValue()._2)
        }
      }
      .combineByKey((v: VD2) => v, reduceFunc, null, vertexPartitioner, false)
  }

  /**
   * Same as mapReduceNeighborhood but map function can return none and there is no default value.
   * As a consequence, the resulting table may be much smaller than the set of vertices.
   */
  def flatMapReduceNeighborhood[VD2: ClassManifest](
    mapFunc: (Vid, EdgeWithVertices[VD, ED]) => Option[VD2],
    reduceFunc: (VD2, VD2) => VD2,
    gatherDirection: EdgeDirection): RDD[(Vid, VD2)] = {

    ClosureCleaner.clean(mapFunc)
    ClosureCleaner.clean(reduceFunc)

    val newVTable = vTableReplicated.mapPartitions({ part =>
        part.map { v => (v._1, MutableTuple2(v._2, Option.empty[VD2])) }
      }, preservesPartitioning = true)

    (new EdgeWithVerticesRDD[MutableTuple2[VD, Option[VD2]], ED](newVTable, eTable))
      .mapPartitions { part =>
        val (vmap, edges) = part.next()
        val edgeSansAcc = new EdgeWithVertices[VD, ED]()
        edgeSansAcc.src = new Vertex[VD]
        edgeSansAcc.dst = new Vertex[VD]
        edges.foreach { e: EdgeWithVertices[MutableTuple2[VD, Option[VD2]], ED] =>
          edgeSansAcc.data = e.data
          edgeSansAcc.src.data = e.src.data._1
          edgeSansAcc.dst.data = e.dst.data._1
          edgeSansAcc.src.id = e.src.id
          edgeSansAcc.dst.id = e.dst.id
          if (gatherDirection == EdgeDirection.In || gatherDirection == EdgeDirection.Both) {
            e.dst.data._2 =
              if (e.dst.data._2.isEmpty) {
                mapFunc(edgeSansAcc.dst.id, edgeSansAcc)
              } else {
                val tmp = mapFunc(edgeSansAcc.dst.id, edgeSansAcc)
                if (!tmp.isEmpty) Some(reduceFunc(e.dst.data._2.get, tmp.get)) else e.dst.data._2
              }
          }
          if (gatherDirection == EdgeDirection.Out || gatherDirection == EdgeDirection.Both) {
            e.dst.data._2 =
              if (e.dst.data._2.isEmpty) {
                mapFunc(edgeSansAcc.src.id, edgeSansAcc)
              } else {
                val tmp = mapFunc(edgeSansAcc.src.id, edgeSansAcc)
                if (!tmp.isEmpty) Some(reduceFunc(e.src.data._2.get, tmp.get)) else e.src.data._2
              }
          }
        }
        vmap.int2ObjectEntrySet().fastIterator().filter(!_.getValue()._2.isEmpty).map{ entry =>
          (entry.getIntKey(), entry.getValue()._2)
        }
      }
      .map{ case (vid, aOpt) => (vid, aOpt.get) }
      .combineByKey((v: VD2) => v, reduceFunc, null, vertexPartitioner, false)
  }

  def updateVertices[U: ClassManifest, VD2: ClassManifest](
      updates: RDD[(Vid, U)],
      updateFunc: (Vertex[VD], Option[U]) => VD2)
    : Graph[VD2, ED] = {

    ClosureCleaner.clean(updateFunc)

    val newVTable = vTable.leftOuterJoin(updates).mapPartitions({ iter =>
      iter.map { case (vid, ((vdata, pids), update)) =>
        val newVdata = updateFunc(Vertex(vid, vdata), update)
        (vid, (newVdata, pids))
      }
    }, preservesPartitioning = true).cache()

    new Graph(newVTable.partitioner.size, eTable.partitioner.size, null, null, newVTable, eTable)
  }

  // def mapPartitions[U: ClassManifest](
  //   f: (VertexHashMap[VD], Iterator[EdgeWithVertices[VD, ED]]) => Iterator[U],
  //   preservesPartitioning: Boolean = false): RDD[U] = {
  //   (new EdgeWithVerticesRDD(vTable, eTable)).mapPartitions({ part =>
  //      val (vmap, iter) = part.next()
  //      f(vmap, iter)
  //   }, preservesPartitioning)
  // }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Internals hidden from callers
  //////////////////////////////////////////////////////////////////////////////////////////////////

  // TODO: Support non-hash partitioning schemes.
  protected val vertexPartitioner = new HashPartitioner(numVertexPartitions)
  protected val edgePartitioner = new HashPartitioner(numEdgePartitions)

  /** Create a new graph but keep the current partitioning scheme. */
  protected def newGraph[VD2: ClassManifest, ED2: ClassManifest](
    vertices: RDD[Vertex[VD2]], edges: RDD[Edge[ED2]]): Graph[VD2, ED2] = {
    (new Graph[VD2, ED2](vertices, edges)).withPartitioner(numVertexPartitions, numEdgePartitions)
  }

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

  protected lazy val vTableReplicated: RDD[(Vid, VD)] = {
    // Join vid2pid and vTable, generate a shuffle dependency on the joined result, and get
    // the shuffle id so we can use it on the slave.
    vTable
      .flatMap { case (vid, (vdata, pids)) => pids.iterator.map { pid => (pid, (vid, vdata)) } }
      .partitionBy(edgePartitioner)
      .mapPartitions(
        { part => part.map { case(pid, (vid, vdata)) => (vid, vdata) } },
        preservesPartitioning = true)
  }
}


object Graph {

  /**
   * Load an edge list from file initializing the Graph RDD
   */
  def textFile[ED: ClassManifest](sc: SparkContext, fname: String, edgeParser: Array[String] => ED) = {

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

    val graph = fromEdges(edges)
    println("Loaded graph:" +
      "\n\t#edges:    " + graph.numEdges +
      "\n\t#vertices: " + graph.numVertices)

    graph
  }

  def fromEdges[ED: ClassManifest](edges: RDD[Edge[ED]]): Graph[Int, ED] = {
    val vertices = edges.flatMap { edge => List((edge.src, 1), (edge.dst, 1)) }
      .reduceByKey(_ + _)
      .map{ case (vid, degree) => Vertex(vid, degree) }
    (new Graph[Int, ED](vertices, edges))
  }

  /**
   * Make k-cycles
   */
  def kCycles(sc: SparkContext, numCycles: Int = 3, size: Int = 3) = {
    // Construct the edges
    val edges = sc.parallelize(for (i <- 0 until numCycles; j <- 0 until size) yield {
      val offset = i * numCycles
      val source = offset + j
      val target = offset + ((j + 1) % size)
      Edge(source, target, i * numCycles + j)
    })
    // Change vertex data to be the lowest vertex id of the vertex in that cycle
    val graph = fromEdges(edges).mapVertices{
      case Vertex(id, degree) => Vertex(id, (id/numCycles) * numCycles)
    }
    graph
  }

  /**
   * Make a regular grid graph
   **/
  def grid(sc: SparkContext, numRows: Int = 5, numCols: Int = 5) = {
    def coord(vid: Int) = (vid % numRows, vid / numRows)
    val vertices = sc.parallelize( 0 until (numRows * numCols) ).map(
      vid => Vertex(vid, coord(vid)))
    def index(r: Int, c:Int) = (r + c * numRows)
    val edges = vertices.flatMap{ case Vertex(vid, (r,c)) =>
      (if(r+1 < numRows) List(Edge(vid, index(r+1,c), 1.0F)) else List.empty) ++
        (if(c+1 < numCols) List(Edge(vid, index(r,c+1), 1.0F)) else List.empty)
    }
    new Graph(vertices, edges)
  }


  /**
   * A partition of edges in 3 large columnar arrays.
   */
  private[graph]
  class EdgePartition[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED: ClassManifest]
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

  /**
   * Create the edge table RDD, which is much more efficient for Java heap storage than the
   * normal edges data structure (RDD[(Vid, Vid, ED)]).
   *
   * The edge table contains multiple partitions, and each partition contains only one RDD
   * key-value pair: the key is the partition id, and the value is an EdgePartition object
   * containing all the edges in a partition.
   */
  protected def createETable[ED: ClassManifest](edges: RDD[Edge[ED]], numPartitions: Int)
    : RDD[(Pid, EdgePartition[ED])] = {
    edges
      .map { e =>
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

  protected def createVTable[VD: ClassManifest, ED: ClassManifest](
      vertices: RDD[Vertex[VD]],
      eTable: RDD[(Pid, EdgePartition[ED])],
      numPartitions: Int)
    : RDD[(Vid, (VD, Array[Pid]))] = {
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
}
