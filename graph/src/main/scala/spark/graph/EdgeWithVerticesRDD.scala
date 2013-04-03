package spark.graph

import spark.{Aggregator, HashPartitioner, Partition, RDD, SparkEnv, TaskContext}
import spark.{Dependency, OneToOneDependency, ShuffleDependency}
import spark.SparkContext._
import spark.graph.Graph.EdgePartition


private[graph]
class EdgeWithVerticesPartition(idx: Int, val eTablePartition: Partition) extends Partition {
  override val index: Int = idx
  override def hashCode(): Int = idx
}


/**
 * A RDD that brings together edge data with its associated vertex data.
 */
private[graph]
class EdgeWithVerticesRDD[VD: ClassManifest, ED: ClassManifest](
    @transient vTable: RDD[(Vid, (VD, Array[Pid]))],
    eTable: RDD[(Pid, EdgePartition[ED])])
  extends RDD[(VertexHashMap[VD], Iterator[EdgeWithVertices[VD, ED]])](eTable.context, Nil) {

  @transient
  private val shuffleDependency = {
    // Join vid2pid and vTable, generate a shuffle dependency on the joined result, and get
    // the shuffle id so we can use it on the slave.
    val vTableReplicated = vTable.flatMap { case (vid, (vdata, pids)) =>
      pids.iterator.map { pid => (pid, (vid, vdata)) }
    }
    new ShuffleDependency(vTableReplicated, eTable.partitioner.get)
  }

  private val shuffleId = shuffleDependency.shuffleId

  override def getDependencies: List[Dependency[_]] = {
    List(new OneToOneDependency(eTable), shuffleDependency)
  }

  override def getPartitions = Array.tabulate[Partition](eTable.partitions.size) {
    i => new EdgeWithVerticesPartition(i, eTable.partitions(i)): Partition
  }

  override val partitioner = eTable.partitioner

  override def getPreferredLocations(s: Partition) =
    eTable.preferredLocations(s.asInstanceOf[EdgeWithVerticesPartition].eTablePartition)

  override def compute(s: Partition, context: TaskContext)
    : Iterator[(VertexHashMap[VD], Iterator[EdgeWithVertices[VD, ED]])] = {

    val split = s.asInstanceOf[EdgeWithVerticesPartition]

    // Fetch the vertices and put them in a hashmap.
    // TODO: use primitive hashmaps for primitive VD types.
    val vmap = new VertexHashMap[VD]//(1000000)
    val fetcher = SparkEnv.get.shuffleFetcher
    fetcher.fetch[Pid, (Vid, VD)](shuffleId, split.index, context.taskMetrics).foreach {
      case (pid, (vid, vdata)) => vmap.put(vid, vdata)
    }

    val (pid, edgePartition) = eTable.iterator(split.eTablePartition, context).next()
      .asInstanceOf[(Pid, EdgePartition[ED])]

    // Return an iterator that looks up the hash map to find matching vertices for each edge.
    val iter = new Iterator[EdgeWithVertices[VD, ED]] {
      private var pos = 0
      private val e = new EdgeWithVertices[VD, ED]
      e.src = new Vertex[VD]
      e.dst = new Vertex[VD]

      override def hasNext: Boolean = pos < edgePartition.size
      override def next() = {
        e.src.id = edgePartition.srcIds.getInt(pos)
        e.src.data = vmap.get(e.src.id)
        e.dst.id = edgePartition.dstIds.getInt(pos)
        e.dst.data = vmap.get(e.dst.id)
        e.data = edgePartition.data(pos)
        pos += 1
        e
      }
    }
    Iterator((vmap, iter))
  }
}
