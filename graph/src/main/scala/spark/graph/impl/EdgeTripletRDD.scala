package spark.graph.impl

import spark.{Aggregator, HashPartitioner, Partition, RDD, SparkEnv, TaskContext}
import spark.{Dependency, OneToOneDependency, ShuffleDependency}
import spark.SparkContext._
import spark.graph._


private[graph]
class EdgeTripletPartition(idx: Int, val vPart: Partition, val ePart: Partition)
  extends Partition {
  override val index: Int = idx
  override def hashCode(): Int = idx
}


/**
 * A RDD that brings together edge data with its associated vertex data.
 */
private[graph]
class EdgeTripletRDD[VD: ClassManifest, ED: ClassManifest](
    vTableReplicated: RDD[(Vid, VD)],
    eTable: RDD[(Pid, EdgePartition[ED])])
  extends RDD[(VertexHashMap[VD], Iterator[EdgeTriplet[VD, ED]])](eTable.context, Nil) {

  println(vTableReplicated.partitioner.get.numPartitions)
  println(eTable.partitioner.get.numPartitions)

  assert(vTableReplicated.partitioner == eTable.partitioner)

  override def getDependencies: List[Dependency[_]] = {
    List(new OneToOneDependency(eTable), new OneToOneDependency(vTableReplicated))
  }

  override def getPartitions = Array.tabulate[Partition](eTable.partitions.size) {
    i => new EdgeTripletPartition(i, eTable.partitions(i), vTableReplicated.partitions(i))
  }

  override val partitioner = eTable.partitioner

  override def getPreferredLocations(s: Partition) =
    eTable.preferredLocations(s.asInstanceOf[EdgeTripletPartition].ePart)

  override def compute(s: Partition, context: TaskContext)
    : Iterator[(VertexHashMap[VD], Iterator[EdgeTriplet[VD, ED]])] = {

    val split = s.asInstanceOf[EdgeTripletPartition]

    // Fetch the vertices and put them in a hashmap.
    // TODO: use primitive hashmaps for primitive VD types.
    val vmap = new VertexHashMap[VD]//(1000000)
    vTableReplicated.iterator(split.vPart, context).foreach { v => vmap.put(v._1, v._2) }

    val (pid, edgePartition) = eTable.iterator(split.ePart, context).next()
      .asInstanceOf[(Pid, EdgePartition[ED])]

    // Return an iterator that looks up the hash map to find matching vertices for each edge.
    val iter = new Iterator[EdgeTriplet[VD, ED]] {
      private var pos = 0
      private val e = new EdgeTriplet[VD, ED]
      e.src = new Vertex[VD]
      e.dst = new Vertex[VD]

      override def hasNext: Boolean = pos < edgePartition.size
      override def next() = {
        e.src.id = edgePartition.srcIds.getInt(pos)
        // assert(vmap.containsKey(e.src.id))
        e.src.data = vmap.get(e.src.id)

        e.dst.id = edgePartition.dstIds.getInt(pos)
        // assert(vmap.containsKey(e.dst.id))
        e.dst.data = vmap.get(e.dst.id)

        e.data = edgePartition.data(pos)
        pos += 1
        e
      }
    }
    Iterator((vmap, iter))
  }
}
