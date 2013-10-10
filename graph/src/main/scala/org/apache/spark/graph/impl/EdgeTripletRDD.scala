package org.apache.spark.graph.impl

import scala.collection.mutable

import org.apache.spark.Aggregator
import org.apache.spark.Partition
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.Dependency
import org.apache.spark.OneToOneDependency
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkContext._
import org.apache.spark.graph._


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

  //println("ddshfkdfhds" + vTableReplicated.partitioner.get.numPartitions)
  //println("9757984589347598734549" + eTable.partitioner.get.numPartitions)

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
        e.src.id = edgePartition.srcIds.getLong(pos)
        // assert(vmap.containsKey(e.src.id))
        e.src.data = vmap.get(e.src.id)

        e.dst.id = edgePartition.dstIds.getLong(pos)
        // assert(vmap.containsKey(e.dst.id))
        e.dst.data = vmap.get(e.dst.id)

        //println("Iter called: " + pos)
        e.data = edgePartition.data(pos)
        pos += 1
        e
      }

      override def toList: List[EdgeTriplet[VD, ED]] = {
        val lb = new mutable.ListBuffer[EdgeTriplet[VD,ED]]
        for (i <- (0 until edgePartition.size)) {
          val currentEdge = new EdgeTriplet[VD, ED]
          currentEdge.src = new Vertex[VD]
          currentEdge.dst = new Vertex[VD]
          currentEdge.src.id = edgePartition.srcIds.getLong(i)
          // assert(vmap.containsKey(e.src.id))
          currentEdge.src.data = vmap.get(currentEdge.src.id)

          currentEdge.dst.id = edgePartition.dstIds.getLong(i)
          // assert(vmap.containsKey(e.dst.id))
          currentEdge.dst.data = vmap.get(currentEdge.dst.id)

          currentEdge.data = edgePartition.data(i)
          //println("Iter: " + pos + " " + e.src.id + " " + e.dst.id + " " + e.data)
          //println("List: " + i + " " + currentEdge.src.id + " " + currentEdge.dst.id + " " + currentEdge.data)
          lb += currentEdge
        }
        lb.toList
      }
    }
    Iterator((vmap, iter))
  }
}
