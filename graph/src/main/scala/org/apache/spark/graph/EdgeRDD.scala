package org.apache.spark.graph

import org.apache.spark.{OneToOneDependency, Partition, Partitioner, TaskContext}
import org.apache.spark.graph.impl.EdgePartition
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


class EdgeRDD[@specialized ED: ClassManifest](
    val partitionsRDD: RDD[(Pid, EdgePartition[ED])])
  extends RDD[Edge[ED]](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  partitionsRDD.setName("EdgeRDD")

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  /**
   * If partitionsRDD already has a partitioner, use it. Otherwise assume that the Pids in
   * partitionsRDD correspond to the actual partitions and create a new partitioner that allows
   * co-partitioning with partitionsRDD.
   */
  override val partitioner =
    partitionsRDD.partitioner.orElse(Some(Partitioner.defaultPartitioner(partitionsRDD)))

  override def compute(part: Partition, context: TaskContext): Iterator[Edge[ED]] = {
    firstParent[(Pid, EdgePartition[ED])].iterator(part, context).next._2.iterator
  }

  override def collect(): Array[Edge[ED]] = this.map(_.copy()).collect()

  /**
   * Caching a VertexRDD causes the index and values to be cached separately.
   */
  override def persist(newLevel: StorageLevel): EdgeRDD[ED] = {
    partitionsRDD.persist(newLevel)
    this
  }

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  override def persist(): EdgeRDD[ED] = persist(StorageLevel.MEMORY_ONLY)

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  override def cache(): EdgeRDD[ED] = persist()

  def mapEdgePartitions[ED2: ClassManifest](f: (Pid, EdgePartition[ED]) => EdgePartition[ED2])
    : EdgeRDD[ED2] = {
//       iter => iter.map { case (pid, ep) => (pid, f(ep)) }
    new EdgeRDD[ED2](partitionsRDD.mapPartitions({ iter =>
      val (pid, ep) = iter.next()
      Iterator(Tuple2(pid, f(pid, ep)))
    }, preservesPartitioning = true))
  }

  def zipEdgePartitions[T: ClassManifest, U: ClassManifest]
      (other: RDD[T])
      (f: (Pid, EdgePartition[ED], Iterator[T]) => Iterator[U]): RDD[U] = {
    partitionsRDD.zipPartitions(other, preservesPartitioning = true) { (ePartIter, otherIter) =>
      val (pid, edgePartition) = ePartIter.next()
      f(pid, edgePartition, otherIter)
    }
  }

  def zipEdgePartitions[ED2: ClassManifest, ED3: ClassManifest]
      (other: EdgeRDD[ED2])
      (f: (Pid, EdgePartition[ED], EdgePartition[ED2]) => EdgePartition[ED3]): EdgeRDD[ED3] = {
    new EdgeRDD[ED3](partitionsRDD.zipPartitions(other.partitionsRDD, preservesPartitioning = true) {
      (thisIter, otherIter) =>
        val (pid, thisEPart) = thisIter.next()
        val (_, otherEPart) = otherIter.next()
      Iterator(Tuple2(pid, f(pid, thisEPart, otherEPart)))
    })
  }

  def innerJoin[ED2: ClassManifest, ED3: ClassManifest]
      (other: EdgeRDD[ED2])
      (f: (Vid, Vid, ED, ED2) => ED3): EdgeRDD[ED3] = {
    val ed2Manifest = classManifest[ED2]
    val ed3Manifest = classManifest[ED3]
    zipEdgePartitions(other) { (pid, thisEPart, otherEPart) =>
      thisEPart.innerJoin(otherEPart)(f)(ed2Manifest, ed3Manifest)
    }
  }

  def collectVids(): RDD[Vid] = {
    partitionsRDD.flatMap { case (_, p) => Array.concat(p.srcIds, p.dstIds) }
  }

}
