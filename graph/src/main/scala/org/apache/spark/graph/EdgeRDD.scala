package org.apache.spark.graph


import org.apache.spark.Partitioner
import org.apache.spark.{TaskContext, Partition, OneToOneDependency}
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

  override def compute(split: Partition, context: TaskContext): Iterator[Edge[ED]] = {
    val edgePartition = partitionsRDD.compute(split, context).next()._2
    edgePartition.iterator
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

  def mapEdgePartitions[ED2: ClassManifest](f: EdgePartition[ED] => EdgePartition[ED2])
    : EdgeRDD[ED2]= {
    val cleanF = sparkContext.clean(f)
    new EdgeRDD[ED2](partitionsRDD.mapPartitions({ iter =>
      val (pid, ep) = iter.next()
      Iterator(Tuple2(pid, cleanF(ep)))
    }, preservesPartitioning = true))
  }

  def zipEdgePartitions[T: ClassManifest, U: ClassManifest]
      (other: RDD[T])
      (f: (EdgePartition[ED], Iterator[T]) => Iterator[U]): RDD[U] = {
    val cleanF = sparkContext.clean(f)
    partitionsRDD.zipPartitions(other, preservesPartitioning = true) { (ePartIter, otherIter) =>
      val (_, edgePartition) = ePartIter.next()
      cleanF(edgePartition, otherIter)
    }
  }

  def collectVids(): RDD[Vid] = {
    partitionsRDD.flatMap { case (_, p) => Array.concat(p.srcIds, p.dstIds) }
  }

}
