package spark

@serializable
abstract class Dependency[T](val rdd: RDD[T], val isShuffle: Boolean)

abstract class NarrowDependency[T](rdd: RDD[T])
extends Dependency(rdd, false) {
  def getParents(outputPartition: Int): Seq[Int]
}

class ShuffleDependency[K, V, C](
  val shuffleId: Int,
  rdd: RDD[(K, V)],
  val aggregator: Aggregator[K, V, C],
  val partitioner: Partitioner
) extends Dependency(rdd, true)

class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int) = List(partitionId)
}

class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int) = {
    if (partitionId >= outStart && partitionId < outStart + length)
      List(partitionId - outStart + inStart)
    else
      Nil
  }
}
