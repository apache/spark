package spark.rdd

import spark.NarrowDependency
import spark.RDD
import spark.SparkContext
import spark.Split

private[spark]
class CartesianSplit(idx: Int, val s1: Split, val s2: Split) extends Split with Serializable {
  override val index: Int = idx
}

private[spark]
class CartesianRDD[T: ClassManifest, U:ClassManifest](
    sc: SparkContext,
    rdd1: RDD[T],
    rdd2: RDD[U])
  extends RDD[Pair[T, U]](sc)
  with Serializable {
  
  val numSplitsInRdd2 = rdd2.splits.size
  
  @transient
  val splits_ = {
    // create the cross product split
    val array = new Array[Split](rdd1.splits.size * rdd2.splits.size)
    for (s1 <- rdd1.splits; s2 <- rdd2.splits) {
      val idx = s1.index * numSplitsInRdd2 + s2.index
      array(idx) = new CartesianSplit(idx, s1, s2)
    }
    array
  }

  override def splits = splits_

  override def preferredLocations(split: Split) = {
    val currSplit = split.asInstanceOf[CartesianSplit]
    rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)
  }

  override def compute(split: Split) = {
    val currSplit = split.asInstanceOf[CartesianSplit]
    for (x <- rdd1.iterator(currSplit.s1); y <- rdd2.iterator(currSplit.s2)) yield (x, y)
  }
  
  override val dependencies = List(
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id / numSplitsInRdd2)
    },
    new NarrowDependency(rdd2) {
      def getParents(id: Int): Seq[Int] = List(id % numSplitsInRdd2)
    }
  )
}
