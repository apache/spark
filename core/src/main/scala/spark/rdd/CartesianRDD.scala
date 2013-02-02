package spark.rdd

import java.io.{ObjectOutputStream, IOException}
import spark._


private[spark]
class CartesianSplit(
    idx: Int,
    @transient rdd1: RDD[_],
    @transient rdd2: RDD[_],
    s1Index: Int,
    s2Index: Int
  ) extends Split {
  var s1 = rdd1.splits(s1Index)
  var s2 = rdd2.splits(s2Index)
  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent split at the time of task serialization
    s1 = rdd1.splits(s1Index)
    s2 = rdd2.splits(s2Index)
    oos.defaultWriteObject()
  }
}

private[spark]
class CartesianRDD[T: ClassManifest, U:ClassManifest](
    sc: SparkContext,
    var rdd1 : RDD[T],
    var rdd2 : RDD[U])
  extends RDD[Pair[T, U]](sc, Nil)
  with Serializable {

  val numSplitsInRdd2 = rdd2.splits.size

  override def getSplits: Array[Split] = {
    // create the cross product split
    val array = new Array[Split](rdd1.splits.size * rdd2.splits.size)
    for (s1 <- rdd1.splits; s2 <- rdd2.splits) {
      val idx = s1.index * numSplitsInRdd2 + s2.index
      array(idx) = new CartesianSplit(idx, rdd1, rdd2, s1.index, s2.index)
    }
    array
  }

  override def getPreferredLocations(split: Split) = {
    val currSplit = split.asInstanceOf[CartesianSplit]
    rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)
  }

  override def compute(split: Split, context: TaskContext) = {
    val currSplit = split.asInstanceOf[CartesianSplit]
    for (x <- rdd1.iterator(currSplit.s1, context);
      y <- rdd2.iterator(currSplit.s2, context)) yield (x, y)
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id / numSplitsInRdd2)
    },
    new NarrowDependency(rdd2) {
      def getParents(id: Int): Seq[Int] = List(id % numSplitsInRdd2)
    }
  )

  override def clearDependencies() {
    rdd1 = null
    rdd2 = null
  }
}
