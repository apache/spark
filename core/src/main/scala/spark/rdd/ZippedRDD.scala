package spark.rdd

import spark.Dependency
import spark.OneToOneDependency
import spark.RDD
import spark.SparkContext
import spark.Split

private[spark] class ZippedSplit[T: ClassManifest, U: ClassManifest](
    idx: Int, 
    rdd1: RDD[T],
    rdd2: RDD[U],
    split1: Split,
    split2: Split)
  extends Split
  with Serializable {
  
  def iterator(): Iterator[(T, U)] = rdd1.iterator(split1).zip(rdd2.iterator(split2))

  def preferredLocations(): Seq[String] =
    rdd1.preferredLocations(split1).intersect(rdd2.preferredLocations(split2))

  override val index: Int = idx
}

class ZippedRDD[T: ClassManifest, U: ClassManifest](
    sc: SparkContext,
    @transient rdd1: RDD[T],
    @transient rdd2: RDD[U])
  extends RDD[(T, U)](sc)
  with Serializable {

  @transient
  val splits_ : Array[Split] = {
    if (rdd1.splits.size != rdd2.splits.size) {
      throw new IllegalArgumentException("Can't zip RDDs with unequal numbers of partitions")
    }
    val array = new Array[Split](rdd1.splits.size)
    for (i <- 0 until rdd1.splits.size) {
      array(i) = new ZippedSplit(i, rdd1, rdd2, rdd1.splits(i), rdd2.splits(i))
    }
    array
  }

  override def splits = splits_

  @transient
  override val dependencies = List(new OneToOneDependency(rdd1), new OneToOneDependency(rdd2))
  
  override def compute(s: Split): Iterator[(T, U)] = s.asInstanceOf[ZippedSplit[T, U]].iterator()

  override def preferredLocations(s: Split): Seq[String] =
    s.asInstanceOf[ZippedSplit[T, U]].preferredLocations()
}
