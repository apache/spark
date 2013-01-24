package spark.rdd

import spark.{OneToOneDependency, RDD, SparkContext, Split, TaskContext}
import java.io.{ObjectOutputStream, IOException}


private[spark] class ZippedSplit[T: ClassManifest, U: ClassManifest](
    idx: Int,
    @transient rdd1: RDD[T],
    @transient rdd2: RDD[U]
  ) extends Split {

  var split1 = rdd1.splits(idx)
  var split2 = rdd1.splits(idx)
  override val index: Int = idx

  def splits = (split1, split2)

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent split at the time of task serialization
    split1 = rdd1.splits(idx)
    split2 = rdd2.splits(idx)
    oos.defaultWriteObject()
  }
}

class ZippedRDD[T: ClassManifest, U: ClassManifest](
    sc: SparkContext,
    var rdd1: RDD[T],
    var rdd2: RDD[U])
  extends RDD[(T, U)](sc, List(new OneToOneDependency(rdd1), new OneToOneDependency(rdd2)))
  with Serializable {

  // TODO: FIX THIS.

  @transient var splits_ : Array[Split] = {
    if (rdd1.splits.size != rdd2.splits.size) {
      throw new IllegalArgumentException("Can't zip RDDs with unequal numbers of partitions")
    }
    val array = new Array[Split](rdd1.splits.size)
    for (i <- 0 until rdd1.splits.size) {
      array(i) = new ZippedSplit(i, rdd1, rdd2)
    }
    array
  }

  override def getSplits = splits_

  override def compute(s: Split, context: TaskContext): Iterator[(T, U)] = {
    val (split1, split2) = s.asInstanceOf[ZippedSplit[T, U]].splits
    rdd1.iterator(split1, context).zip(rdd2.iterator(split2, context))
  }

  override def getPreferredLocations(s: Split): Seq[String] = {
    val (split1, split2) = s.asInstanceOf[ZippedSplit[T, U]].splits
    rdd1.preferredLocations(split1).intersect(rdd2.preferredLocations(split2))
  }

  override def clearDependencies() {
    splits_ = null
    rdd1 = null
    rdd2 = null
  }
}
