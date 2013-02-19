package spark.rdd

import spark.{OneToOneDependency, RDD, SparkContext, Partition, TaskContext}
import java.io.{ObjectOutputStream, IOException}


private[spark] class ZippedPartition[T: ClassManifest, U: ClassManifest](
    idx: Int,
    @transient rdd1: RDD[T],
    @transient rdd2: RDD[U]
  ) extends Partition {

  var split1 = rdd1.partitions(idx)
  var split2 = rdd1.partitions(idx)
  override val index: Int = idx

  def splits = (split1, split2)

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent split at the time of task serialization
    split1 = rdd1.partitions(idx)
    split2 = rdd2.partitions(idx)
    oos.defaultWriteObject()
  }
}

class ZippedRDD[T: ClassManifest, U: ClassManifest](
    sc: SparkContext,
    var rdd1: RDD[T],
    var rdd2: RDD[U])
  extends RDD[(T, U)](sc, List(new OneToOneDependency(rdd1), new OneToOneDependency(rdd2))) {

  override def getPartitions: Array[Partition] = {
    if (rdd1.partitions.size != rdd2.partitions.size) {
      throw new IllegalArgumentException("Can't zip RDDs with unequal numbers of partitions")
    }
    val array = new Array[Partition](rdd1.partitions.size)
    for (i <- 0 until rdd1.partitions.size) {
      array(i) = new ZippedPartition(i, rdd1, rdd2)
    }
    array
  }

  override def compute(s: Partition, context: TaskContext): Iterator[(T, U)] = {
    val (split1, split2) = s.asInstanceOf[ZippedPartition[T, U]].splits
    rdd1.iterator(split1, context).zip(rdd2.iterator(split2, context))
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    val (split1, split2) = s.asInstanceOf[ZippedPartition[T, U]].splits
    rdd1.preferredLocations(split1).intersect(rdd2.preferredLocations(split2))
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }
}
