package spark.rdd

import scala.collection.mutable.ArrayBuffer
import spark.{Dependency, RangeDependency, RDD, SparkContext, Partition, TaskContext}
import java.io.{ObjectOutputStream, IOException}

private[spark] class UnionPartition[T: ClassManifest](idx: Int, rdd: RDD[T], splitIndex: Int)
  extends Partition {

  var split: Partition = rdd.partitions(splitIndex)

  def iterator(context: TaskContext) = rdd.iterator(split, context)

  def preferredLocations() = rdd.preferredLocations(split)

  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent split at the time of task serialization
    split = rdd.partitions(splitIndex)
    oos.defaultWriteObject()
  }
}

class UnionRDD[T: ClassManifest](
    sc: SparkContext,
    @transient var rdds: Seq[RDD[T]])
  extends RDD[T](sc, Nil) {  // Nil since we implement getDependencies

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](rdds.map(_.partitions.size).sum)
    var pos = 0
    for (rdd <- rdds; split <- rdd.partitions) {
      array(pos) = new UnionPartition(pos, rdd, split.index)
      pos += 1
    }
    array
  }

  override def getDependencies: Seq[Dependency[_]] = {
    val deps = new ArrayBuffer[Dependency[_]]
    var pos = 0
    for (rdd <- rdds) {
      deps += new RangeDependency(rdd, 0, pos, rdd.partitions.size)
      pos += rdd.partitions.size
    }
    deps
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] =
    s.asInstanceOf[UnionPartition[T]].iterator(context)

  override def getPreferredLocations(s: Partition): Seq[String] =
    s.asInstanceOf[UnionPartition[T]].preferredLocations()
}
