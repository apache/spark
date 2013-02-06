package spark.rdd

import scala.collection.mutable.ArrayBuffer
import spark.{Dependency, RangeDependency, RDD, SparkContext, Split, TaskContext}
import java.io.{ObjectOutputStream, IOException}

private[spark] class UnionSplit[T: ClassManifest](idx: Int, rdd: RDD[T], splitIndex: Int)
  extends Split {

  var split: Split = rdd.splits(splitIndex)

  def iterator(context: TaskContext) = rdd.iterator(split, context)

  def preferredLocations() = rdd.preferredLocations(split)

  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent split at the time of task serialization
    split = rdd.splits(splitIndex)
    oos.defaultWriteObject()
  }
}

class UnionRDD[T: ClassManifest](
    sc: SparkContext,
    @transient var rdds: Seq[RDD[T]])
  extends RDD[T](sc, Nil) {  // Nil since we implement getDependencies

  override def getSplits = {
    val array = new Array[Split](rdds.map(_.splits.size).sum)
    var pos = 0
    for (rdd <- rdds; split <- rdd.splits) {
      array(pos) = new UnionSplit(pos, rdd, split.index)
      pos += 1
    }
    array
  }

  override def getDependencies = {
    val deps = new ArrayBuffer[Dependency[_]]
    var pos = 0
    for (rdd <- rdds) {
      deps += new RangeDependency(rdd, 0, pos, rdd.splits.size)
      pos += rdd.splits.size
    }
    deps
  }

  override def compute(s: Split, context: TaskContext): Iterator[T] =
    s.asInstanceOf[UnionSplit[T]].iterator(context)

  override def getPreferredLocations(s: Split): Seq[String] =
    s.asInstanceOf[UnionSplit[T]].preferredLocations()
}
