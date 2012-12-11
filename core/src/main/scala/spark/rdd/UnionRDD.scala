package spark.rdd

import scala.collection.mutable.ArrayBuffer

import spark._
import java.io.{ObjectOutputStream, IOException}

private[spark] class UnionSplit[T: ClassManifest](
    idx: Int,
    rdd: RDD[T],
    splitIndex: Int,
    var split: Split = null)
  extends Split
  with Serializable {
  
  def iterator() = rdd.iterator(split)
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
  extends RDD[T](sc, Nil)  {    // Nil, so the dependencies_ var does not refer to parent RDDs

  @transient
  var splits_ : Array[Split] = {
    val array = new Array[Split](rdds.map(_.splits.size).sum)
    var pos = 0
    for (rdd <- rdds; split <- rdd.splits) {
      array(pos) = new UnionSplit(pos, rdd, split.index)
      pos += 1
    }
    array
  }

  override def splits = splits_

  @transient var deps_ = {
    val deps = new ArrayBuffer[Dependency[_]]
    var pos = 0
    for (rdd <- rdds) {
      deps += new RangeDependency(rdd, 0, pos, rdd.splits.size)
      pos += rdd.splits.size
    }
    deps.toList
  }

  // Pre-checkpoint dependencies deps_ should be transient (deps_)
  // but post-checkpoint dependencies must not be transient (dependencies_)
  override def dependencies = if (isCheckpointed) dependencies_ else deps_

  override def compute(s: Split): Iterator[T] = s.asInstanceOf[UnionSplit[T]].iterator()

  override def preferredLocations(s: Split): Seq[String] =
    s.asInstanceOf[UnionSplit[T]].preferredLocations()

  override def changeDependencies(newRDD: RDD[_]) {
    deps_ = null
    dependencies_ = List(new OneToOneDependency(newRDD))
    splits_ = newRDD.splits
    rdds = null
  }
}
