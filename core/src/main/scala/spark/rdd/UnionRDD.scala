package spark.rdd

import scala.collection.mutable.ArrayBuffer

import spark.{Dependency, RangeDependency, RDD, SparkContext, Split, TaskContext}


private[spark] class UnionSplit[T: ClassManifest](
    idx: Int,
    rdd: RDD[T],
    split: Split)
  extends Split
  with Serializable {

  def iterator(context: TaskContext) = rdd.iterator(split, context)
  def preferredLocations() = rdd.preferredLocations(split)
  override val index: Int = idx
}

class UnionRDD[T: ClassManifest](
    sc: SparkContext,
    @transient rdds: Seq[RDD[T]])
  extends RDD[T](sc)
  with Serializable {

  @transient
  val splits_ : Array[Split] = {
    val array = new Array[Split](rdds.map(_.splits.size).sum)
    var pos = 0
    for (rdd <- rdds; split <- rdd.splits) {
      array(pos) = new UnionSplit(pos, rdd, split)
      pos += 1
    }
    array
  }

  override def splits = splits_

  @transient
  override val dependencies = {
    val deps = new ArrayBuffer[Dependency[_]]
    var pos = 0
    for (rdd <- rdds) {
      deps += new RangeDependency(rdd, 0, pos, rdd.splits.size)
      pos += rdd.splits.size
    }
    deps.toList
  }

  override def compute(s: Split, context: TaskContext): Iterator[T] =
    s.asInstanceOf[UnionSplit[T]].iterator(context)

  override def preferredLocations(s: Split): Seq[String] =
    s.asInstanceOf[UnionSplit[T]].preferredLocations()
}
