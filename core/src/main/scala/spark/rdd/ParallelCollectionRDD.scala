package spark.rdd

import scala.collection.immutable.NumericRange
import scala.collection.mutable.ArrayBuffer
import scala.collection.Map
import spark.{RDD, TaskContext, SparkContext, Partition}

private[spark] class ParallelCollectionPartition[T: ClassManifest](
    val rddId: Long,
    val slice: Int,
    values: Seq[T])
  extends Partition with Serializable {

  def iterator: Iterator[T] = values.iterator

  override def hashCode(): Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: ParallelCollectionPartition[_] => (this.rddId == that.rddId && this.slice == that.slice)
    case _ => false
  }

  override val index: Int = slice
}

private[spark] class ParallelCollectionRDD[T: ClassManifest](
    @transient sc: SparkContext,
    @transient data: Seq[T],
    numSlices: Int,
    locationPrefs: Map[Int,Seq[String]])
  extends RDD[T](sc, Nil) {
  // TODO: Right now, each split sends along its full data, even if later down the RDD chain it gets
  // cached. It might be worthwhile to write the data to a file in the DFS and read it in the split
  // instead.
  // UPDATE: A parallel collection can be checkpointed to HDFS, which achieves this goal.

  override def getPartitions: Array[Partition] = {
    val slices = ParallelCollectionRDD.slice(data, numSlices).toArray
    slices.indices.map(i => new ParallelCollectionPartition(id, i, slices(i))).toArray
  }

  override def compute(s: Partition, context: TaskContext) =
    s.asInstanceOf[ParallelCollectionPartition[T]].iterator

  override def getPreferredLocations(s: Partition): Seq[String] = {
    locationPrefs.getOrElse(s.index, Nil)
  }
}

private object ParallelCollectionRDD {
  /**
   * Slice a collection into numSlices sub-collections. One extra thing we do here is to treat Range
   * collections specially, encoding the slices as other Ranges to minimize memory cost. This makes
   * it efficient to run Spark over RDDs representing large sets of numbers.
   */
  def slice[T: ClassManifest](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    if (numSlices < 1) {
      throw new IllegalArgumentException("Positive number of slices required")
    }
    seq match {
      case r: Range.Inclusive => {
        val sign = if (r.step < 0) {
          -1
        } else {
          1
        }
        slice(new Range(
            r.start, r.end + sign, r.step).asInstanceOf[Seq[T]], numSlices)
      }
      case r: Range => {
        (0 until numSlices).map(i => {
          val start = ((i * r.length.toLong) / numSlices).toInt
          val end = (((i+1) * r.length.toLong) / numSlices).toInt
          new Range(r.start + start * r.step, r.start + end * r.step, r.step)
        }).asInstanceOf[Seq[Seq[T]]]
      }
      case nr: NumericRange[_] => { // For ranges of Long, Double, BigInteger, etc
        val slices = new ArrayBuffer[Seq[T]](numSlices)
        val sliceSize = (nr.size + numSlices - 1) / numSlices // Round up to catch everything
        var r = nr
        for (i <- 0 until numSlices) {
          slices += r.take(sliceSize).asInstanceOf[Seq[T]]
          r = r.drop(sliceSize)
        }
        slices
      }
      case _ => {
        val array = seq.toArray  // To prevent O(n^2) operations for List etc
        (0 until numSlices).map(i => {
          val start = ((i * array.length.toLong) / numSlices).toInt
          val end = (((i+1) * array.length.toLong) / numSlices).toInt
          array.slice(start, end).toSeq
        })
      }
    }
  }
}
