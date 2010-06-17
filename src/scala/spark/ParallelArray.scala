package spark

import nexus.SlaveOffer

import java.util.concurrent.atomic.AtomicLong

@serializable class ParallelArraySplit[T: ClassManifest](
    val arrayId: Long, val slice: Int, values: Seq[T]) {
  def iterator(): Iterator[T] = values.iterator

  override def hashCode(): Int = (41 * (41 + arrayId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: ParallelArraySplit[_] =>
      (this.arrayId == that.arrayId && this.slice == that.slice)
    case _ => false
  }

  override def toString() =
    "ParallelArraySplit(arrayId %d, slice %d)".format(arrayId, slice)
}

class ParallelArray[T: ClassManifest](
  sc: SparkContext, @transient data: Seq[T], numSlices: Int)
extends RDD[T, ParallelArraySplit[T]](sc) {
  // TODO: Right now, each split sends along its full data, even if later down
  // the RDD chain it gets cached. It might be worthwhile to write the data to
  // a file in the DFS and read it in the split instead.

  val id = ParallelArray.newId()

  @transient val splits_ = {
    val slices = ParallelArray.slice(data, numSlices).toArray
    slices.indices.map(i => new ParallelArraySplit(id, i, slices(i))).toArray
  }

  override def splits = splits_

  override def iterator(s: ParallelArraySplit[T]) = s.iterator
  
  override def prefers(s: ParallelArraySplit[T], offer: SlaveOffer) = true
}

private object ParallelArray {
  val nextId = new AtomicLong(0) // Creates IDs for ParallelArrays (on master)
  def newId() = nextId.getAndIncrement()

  def slice[T: ClassManifest](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    if (numSlices < 1)
      throw new IllegalArgumentException("Positive number of slices required")
    seq match {
      case r: Range.Inclusive => {
        val sign = if (r.step < 0) -1 else 1
        slice(new Range(r.start, r.end + sign, r.step).asInstanceOf[Seq[T]],
              numSlices)
      }
      case r: Range => {
        (0 until numSlices).map(i => {
          val start = ((i * r.length.toLong) / numSlices).toInt
          val end = (((i+1) * r.length.toLong) / numSlices).toInt
          new Range(
            r.start + start * r.step, r.start + end * r.step, r.step)
        }).asInstanceOf[Seq[Seq[T]]]
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
