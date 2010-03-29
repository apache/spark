package spark

abstract class ParallelArray[T](sc: SparkContext) {
  def filter(f: T => Boolean): ParallelArray[T] = {
    val cleanF = sc.clean(f)
    new FilteredParallelArray[T](sc, this, cleanF)
  }
  
  def foreach(f: T => Unit): Unit
  
  def map[U](f: T => U): Array[U]
}

private object ParallelArray {
  def slice[T](seq: Seq[T], numSlices: Int): Array[Seq[T]] = {
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
          new SerializableRange(
            r.start + start * r.step, r.start + end * r.step, r.step)
        }).asInstanceOf[Seq[Seq[T]]].toArray
      }
      case _ => {
        val array = seq.toArray  // To prevent O(n^2) operations for List etc
        (0 until numSlices).map(i => {
          val start = ((i * array.length.toLong) / numSlices).toInt
          val end = (((i+1) * array.length.toLong) / numSlices).toInt
          array.slice(start, end).toArray
        }).toArray
      }
    }
  }
}

private class SimpleParallelArray[T](
  sc: SparkContext, data: Seq[T], numSlices: Int)
extends ParallelArray[T](sc) {
  val slices = ParallelArray.slice(data, numSlices)
  
  def foreach(f: T => Unit) {
    val cleanF = sc.clean(f)
    var tasks = for (i <- 0 until numSlices) yield
      new ForeachRunner(i, slices(i), cleanF)
    sc.runTasks[Unit](tasks.toArray)
  }

  def map[U](f: T => U): Array[U] = {
    val cleanF = sc.clean(f)
    var tasks = for (i <- 0 until numSlices) yield
      new MapRunner(i, slices(i), cleanF)
    return Array.concat(sc.runTasks[Array[U]](tasks.toArray): _*)
  }
}

@serializable
private class ForeachRunner[T](sliceNum: Int, data: Seq[T], f: T => Unit)
extends Function0[Unit] {
  def apply() = {
    printf("Running slice %d of parallel foreach\n", sliceNum)
    data.foreach(f)
  }
}

@serializable
private class MapRunner[T, U](sliceNum: Int, data: Seq[T], f: T => U)
extends Function0[Array[U]] {
  def apply(): Array[U] = {
    printf("Running slice %d of parallel map\n", sliceNum)
    return data.map(f).toArray
  }
}

private class FilteredParallelArray[T](
  sc: SparkContext, array: ParallelArray[T], predicate: T => Boolean)
extends ParallelArray[T](sc) {
  val cleanPred = sc.clean(predicate)
  
  def foreach(f: T => Unit) {
    val cleanF = sc.clean(f)
    array.foreach(t => if (cleanPred(t)) cleanF(t))
  }

  def map[U](f: T => U): Array[U] = {
    val cleanF = sc.clean(f)
    throw new UnsupportedOperationException(
      "Map is not yet supported on FilteredParallelArray")
  }
}
