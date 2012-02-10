package spark

import java.util.Random

class SampledRDDSplit(val prev: Split, val seed: Int) extends Split with Serializable {
  override val index = prev.index
}

class SampledRDD[T: ClassManifest](
    prev: RDD[T],
    withReplacement: Boolean, 
    frac: Double,
    seed: Int)
  extends RDD[T](prev.context) {

  @transient
  val splits_ = {
    val rg = new Random(seed);
    prev.splits.map(x => new SampledRDDSplit(x, rg.nextInt))
  }

  override def splits = splits_.asInstanceOf[Array[Split]]

  override val dependencies = List(new OneToOneDependency(prev))
  
  override def preferredLocations(split: Split) =
    prev.preferredLocations(split.asInstanceOf[SampledRDDSplit].prev)

  override def compute(splitIn: Split) = {
    val split = splitIn.asInstanceOf[SampledRDDSplit]
    val rg = new Random(split.seed);
    // Sampling with replacement (TODO: use reservoir sampling to make this more efficient?)
    if (withReplacement) {
      val oldData = prev.iterator(split.prev).toArray
      val sampleSize = (oldData.size * frac).ceil.toInt
      val sampledData = { 
        // all of oldData's indices are candidates, even if sampleSize < oldData.size
        for (i <- 1 to sampleSize)
          yield oldData(rg.nextInt(oldData.size)) 
      }
      sampledData.iterator
    } else { // Sampling without replacement
      prev.iterator(split.prev).filter(x => (rg.nextDouble <= frac))
    }
  }
}
