package spark.rdd

import java.util.Random
import cern.jet.random.Poisson
import cern.jet.random.engine.DRand

import spark.RDD
import spark.OneToOneDependency
import spark.Split

private[spark]
class SampledRDDSplit(val prev: Split, val seed: Int) extends Split with Serializable {
  override val index: Int = prev.index
}

class SampledRDD[T: ClassManifest](
    prev: RDD[T],
    withReplacement: Boolean, 
    frac: Double,
    seed: Int)
  extends RDD[T](prev.context) {

  @transient
  val splits_ = {
    val rg = new Random(seed)
    prev.splits.map(x => new SampledRDDSplit(x, rg.nextInt))
  }

  override def splits = splits_.asInstanceOf[Array[Split]]

  override val dependencies = List(new OneToOneDependency(prev))
  
  override def preferredLocations(split: Split) =
    prev.preferredLocations(split.asInstanceOf[SampledRDDSplit].prev)

  override def compute(splitIn: Split) = {
    val split = splitIn.asInstanceOf[SampledRDDSplit]
    if (withReplacement) {
      // For large datasets, the expected number of occurrences of each element in a sample with
      // replacement is Poisson(frac). We use that to get a count for each element.
      val poisson = new Poisson(frac, new DRand(split.seed))
      prev.iterator(split.prev).flatMap { element =>
        val count = poisson.nextInt()
        if (count == 0) {
          Iterator.empty  // Avoid object allocation when we return 0 items, which is quite often
        } else {
          Iterator.fill(count)(element)
        }
      }
    } else { // Sampling without replacement
      val rand = new Random(split.seed)
      prev.iterator(split.prev).filter(x => (rand.nextDouble <= frac))
    }
  }
}
