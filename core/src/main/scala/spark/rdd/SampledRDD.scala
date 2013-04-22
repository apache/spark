package spark.rdd

import java.util.Random

import cern.jet.random.Poisson
import cern.jet.random.engine.DRand

import spark.{RDD, Partition, TaskContext}

private[spark]
class SampledRDDPartition(val prev: Partition, val seed: Int) extends Partition with Serializable {
  override val index: Int = prev.index
}

class SampledRDD[T: ClassManifest](
    prev: RDD[T],
    withReplacement: Boolean, 
    frac: Double,
    seed: Int)
  extends RDD[T](prev) {

  override def getPartitions: Array[Partition] = {
    val rg = new Random(seed)
    firstParent[T].partitions.map(x => new SampledRDDPartition(x, rg.nextInt))
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].preferredLocations(split.asInstanceOf[SampledRDDPartition].prev)

  override def compute(splitIn: Partition, context: TaskContext): Iterator[T] = {
    val split = splitIn.asInstanceOf[SampledRDDPartition]
    if (withReplacement) {
      // For large datasets, the expected number of occurrences of each element in a sample with
      // replacement is Poisson(frac). We use that to get a count for each element.
      val poisson = new Poisson(frac, new DRand(split.seed))
      firstParent[T].iterator(split.prev, context).flatMap { element =>
        val count = poisson.nextInt()
        if (count == 0) {
          Iterator.empty  // Avoid object allocation when we return 0 items, which is quite often
        } else {
          Iterator.fill(count)(element)
        }
      }
    } else { // Sampling without replacement
      val rand = new Random(split.seed)
      firstParent[T].iterator(split.prev, context).filter(x => (rand.nextDouble <= frac))
    }
  }
}
