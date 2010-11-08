package spark

/**
 * A trait for shuffle system. Given an input RDD and combiner functions
 * for PairRDDExtras.combineByKey(), returns an output RDD.
 */
@serializable
trait Shuffle[K, V, C] {
  def compute(input: RDD[(K, V)],
              numOutputSplits: Int,
              createCombiner: V => C,
              mergeValue: (C, V) => C,
              mergeCombiners: (C, C) => C)
  : RDD[(K, C)]
}
