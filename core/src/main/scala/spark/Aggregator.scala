package spark

/** A set of functions used to aggregate data.
  * 
  * @param createCombiner function to create the initial value of the aggregation.
  * @param mergeValue function to merge a new value into the aggregation result.
  * @param mergeCombiners function to merge outputs from multiple mergeValue function.
  * @param mapSideCombine whether to apply combiners on map partitions, also
  *                       known as map-side aggregations. When set to false, 
  *                       mergeCombiners function is not used.
  */
case class Aggregator[K, V, C] (
    val createCombiner: V => C,
    val mergeValue: (C, V) => C,
    val mergeCombiners: (C, C) => C,
    val mapSideCombine: Boolean = true)

