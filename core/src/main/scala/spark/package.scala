/**
 * Core Spark functionality. [[spark.SparkContext]] serves as the main entry point to Spark, while
 * [[spark.RDD]] is the data type representing a distributed collection, and provides most
 * parallel operations. 
 *
 * In addition, [[spark.PairRDDFunctions]] contains operations available only on RDDs of key-value
 * pairs, such as `groupByKey` and `join`; [[spark.DoubleRDDFunctions]] contains operations
 * available only on RDDs of Doubles; and [[spark.SequenceFileRDDFunctions]] contains operations
 * available on RDDs that can be saved as SequenceFiles. These operations are automatically
 * available on any RDD of the right type (e.g. RDD[(Int, Int)] through implicit conversions when
 * you `import spark.SparkContext._`.
 */
package object spark { 
  // For package docs only
}
