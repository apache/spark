package org.apache.spark

/**
 * A test suite to make sure all `implicit` functions work correctly.
 * Please don't `import org.apache.spark.SparkContext._` in this class.
 *
 * As `implicit` is a compiler feature, we don't need to run this class.
 * What we need to do is making the compiler happy.
 */
class ImplicitSuite {


  // We only want to test if `implict` works well with the compiler, so we don't need a real
  // SparkContext.
  def mockSparkContext[T]: org.apache.spark.SparkContext = null

  // We only want to test if `implict` works well with the compiler, so we don't need a real RDD.
  def mockRDD[T]: org.apache.spark.rdd.RDD[T] = null

  def testRddToPairRDDFunctions(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[(Int, Int)] = mockRDD
    rdd.groupByKey
  }

  def testRddToAsyncRDDActions(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[Int] = mockRDD
    rdd.countAsync()
  }

  def testRddToSequenceFileRDDFunctions(): Unit = {
    // TODO eliminating `import intToIntWritable` needs refactoring SequenceFileRDDFunctions.
    // That will be a breaking change.
    import org.apache.spark.SparkContext.intToIntWritable
    val rdd: org.apache.spark.rdd.RDD[(Int, Int)] = mockRDD
    rdd.saveAsSequenceFile("/a/test/path")
  }

  def testRddToOrderedRDDFunctions(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[(Int, Int)] = mockRDD
    rdd.sortByKey()
  }

  def testDoubleRDDToDoubleRDDFunctions(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[Double] = mockRDD
    rdd.stats()
  }


  def testNumericRDDToDoubleRDDFunctions(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[Int] = mockRDD
    rdd.stats()
  }

  def testDoubleAccumulatorParam(): Unit = {
    val sc = mockSparkContext
    sc.accumulator(123.4)
  }

  def testIntAccumulatorParam(): Unit = {
    val sc = mockSparkContext
    sc.accumulator(123)
  }

  def testLongAccumulatorParam(): Unit = {
    val sc = mockSparkContext
    sc.accumulator(123L)
  }

  def testFloatAccumulatorParam(): Unit = {
    val sc = mockSparkContext
    sc.accumulator(123F)
  }
}
