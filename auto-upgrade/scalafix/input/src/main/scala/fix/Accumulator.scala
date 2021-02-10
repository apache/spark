/*
 rule=AccumulatorUpgrade
 */
import org.apache.spark._

object BadAcc {
  def boop(): Unit = {
    val sc = new SparkContext()
    val num = 0
    val numAcc = sc.accumulator(num)
    val litAcc = sc.accumulator(0)
    val namedAcc = sc.accumulator(0, "cheese")
    val litDoubleAcc = sc.accumulator(0.0)
    val rdd = sc.parallelize(List(1,2,3))
    rdd.foreach(x => numAcc += x)
  }
}
