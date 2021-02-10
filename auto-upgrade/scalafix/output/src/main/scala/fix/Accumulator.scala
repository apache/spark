/*
 rule=AccumulatorUpgrade
 */
import org.apache.spark._

object BadAcc {
  def boop(): Unit = {
    val sc = new SparkContext()
    val numAcc = sc.accumulator(0)
    val rdd = sc.parallelize(List(1,2,3))
    rdd.foreach(x => numAcc += x)
  }
}
