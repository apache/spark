package spark

import org.scalatest.FunSuite
import org.scalatest.concurrent.Timeouts._
import org.scalatest.time.{Span, Millis}
import spark.SparkContext._

class UnpersistSuite extends FunSuite with LocalSparkContext {
  test("unpersist RDD") {
    sc = new SparkContext("local", "test")
    val rdd = sc.makeRDD(Array(1, 2, 3, 4), 2).cache()
    rdd.count
    assert(sc.persistentRdds.isEmpty === false)
    rdd.unpersist()
    assert(sc.persistentRdds.isEmpty === true)

    failAfter(Span(3000, Millis)) {
      try {
        while (! sc.getRDDStorageInfo.isEmpty) {
          Thread.sleep(200)
        }
      } catch {
        case _ => { Thread.sleep(10) }
          // Do nothing. We might see exceptions because block manager
          // is racing this thread to remove entries from the driver.
      }
    }
    assert(sc.getRDDStorageInfo.isEmpty === true)
  }
}
