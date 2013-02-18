package spark

import org.scalatest.FunSuite
import SparkContext._

class PipedRDDSuite extends FunSuite with LocalSparkContext {
  
  test("basic pipe") {
    sc = new SparkContext("local", "test")
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)

    val piped = nums.pipe(Seq("cat"))

    val c = piped.collect()
    assert(c.size === 4)
    assert(c(0) === "1")
    assert(c(1) === "2")
    assert(c(2) === "3")
    assert(c(3) === "4")
  }

  test("pipe with env variable") {
    sc = new SparkContext("local", "test")
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    val piped = nums.pipe(Seq("printenv", "MY_TEST_ENV"), Map("MY_TEST_ENV" -> "LALALA"))
    val c = piped.collect()
    assert(c.size === 2)
    assert(c(0) === "LALALA")
    assert(c(1) === "LALALA")
  }

  test("pipe with non-zero exit status") {
    sc = new SparkContext("local", "test")
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    val piped = nums.pipe("cat nonexistent_file")
    intercept[SparkException] {
      piped.collect()
    }
  }

}
