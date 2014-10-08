package ssj

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext}
import org.scalatest.FunSuite

/**
 * 这儿继承了LocalSparkContext，保存了一个SparkContext变量
 * Created by shenshijun on 14-10-8.
 */
class HelloWorld extends FunSuite with LocalSparkContext{

  override def beforeAll() {
    super.beforeAll()
    val conf = new SparkConf().setAppName("HelloWorld").setMaster("local")
    sc = new SparkContext(conf)
  }

  test("Parallelized rdd") {
    println("hello world")
    assert(1 == 1)
  }

}
