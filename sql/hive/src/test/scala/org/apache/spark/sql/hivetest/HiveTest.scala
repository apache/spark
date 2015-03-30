package org.apache.spark.sql.hive.hivetest

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

case class Person(val name: String, val age: Int, val data: Array[Int])
/**
 * Created by Administrator on 2015/3/28 0028.
 */
object HiveTest extends App{

  val ppl = Array(
    Person("A", 20, Array(10, 12, 19)),
    Person("B", 25, Array(7, 8, 4)),
    Person("C", 19, Array(12, 4, 232)))


  val conf = new SparkConf().setMaster("local[2]").setAppName("sql")
  val sc = new SparkContext(conf)
  val sqlCtx = new HiveContext(sc)
  import sqlCtx.implicits._
  val df = sc.makeRDD(ppl).toDF
  df.registerTempTable("ppl")
  sqlCtx.cacheTable("ppl") // cache table otherwise ExistingRDD will be used that do not support column pruning

  val s = sqlCtx.sql("select name, sum(d) from ppl lateral view explode(data) d as d group by name")
  s.explain(true)
  val rows = s.collect();
  rows.map { r =>
    println(r)
  }
}