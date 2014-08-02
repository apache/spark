/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main.scala

import scala.collection.mutable.{ListBuffer, Queue}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

case class Person(name: String, age: Int)

object SparkSqlExample {

  def main(args: Array[String]) {
    val conf = sys.env.get("SPARK_AUDIT_MASTER") match {
      case Some(master) => new SparkConf().setAppName("Simple Sql App").setMaster(master)
      case None => new SparkConf().setAppName("Simple Sql App")
    }
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    import hiveContext._
    hql("DROP TABLE IF EXISTS src")
    hql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    hql("LOAD DATA LOCAL INPATH 'data.txt' INTO TABLE src")
    val results = hql("FROM src SELECT key, value WHERE key >= 0 AND KEY < 5").collect()
    results.foreach(println)
    
    def test(f: => Boolean, failureMsg: String) = {
      if (!f) {
        println(failureMsg)
        System.exit(-1)
      }
    }
    
    test(results.size == 5, "Unexpected number of selected elements: " + results)
    println("Test succeeded")
    sc.stop()
  }
}
