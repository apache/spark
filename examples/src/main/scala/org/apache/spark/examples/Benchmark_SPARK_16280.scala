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

package org.apache.spark.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.debug._


object Benchmark_SPARK_16280 {

  private def rnd = scala.util.Random

  def main(args: Array[String]) {
    val sqlContext = SparkSession.builder().master("local").appName("Spark-16280").getOrCreate()
    val sc = sqlContext.sparkContext
    import sqlContext.implicits._
    Seq(10, 1000, 100000).foreach((rows) => {
      Seq(1, 10, 100).foreach((bins) => {
        val df1 = sc.makeRDD(Seq.tabulate(rows)((i) => rnd.nextInt(10000))).
          toDF("value").cache()
        println($"rows: $rows, bins: $bins")
        val elapseds1 = Seq.tabulate(3)((_) => {
          val start1 = java.lang.System.currentTimeMillis()
          df1.select(codegen_histogram_numeric("value", bins)).collect()
          java.lang.System.currentTimeMillis() - start1
        })
        println($"codegen_histogram_numeric: ${elapseds1.sum / elapseds1.size}")
        val elapseds2 = Seq.tabulate(3)((_) => {
          val start2 = java.lang.System.currentTimeMillis()
          df1.select(imperative_histogram_numeric("value", bins)).collect()
          java.lang.System.currentTimeMillis() - start2
        })
        println($"imperative_histogram_numeric: ${elapseds2.sum / elapseds2.size}")
        val elapseds3 = Seq.tabulate(3)((_) => {
          val start3 = java.lang.System.currentTimeMillis()
          df1.select(declarative_histogram_numeric("value", bins)).collect()
          java.lang.System.currentTimeMillis() - start3
        })
        println($"declarative_histogram_numeric: ${elapseds3.sum / elapseds3.size}")
      })
    })


//    println(result2)
//    sql2.debug()
//    sql2.debugCodegen()
//    println(sql2.collect().mkString(","))
  }

}
