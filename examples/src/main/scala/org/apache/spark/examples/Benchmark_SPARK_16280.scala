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

object Benchmark_SPARK_16280 {

  private def rnd = scala.util.Random

  def main(args: Array[String]) {
    val sqlContext = SparkSession.builder().master("local[4]")
      .appName("Spark-16280").config("spark.executor.heartbeatInterval", "100000s")
      .config("spark.network.timeout", "100000s").config("spark.shuffle.manager", "tungsten-sort")
      .getOrCreate()
    val sc = sqlContext.sparkContext
    import sqlContext.implicits._
    val statistics = Seq(
      (10, 1),
      (10, 10),
      (10, 100),
      (1000, 1),
      (1000, 10),
      (1000, 100),
      (100000, 1),
      (100000, 10),
      (100000, 100)
      ).map((pair) => {
      val rows = pair._1
      val bins = pair._2
      println(pair)
      val df1 = sc.makeRDD(Seq.tabulate(rows)((i) => rnd.nextInt(10000))).
        toDF("value").cache()
      df1.first()
      val elapseds1 = Seq.tabulate(3)((_) => {
        val start = java.lang.System.currentTimeMillis()
        df1.select(histogram_numeric("value", bins))
          .collect()
        java.lang.System.currentTimeMillis() - start
      })
      val elapseds2 = Seq.tabulate(3)((_) => {
        val start = java.lang.System.currentTimeMillis()
        df1.select(codegen_histogram_numeric("value", bins))
          .collect()
        java.lang.System.currentTimeMillis() - start
      })
      val elapseds3 = Seq.tabulate(3)((_) => {
        val start = java.lang.System.currentTimeMillis()
        df1.select(declarative_histogram_numeric("value", bins))
          .collect()
        java.lang.System.currentTimeMillis() - start
      })
      val elapseds4 = Seq.tabulate(3)((_) => {
        val start = java.lang.System.currentTimeMillis()
        df1.select(imperative_histogram_numeric("value", bins))
          .collect()
        java.lang.System.currentTimeMillis() - start
      })
      Seq(pair, elapseds1.sum / elapseds1.size
        , elapseds2.sum / elapseds2.size
        , elapseds3.sum / elapseds3.size
        , elapseds4.sum / elapseds4.size
      )
    })

    println(Tabulator.format(Seq(
      Seq("(rows, numOfBins)","codegen_with_array_agg_buffer_histogram_numeric"
        ,"codegen_histogram_numeric"
        ,"declarative_histogram_numeric"
        ,"imperative_histogram_numeric"
      )) ++ statistics))
  }
}


object Tabulator {
  def format(table: Seq[Seq[Any]]): String = table match {
    case Seq() => ""
    case _ =>
      val sizes = for (row <- table) yield
        (for (cell <- row) yield if (cell == null) 0 else cell.toString.length)
      val colSizes = for (col <- sizes.transpose) yield col.max
      val rows = for (row <- table) yield formatRow(row, colSizes)
      formatRows(rowSeparator(colSizes), rows)
  }

  def formatRows(rowSeparator: String, rows: Seq[String]): String = (
    rowSeparator ::
      rows.head ::
      rowSeparator ::
      rows.tail.toList :::
      rowSeparator ::
      List()).mkString("\n")

  def formatRow(row: Seq[Any], colSizes: Seq[Int]): String = {
    val cells = (for ((item, size) <- row.zip(colSizes))
      yield if (size == 0) "" else ("%" + size + "s").format(item))
    cells.mkString("|", "|", "|")
  }

  def rowSeparator(colSizes: Seq[Int]): String =
    colSizes map { "-" * _ } mkString("|", "|", "|")
}