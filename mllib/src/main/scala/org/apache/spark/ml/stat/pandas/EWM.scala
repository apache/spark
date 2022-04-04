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

package org.apache.spark.ml.stat.pandas

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// scalastyle:off println
/**
 * API for EWM functions in MLlib, compatible with DataFrames and Datasets.
 */
@Since("3.4.0")
object EWM extends Logging {

  private val MEAN = "mean"

  private val supportedFunctions = Array(MEAN)

  // TODO: support grouping
  def ewm(
      dataset: Dataset[_],
      orderingCols: Seq[Column],
      analysisCols: Seq[Column],
      com: Option[Double] = None,
      span: Option[Double] = None,
      halflife: Option[Double] = None,
      alpha: Option[Double] = None,
      adjust: Boolean = true,
      function: String = MEAN): DataFrame = {
    require(supportedFunctions.contains(function), s"Unsupported function $function")
    require(orderingCols.nonEmpty && analysisCols.nonEmpty)

    val _alpha = (com, span, halflife, alpha) match {
      case (Some(c), None, None, None) =>
        require(c >= 0, s"com Must be no less than 0, but got $c")
        1 / (1 + c)

      case (None, Some(s), None, None) =>
        require(s >= 1, s"span Must be no less than 1, but got $s")
        2 / (1 + s)

      case (None, None, Some(h), None) =>
        require(h > 0, s"halflife Must be positive, but got $h")
        1 - math.exp(-math.log(2) / h)

      case (None, None, None, Some(a)) =>
        require(0 < a && a <= 1, s"alpha Must be in (0, 1], but got $a")
        a

      case _ =>
        throw new IllegalArgumentException("Exact one option among com/span/halflife/alpha " +
          s"Must be set, but got $com/$span/$halflife/$alpha")
    }

    val spark = dataset.sparkSession
    import spark.implicits._
    val sorted = dataset.sort(orderingCols: _*)

    val arrayCol = array(analysisCols.map(_.cast(DoubleType)): _*).as("_array_")
    val arrays = sorted.select(arrayCol).as[Array[Double]].rdd.setName("arrays")

    val numPartitions = arrays.getNumPartitions
    println(s"numPartitions: $numPartitions")
    val numColumns = analysisCols.size
    val statistics = arrays.mapPartitionsWithIndex { case (pid, iter) =>
      // TODO: do not need to compute the last partition
      val accs = Array.fill(numColumns)(new MeanAccumulatorWithAdjust(_alpha))
      var count = 0L
      iter.foreach { array =>
        var c = 0
        while (c < numColumns) {
          accs(c).update(array(c))
          c += 1
        }
        count += 1L
      }
      Iterator.single((count, accs))
    }.collect()

    val acc = new MeanAccumulatorWithAdjust(_alpha)
    val sizes = statistics.map(_._1)
    val status = Array.tabulate(numColumns) { c =>
      val stats = statistics.map(_._2(c).getStatistics)
      acc.accumulate(sizes, stats)
    }

    status.zipWithIndex.foreach { case (s, i) =>
      val a = s.map(_.map(_.toString.take(10)).mkString("(", ",", ")")).mkString("[", ",", "]")
      println(s"col $i: $a")
    }

    val arrayColIdx = orderingCols.length
    val results = sorted.select((orderingCols :+ arrayCol): _*)
      .rdd
      .mapPartitionsWithIndex { case (pid, iter) =>
        val accs = Array.fill(numColumns)(new MeanAccumulatorWithAdjust(_alpha))
        var c = 0
        while (c < numColumns) {
          accs(c).setStatistics(status(c)(pid))
          c += 1
        }
        iter.map { row =>
          val array = row.getSeq[Double](arrayColIdx).toArray
          val means = accs.zip(array).map { case (acc, value) =>
            acc.update(value)
            acc.compute()
          }
          val anys = row.copy().toSeq.dropRight(1) ++ Seq(means)
          Row(anys: _*)
        }
      }

    // results.collect().foreach(println)
    val schema = sorted.select((orderingCols :+ arrayCol.as("_ewma_")): _*).schema
    spark.createDataFrame(results, schema)
  }

}


private abstract class EWMAccumulator extends Serializable {
  def reset(): Unit

  def update(value: Double): Unit

  def compute(): Double

  def setStatistics(stat: Array[BigDecimal]): Unit

  def getStatistics: Array[BigDecimal]

  def accumulate(
      sizes: Array[Long],
      stats: Array[Array[BigDecimal]]): Array[Array[BigDecimal]]
}

private class MeanAccumulatorWithAdjust(val alpha: Double) extends EWMAccumulator {
  require(0 < alpha && alpha <= 1)

  private val beta = BigDecimal(1 - alpha)

  private var numerator = BigDecimal(0)

  private var denominator = BigDecimal(0)

  override def reset(): Unit = {
    numerator = 0
    denominator = 0
  }

  override def update(value: Double): Unit = {
    numerator = numerator * beta + value
    denominator = denominator * beta + 1
  }

  override def compute(): Double = {
    (numerator / denominator).doubleValue()
  }

  override def setStatistics(stat: Array[BigDecimal]): Unit = {
    val Array(n, d) = stat
    numerator = n
    denominator = d
  }

  override def getStatistics: Array[BigDecimal] = {
    Array(numerator, denominator)
  }

  override def accumulate(
      sizes: Array[Long],
      stats: Array[Array[BigDecimal]]): Array[Array[BigDecimal]] = {
    require(sizes.length == stats.length)
    val n = sizes.length
    val result = stats.zip(sizes).take(n - 1)
      .scanLeft(Array(BigDecimal(0), BigDecimal(0)))(
        op = {
          case (Array(accNum, accDen), (Array(num, den), size)) =>
            if (size == 0) {
              Array(accNum, accDen)
            } else {
              require(size.toInt > 0)
              val decay = beta.pow(size.toInt)
              Array(num + accNum * decay, den + accDen * decay)
            }
        }
      )
    require(result.length == n)
    result
  }
}
// scalastyle:on println
