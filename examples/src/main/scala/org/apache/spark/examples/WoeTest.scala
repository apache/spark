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

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.WeightOfEvidence
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yuhao on 1/14/16.
  */
object WoeTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("woeTest").setMaster("local[6]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    Logger.getRootLogger.setLevel(Level.WARN)

    val df = sqlContext.createDataFrame(Seq(
      (0, "ab"),
      (1, "ab"),
      (1, "ab"),
      (0, "cd"),
      (0, "cd"),
      (0, "efg"),
      (1, "efg")
    )).toDF("label", "word")

    val woe = new WeightOfEvidence().setInputCols(Array("word", "label")).setOutputCol("out")

    woe.transform(df).show(false)





//    df.registerTempTable("temp")
//
//    val total0 = df.where("label = '0'").count()
//    val total1 = df.where("label = '1'").count()
//
//    val tt = df.sqlContext.sql(
//      """
//        |select
//        |word,
//        |sum (IF(label='1', 1, 1e-2)) as 1count,
//        |sum (IF(label='0', 1, 1e-2)) as 0count
//        |from temp
//        |group by word
//      """.stripMargin
//      ).selectExpr("word", "1count", "0count", s"1count/$total1 as p1", s"0count/$total0 as p0", "1count/0count as ratio")
//
//    tt.show(false)
//
//    val woeMap = tt.map(r => {
//      val category = r.getString(0)
//      val oneZeroRatio = r.getAs[Double]("ratio")
//      val base = total1.toDouble / total0
//      val woe = math.log(oneZeroRatio / base)
//      (category, woe)
//    }).collectAsMap()
//
//    val iv = tt.map(r => {
//      val oneZeroRatio = r.getAs[Double]("ratio")
//      val base = total1.toDouble / total0
//      val woe = math.log(oneZeroRatio / base)
//      val p1 = r.getAs[Double]("p1")
//      val p0 = r.getAs[Double]("p0")
//      woe * (p1 - p0)
//    }).sum()
//
//    println("information value: " + iv)
//
//    val trans = udf { (factor: String) =>
//      woeMap.get(factor)
//    }
//
//    df.withColumn("out", trans(col("word"))).show(false)

//    val woe = new WeightOfEvidence()
//      .setInputCols(Array("label", "word"))
//      .setOutputCol("woe")
//
//    val tokens = woe.transform(df).cache()
//    println(tokens.count())
//    tokens.show(false)
  }

}
