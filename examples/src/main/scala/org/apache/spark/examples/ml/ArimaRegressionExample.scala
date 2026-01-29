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

package org.apache.spark.examples.ml

import org.apache.spark.ml.regression.ArimaRegression
import org.apache.spark.sql.SparkSession

object ArimaRegressionExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("ARIMA Example").getOrCreate()
    import spark.implicits._

    val tsData = Seq(1.2, 2.3, 3.1, 4.0, 5.5).toDF("y")

    val arima = new ArimaRegression().setP(1).setD(0).setQ(1)
    val model = arima.fit(tsData)

    val result = model.transform(tsData)
    result.show()

    spark.stop()
  }
}
