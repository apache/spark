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

package org.apache.spark.ml.r

import org.apache.spark.SparkException
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.DataFrame

private [r] object BinarizerWrapper {

  def transform(data: DataFrame, inputCol: String,
                outputCol: String, threshold: Double): DataFrame = {
    try {
      val inColDouble = "_" + inputCol
      val df = data.withColumn(inColDouble, data.col(inputCol) + 0.0)
      val binarizer: Binarizer = new Binarizer()
        .setInputCol(inColDouble)
        .setOutputCol(outputCol)
        .setThreshold(threshold)

      binarizer.transform(df).drop(inColDouble)
    }
    catch {
      case e: Exception =>
        throw new SparkException(s"Could not convert elements in column: " +
          s"$inputCol to binary values.")
    }
  }

}

