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

// scalastyle:off println
package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
// $example off$

object LabeledPointSparseDataExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LabeledPointSparseDataExample")
    val sc = new SparkContext(conf)

    // $example on$
    val examples: RDD[LabeledPoint] = MLUtils.
      loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
