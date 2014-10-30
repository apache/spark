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

package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.GaussianMixtureModel
import org.apache.spark.mllib.clustering.GMMExpectationMaximization
import org.apache.spark.mllib.linalg.Vectors

object DenseGmmEM {
  def main(args: Array[String]): Unit = {
    if( args.length != 3 ) {
      println("usage: DenseGmmEM <input file> <k> <delta>")
    } else {
      run(args(0), args(1).toInt, args(2).toDouble)
    }
  }

  def run(inputFile: String, k: Int, tol: Double) {
    val conf = new SparkConf().setAppName("Spark EM Sample")
    val ctx  = new SparkContext(conf)
    
    val data = ctx.textFile(inputFile).map(line =>
        Vectors.dense(line.trim.split(' ').map(_.toDouble))).cache()
      
    val clusters = GMMExpectationMaximization.train(data, k)
    
    for(i <- 0 until clusters.k) {
      println("w=%f mu=%s sigma=\n%s\n" format (clusters.w(i), clusters.mu(i), clusters.sigma(i)))
    }
  }
}
