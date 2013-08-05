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

package spark.mllib.recommendation

import scala.util.Random

import org.jblas.DoubleMatrix

import spark.{RDD, SparkContext}
import spark.mllib.util.MLUtils

/**
* Generate RDD(s) containing data for Matrix Factorization.
*
* This method samples training entries according to the oversampling factor
* 'tr_samp_fact', which is a multiplicative factor of the number of
* degrees of freedom of the matrix: rank*(m+n-rank).
* 
* It optionally samples entries for a testing matrix using 
* 'te_samp_fact', the percentage of the number of training entries 
* to use for testing.
*
* This method takes the following inputs:
* 	sparkMaster 		 (String) The master URL.
* 	outputPath  		 (String) Directory to save output.
* 	m 					 		 (Int) Number of rows in data matrix.
* 	n 							 (Int) Number of columns in data matrix.
* 	rank 					 (Int) Underlying rank of data matrix.
* 	tr_samp_fact 	 (Double) Oversampling factor.
* 	noise 					 (Boolean) Whether to add gaussian noise to training data.
* 	sigma 					 (Double) Standard deviation of added gaussian noise.
* 	test 					 (Boolean) Whether to create testing RDD.
* 	te_samp_fact 	 (Double) Percentage of training data to use as test data.
*/

object MFDataGenerator{

  def main(args: Array[String]) {
    if (args.length != 10) {
      println("Usage: MFGenerator " +
        "<master> <output_dir> <m> <n> <rank> <tr_samp_fact> <noise> <sigma> <test> <te_samp_fact>")
      System.exit(1)
    }

    val sparkMaster: String = args(0)
    val outputPath: String = args(1)
    val m: Int = if (args.length > 2) args(2).toInt else 100
    val n: Int = if (args.length > 3) args(3).toInt else 100
    val rank: Int = if (args.length > 4) args(4).toInt else 10
    val tr_samp_fact: Double = if (args.length > 5) args(5).toDouble else 1.0
    val noise: Boolean = if (args.length > 6) args(6).toBoolean else false
    val sigma: Double = if (args.length > 7) args(7).toDouble else 0.1
    val test: Boolean = if (args.length > 8) args(8).toBoolean else false
    val te_samp_fact: Double = if (args.length > 9) args(9).toDouble else 0.1

    val sc = new SparkContext(sparkMaster, "MFDataGenerator")

    val A = DoubleMatrix.randn(m,rank)
    val B = DoubleMatrix.randn(rank,n)
    val z = 1/(scala.math.sqrt(scala.math.sqrt(rank)))
    A.mmuli(z)
    B.mmuli(z)
    val fullData = A.mmul(B)

    val df = rank*(m+n-rank)
    val sampsize = scala.math.min(scala.math.round(tr_samp_fact*df), scala.math.round(.99*m*n)).toInt
    val rand = new Random()
    val mn = m*n
    val shuffled = rand.shuffle(1 to mn toIterable)

    val omega = shuffled.slice(0,sampsize)
    val ordered = omega.sortWith(_ < _).toArray
    val trainData: RDD[(Int, Int, Double)] = sc.parallelize(ordered)
    		.map(x => (fullData.indexRows(x-1),fullData.indexColumns(x-1),fullData.get(x-1)))

    // optionally add gaussian noise
    if(noise){
        trainData.map(x => (x._1,x._2,x._3+rand.nextGaussian*sigma))
    }

    trainData.map(x => x._1 + "," + x._2 + "," + x._3).saveAsTextFile(outputPath)

    // optionally generate testing data
    if(test){
    	val test_sampsize = scala.math
    		.min(scala.math.round(sampsize*te_samp_fact),scala.math.round(mn-sampsize))
    		.toInt
    	val test_omega = shuffled.slice(sampsize,sampsize+test_sampsize)
    	val test_ordered = test_omega.sortWith(_ < _).toArray
    	val testData: RDD[(Int, Int, Double)] = sc.parallelize(test_ordered)
    		.map(x=> (fullData.indexRows(x-1),fullData.indexColumns(x-1),fullData.get(x-1)))
      testData.map(x => x._1 + "," + x._2 + "," + x._3).saveAsTextFile(outputPath)
    }
        
	sc.stop()
  }
}