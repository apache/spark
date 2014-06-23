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

import scala.util.control.Breaks._
import breeze.linalg._
import breeze.numerics._
import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.language.existentials // for feature warning in scala 2.10.x

/**
 * @author: Kiran Lonikar
 * Logistic regression based classification for multiple lables. Uses simple gradient descent algorithm with regularization.
 * Usage: SparkLRMultiClass [dataFileName] [numClasses] [lambda] [alpha] [maxIterations] [costThreshold] [numFeatures]
 * From the spark base dir:
 * Compile standalone using (make sure you have built spark before that):
 * scalac -d target -cp assembly/target/scala-2.10/spark-assembly-1.1.0-SNAPSHOT-hadoop1.0.4.jar examples/src/main/scala/org/apache/spark/examples/SparkLRMultiClass.scala
 * Execute using:
 * java -cp target;assembly/target/scala-2.10/spark-assembly-1.1.0-SNAPSHOT-hadoop1.0.4.jar -Dspark.master=local[4] org.apache.spark.examples.SparkLRMultiClass examples\src\main\resources\ny-weather-preprocessed.csv 9 0.5 10 50 0.001

 * NOTE: to change the spark logging level to WARN, do the following (no other way works):
 * Open the file core/src/main/resources/org/apache/spark/log4j-defaults.properties and replace INFO with WARN
 * Then using the command like below, update the spark archive to have the new file:
 * jar uvf assembly/target/scala-2.10/spark-assembly-1.1.0-SNAPSHOT-hadoop1.0.4.jar -C core/src/main/resources org/apache/spark/log4j-defaults.properties
*/
object SparkLRMultiClass {

  /**
    Builds an RDD from a CSV file. Each row of the RDD contains the class label yVal and a feature vector.
  */
  def csv2featureVectors(sc: SparkContext, dataFile: String, numFeaturesIn: Int) = {
      val data = sc.textFile(dataFile)
	  val dataArrays = data.map { line =>
								val fields = line.split(",")
								if(fields(0) contains ":") {
									val indVals = (0 to (fields.length-2)).map { i =>
																	val parts = fields(i).split(":")
																	(parts(0).toInt, parts(1).toDouble) // index, value
																}.unzip
									val yVal = fields(fields.length-1).split(":")(1).toInt
									// The bias/intercept element is assumed to be part of the index-value encoded vector (for example due to mahout kind of encoding)
									(yVal, (indVals._1.toArray, indVals._2.toArray))
								}
								else {
									val vals = fields.map { _.toDouble }
									// Bias/intercept of 1.0 added in front
									val x = 1.0 +: vals.slice(0, vals.length-1)
									val yVal = vals(vals.length-1).toInt
									(yVal, x)
								}
							}
	  val featureVec = dataArrays.take(1)(0)._2
	  val numFeatures = if(numFeaturesIn == 0) {
							featureVec match {
								case (indices, values) => dataArrays.map { p => p.asInstanceOf[Tuple2[Int, Tuple2[Array[Int], Array[Double]]]]._2._1.max+1 }.reduce((acc, curr) => if(acc > curr) acc else curr) // no bias/intercept
								case values: Array[Double] => values.asInstanceOf[Array[Double]].length - 1 // to account for bias/intercept element
								case _ => 0 // throw exception
								}
						}
						else
							numFeaturesIn
	  
	  val featureVectors = featureVec match {
			case (indices, values) => dataArrays.map { p => 
										val p1 = p.asInstanceOf[Tuple2[Int, Tuple2[Array[Int], Array[Double]]]]
										(p1._1, new VectorBuilder[Double](p1._2._1, p1._2._2, p1._2._1.length, numFeatures+1).toSparseVector)
									}
			case values: Array[Double] => dataArrays.map { p => val p1 = p.asInstanceOf[Tuple2[Int, Array[Double]]]; (p1._1, DenseVector(p1._2)) }
		}
/*	  
	  // Use this code if the above code to build featureVectors fails to compile due to existiantials feature of scala.
	  val featureVectors = dataArrays.map { p => 
	    p._2 match {
			case (indices: Array[Int], values: Array[Double]) => {
												(p._1, new VectorBuilder[Double](indices, values, indices.length, numFeatures+1).toSparseVector)
											}
			case values: Array[Double] => (p._1, DenseVector(values))
			//case _ => p // throw exception
		}
	  }
*/
	  // This will allow the feature vectors to be cached and avoid recomputation.
	  // Building a SparseVector is expensive operation since it involves sorting.
	  (numFeatures, featureVectors.cache)
  }
  
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SparkLRMultiClass")
    val sc = new SparkContext(sparkConf)

	var dataFile = "examples/src/main/resources/ny-weather-preprocessed.csv"
	var numClasses = 8
	var lambda = 1.0 // regularization parameter
	var alpha = 3.0 // learning rate
	var ITERATIONS = 30
	var costThreshold = 0.01
	var numFeaturesIn = 0

	// TODO: Crude cmd line args processing. Change later to use --name value style.
	if(args.length > 0) dataFile = args(0)
	if(args.length > 1) numClasses = args(1).toInt
	if(args.length > 2) lambda = args(2).toDouble
	if(args.length > 3) alpha = args(3).toDouble
	if(args.length > 4) ITERATIONS = args(4).toInt
	if(args.length > 5) costThreshold = args(5).toDouble
	if(args.length > 6) numFeaturesIn = args(6).toInt
	
    val (numFeatures, featureVectors) = csv2featureVectors(sc, dataFile, numFeaturesIn)
	
	val m : Double = featureVectors.count // size of training data
	var all_theta = DenseMatrix.zeros[Double](numClasses, numFeatures+1)
	// training
	for(c <- 0 to (numClasses-1)) {
		var theta = DenseVector.zeros[Double](numFeatures+1)
		var cost = 0.0
		breakable { for (i <- 1 to ITERATIONS) {
			val start = System.currentTimeMillis
			val grad_cost = featureVectors.map {p =>
				val (yVal, x) = p
				val y = if(yVal == c) 1 else 0
				val z = theta.t*x
				val h = sigmoid(z)
				val grad = x*(h - y)
				val J = -(if(y == 1) breeze.numerics.log(h) else breeze.numerics.log(1.0-h))
				(grad, J)
			}.reduce {(acc, curr) => ((acc._1 + curr._1), (acc._2 + curr._2))}
			val end = System.currentTimeMillis
			var grad = (grad_cost._1 + theta*lambda)/m
			grad(0) -= theta(0)*lambda/m
			cost = (grad_cost._2 + lambda*(theta.t*theta - theta(0)*theta(0))/2.0)/m
			theta -= grad*alpha
			println("label: " + c + ", Cost: " + cost + ", time: " + (end-start)/1000.0)
			if(cost <= costThreshold) {
				println("Terminating gradient descent for label " + c);
				break
			}
		} }
		println("Label: " + c + ", final cost: " + cost + ", theta: " + theta);
		all_theta(c, ::) := theta.t
	}

	println("Final model: ")
	println(all_theta.toString(Int.MaxValue, Int.MaxValue))
	
	// Prediction
	val predictions = featureVectors.map {p =>
				val (yVal, x) = p
				val z = all_theta*x
				val h = sigmoid(z)
				val c = argmax(h)
				(yVal, c)
			}

	val correctPredictions = predictions.map { p => if(p._1 == p._2) 1 else 0 }.reduce(_ + _)
	
	val accuracy = correctPredictions/m
	println("Total correct predictions:  " + correctPredictions + " out of " + m + ", accuracy: " + accuracy);

	val confusionMatrix = predictions.map {p => ((p._1, p._2), 1) }.reduceByKey {(a,b) => a + b}.collect
	println("Distribution of predictions: ")
	confusionMatrix.map { x => x match { case ((actualVal, predictedVal), count) => println("(("+ actualVal+","+predictedVal+")," + count + ")") }}

	sc.stop()
}
}
