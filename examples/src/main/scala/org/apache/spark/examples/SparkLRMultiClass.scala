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

import breeze.linalg._
import breeze.numerics._
import org.apache.spark._
import org.apache.spark.SparkContext._

/**
   From the spark base dir:
   Compile standalone using:
   scalac -d target -cp assembly/target/scala-2.10/spark-assembly-1.1.0-SNAPSHOT-hadoop1.0.4.jar examples/src/main/scala/org/apache/spark/examples/SparkLRMultiClass.scala
   Execute using:
   java -cp target;assembly/target/scala-2.10/spark-assembly-1.1.0-SNAPSHOT-hadoop1.0.4.jar -Dspark.master=local[4] org.apache.spark.examples.SparkLRMultiClass

   NOTE: to change the spark logging level to WARN, do the following (no other way works):
   Open the file core/src/main/resources/org/apache/spark/log4j-defaults.properties and replace INFO with WARN
   Then using the command like below, update the spark archive to have the new file:
   jar uvf assembly/target/scala-2.10/spark-assembly-1.1.0-SNAPSHOT-hadoop1.0.4.jar -C core/src/main/resources org/apache/spark/log4j-defaults.properties
*/
/**
 * Logistic regression based classification for multiple lables.
 * Usage: SparkLRMultiClass [dataFileName] [numClasses] [lambda] [alpha] [iterations]
 */
object SparkLRMultiClass {
/*
	// Unused vector/matrix functions: Breeze Linear Algrbra library provides these and more.
	// Leaving these in comments since it shows a functional implementation of vector and multiplication using Array.zip
	val random = new java.security.SecureRandom
	def random2dArray(dim1: Int, dim2: Int, maxValue: Double) = Array.fill(dim1, dim2) { random.nextDouble()*maxValue }
	def vecMul(v1: Array[Double], v2: Array[Double]) : Double = (v1 zip v2).map(z => z._1*z._2).sum
	def dotProduct(vector: Array[Double], matrix: Array[Array[Double]]): Array[Double] = { 
		// ignore dimensionality checks for simplicity of example 
		(0 to (matrix(0).size - 1)).toArray.map( colIdx => { 
			val colVec: Array[Double] = matrix.map( rowVec => rowVec(colIdx) ) 
			val elemWiseProd: Array[Double] = (vector zip colVec).map( entryTuple => entryTuple._1 * entryTuple._2 ) 
			elemWiseProd.sum 
		} )
	}

	def sigmoid(z : Double) : Double = 1/(1+exp(-z))
*/

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SparkLRMultiClass")
    val sc = new SparkContext(sparkConf)

	var dataFile = "examples/src/main/resources/ny-weather-preprocessed.csv"
	var numClasses = 8
	var lambda = 1.0 // regularization parameter
	var alpha = 3.0 // learning rate
	var ITERATIONS = 30

	if(args.length > 0) dataFile = args(0)
	if(args.length > 1) numClasses = args(1).toInt
	if(args.length > 2) lambda = args(2).toDouble
	if(args.length > 3) alpha = args(3).toDouble
	if(args.length > 4) ITERATIONS = args(4).toInt
	
	val data = sc.textFile(dataFile)
	val vectors = data.map(line => line.split(",").map(java.lang.Double.parseDouble(_)))
	val numFeatures = vectors.take(1)(0).size - 1
	val m : Double = vectors.count // size of training data
	var all_theta = DenseMatrix.zeros[Double](numClasses, numFeatures+1)
	// training
	for(c <- 1 to numClasses) {
		var theta = DenseVector.zeros[Double](numFeatures+1)
		var cost = 0.0
		for (i <- 1 to ITERATIONS) {
			val grad_cost = vectors.map {p =>
				val x = DenseVector.vertcat(DenseVector(1.0), DenseVector(p.slice(0, p.length-1)))
				val yVal = p(p.length-1)
				val y = if(yVal == c) 1.0 else 0.0
				val z = theta.t*x
				val ones = DenseVector.ones[Double](numClasses)
				val h = sigmoid(z)
				val grad = x*(h - y)
				val J = -(y*breeze.numerics.log(h) + (1.0-y)*breeze.numerics.log(1.0-h))
				(grad, J)
			}.reduce((acc, curr) => ((acc._1 + curr._1), (acc._2 + curr._2)))
			var grad = (grad_cost._1 + theta*lambda)/m
			grad(0) -= theta(0)*lambda/m
			cost = (grad_cost._2 + lambda*(theta.t*theta - theta(0)*theta(0))/2.0)/m
			theta -= grad*alpha
			println("label: " + c + ", Cost: " + cost + ", gradient: " + grad)
		}
		println("Label: " + c + ", final cost: " + cost + ", theta: " + theta);
		all_theta(c-1, ::) := theta.t
	}

	println("Final model: ")
	println(all_theta.toString(Int.MaxValue, Int.MaxValue))
	
	// Prediction
	val correctPredictions = vectors.map {p =>
				val x = DenseVector.vertcat(DenseVector(1.0), DenseVector(p.slice(0, p.length-1)))
				val yVal = p(p.length-1)
				val z = all_theta*x
				val h = sigmoid(z)
				val c = argmax(h)+1
				if(c == yVal) 1 else 0
			}.reduce (_ + _)

	val accuracy = correctPredictions/m
	println("Total correct predictions:  " + correctPredictions + " out of " + m + ", accuracy: " + accuracy);

	val confusionMatrix = vectors.map {p =>
				val x = DenseVector.vertcat(DenseVector(1.0), DenseVector(p.slice(0, p.length-1)))
				val yVal = p(p.length-1)
				val z = all_theta*x
				val h = sigmoid(z)
				val c = argmax(h)+1
				((yVal.toInt, c), 1)
			}
			.reduceByKey {(a,b) => a + b}.collect
	confusionMatrix.map { x => x match { case ((actualVal, realVal), count) => println("(("+ actualVal+","+realVal+")," + count + ")") }}

	sc.stop()
}
}
