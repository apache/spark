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

package org.apache.spark.mllib.discretization

import scala.util.Random
import org.scalatest.FunSuite
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.LocalSparkContext

object EntropyMinimizationDiscretizerSuite {
  val nFeatures = 5
  val nDatapoints = 50
  val nLabels = 3
  val nPartitions = 3
	    
	def generateLabeledData : Array[LabeledPoint] = {
	    val rnd = new Random(42)
	    val labels = Array.fill[Double](nLabels)(rnd.nextDouble)
	    	    
	    Array.fill[LabeledPoint](nDatapoints) {
	        LabeledPoint(labels(rnd.nextInt(nLabels)),
	                     Array.fill[Double](nFeatures)(rnd.nextDouble))
	    } 
	}
}

class EntropyMinimizationDiscretizerSuite extends FunSuite with LocalSparkContext {
    
  test("EMD discretization") {
    val rnd = new Random(13)
		
		val data = for (i <- 1 to 99) yield
			if (i <= 33) {
			    LabeledPoint(1.0, Array(i.toDouble + rnd.nextDouble*2 - 1))
			} else if (i <= 66) {
			    LabeledPoint(2.0, Array(i.toDouble + rnd.nextDouble*2 - 1))
			} else {
			    LabeledPoint(3.0, Array(i.toDouble + rnd.nextDouble*2 - 1))
			}
		
		val shuffledData = data.sortWith((lp1, lp2) => rnd.nextDouble < 0.5)
		
		val rdd = sc.parallelize(shuffledData, 3)
		
		val thresholds = EntropyMinimizationDiscretizer(rdd, Seq(0)).getThresholdsForFeature(0)
				
		val thresholdsArray = thresholds.toArray
		if (math.abs(thresholdsArray(1) - 33.5) > 1.55) {
		    fail("Selected thresholds aren't what they should be.")
		}
		if (math.abs(thresholdsArray(2) - 66.5) > 1.55) {
		    fail("Selected thresholds aren't what they should be.")
		}
  }
    
}