package org.apache.spark.mllib.discretization

import org.scalatest.FunSuite
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.regression.LabeledPoint
import scala.util.Random
import org.apache.spark.mllib.util.InfoTheory

object EMDDiscretizerSuite {
    val nFeatures = 5
    val nDatapoints = 50
    val nLabels = 3
    val nPartitions = 3
	    
	def generateLabeledData : Array[LabeledPoint] =
	{
        
	    val rnd = new Random(42)
	    val labels = Array.fill[Double](nLabels)(rnd.nextDouble)
	    	    
	    Array.fill[LabeledPoint](nDatapoints) {
	        LabeledPoint(labels(rnd.nextInt(nLabels)),
	                     Array.fill[Double](nFeatures)(rnd.nextDouble))
	    } 
	}
}

class EMDDiscretizerSuite extends FunSuite with LocalSparkContext {
    
    test("EMD discretization") {
        val rnd = new Random()
		
		val data =
		for (i <- 1 to 99) yield
			if (i <= 33) {
			    LabeledPoint(1.0, Array(i.toDouble + rnd.nextDouble*2 - 1))
			} else if (i <= 66) {
			    LabeledPoint(2.0, Array(i.toDouble + rnd.nextDouble*2 - 1))
			} else {
			    LabeledPoint(3.0, Array(i.toDouble + rnd.nextDouble*2 - 1))
			}
		
		val shuffledData = data.sortWith((lp1, lp2) => if (rnd.nextDouble < 0.5) true else false)
		
		val rdd = sc.parallelize(shuffledData, 3)
		
		val thresholds = EMDDiscretizer(rdd, Seq(0)).getThresholdsForFeature(0)
				
		val thresholdsArray = thresholds.toArray
		if (math.abs(thresholdsArray(1) - 33.5) > 1.55) {
		    fail("Selected thresholds aren't what they should be.")
		}
		if (math.abs(thresholdsArray(2) - 66.5) > 1.55) {
		    fail("Selected thresholds aren't what they should be.")
		}
    }
    
}