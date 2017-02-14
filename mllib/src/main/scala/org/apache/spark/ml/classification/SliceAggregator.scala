// scalastyle:off

package org.apache.spark.ml.classification

import java.util.Random

import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.ml.rdd.RDDFunctions._
import org.apache.spark.ml.feature.{Instance, LabeledPoint}
import org.apache.spark.ml.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

/**
 * Created by kevinzwyou on 17-1-22.
 */
object SliceAggregator {

  def generateData(sampleNum: Int, featLength: Int, partitionNum: Int): RDD[Instance] = {

    def randVector(rand: Random, dim: Int): Vector = {
      Vectors.dense(Array.tabulate(dim)(i => rand.nextGaussian()))
    }

    val spark = SparkSession.builder().getOrCreate()

    val rand = new Random(42)
    val initModel = randVector(rand, featLength)
    println(s"init model: ${initModel.toArray.slice(0, 100).mkString(" ")}")

    val bcModel = spark.sparkContext.broadcast(initModel)

    spark.sparkContext.parallelize(0 until sampleNum, partitionNum)
      .map { id =>
        val rand = new Random(id)
        val feat = randVector(rand, featLength)

        val margin = BLAS.dot(feat, bcModel.value)
        val prob = 1.0 / (1.0 + math.exp(-1 * margin))

        val label = if (rand.nextDouble() > prob) 0.0 else 1.0
        Instance(label, 1.0, feat)
      }
  }

  def compare(instances: RDD[Instance], featNum: Int, slice: Int, depth: Int): (Long, Long, Long) = {
    val thisInstances = if (instances.first().features.size == featNum) {
      instances
    } else {
      val thisInstances = instances.map { case Instance(label, weight, features) =>
        Instance(label, weight, Vectors.dense(features.toArray.slice(0, featNum)))
      }
      thisInstances.cache()
      thisInstances.count()
      thisInstances
    }

    // slice aggregate
    var startTime = System.currentTimeMillis()
    val summarizer = {
      val seqOp = (c: MultivariateOnlineSummarizer, instance: Instance) =>
        c.add(instance.features, instance.weight)
      val combOp = (c1: MultivariateOnlineSummarizer, c2: MultivariateOnlineSummarizer) =>
        c1.merge(c2)
      thisInstances.sliceAggregate(new MultivariateOnlineSummarizer)(seqOp, combOp, slice)
    }
    var endTime = System.currentTimeMillis()
    val sliceTime = endTime - startTime
    println(s"slice aggregate time: $sliceTime ms")
//    println(s"summarizer count: ${summarizer.count}")
//    println(s"summarizer variance: ${summarizer.variance.toArray.slice(0, 100).mkString(" ")}")
//    println(s"summarizer mean: ${summarizer.mean.toArray.slice(0, 100).mkString(" ")}")
//    println(s"summarizer min: ${summarizer.min.toArray.slice(0, 100).mkString(" ")}")
//    println(s"summarizer max: ${summarizer.max.toArray.slice(0, 100).mkString(" ")}")

    // slice aggregate
    startTime = System.currentTimeMillis()
    val summarizer2 = {
      val seqOp = (c: MultivariateOnlineSummarizer, instance: Instance) =>
        c.add(instance.features, instance.weight)
      val combOp = (c1: MultivariateOnlineSummarizer, c2: MultivariateOnlineSummarizer) =>
        c1.merge(c2)
      thisInstances.sliceAggregate(new MultivariateOnlineSummarizer)(seqOp, combOp, slice)
    }
    endTime = System.currentTimeMillis()
    val slice2Time = endTime - startTime
    println(s"slice2 aggregate time: $slice2Time ms")


    // tree aggregate
    var treeTime = 0L
    try {
      startTime = System.currentTimeMillis()
      val summarizer1 = {
        val seqOp = (c: MultivariateOnlineSummarizer, instance: Instance) =>
          c.add(instance.features, instance.weight)
        val combOp = (c1: MultivariateOnlineSummarizer, c2: MultivariateOnlineSummarizer) =>
          c1.merge(c2)
//        thisInstances.treeAggregate(new MultivariateOnlineSummarizer)(seqOp, combOp, depth)
      }
      endTime = System.currentTimeMillis()
      treeTime = endTime - startTime
      println(s"tree aggregate time: $treeTime ms")
//      println(s"summarizer count: ${summarizer1.count}")
//      println(s"summarizer variance: ${summarizer1.variance.toArray.slice(0, 100).mkString(" ")}")
//      println(s"summarizer mean: ${summarizer1.mean.toArray.slice(0, 100).mkString(" ")}")
//      println(s"summarizer min: ${summarizer1.min.toArray.slice(0, 100).mkString(" ")}")
//      println(s"summarizer max: ${summarizer1.max.toArray.slice(0, 100).mkString(" ")}")
    } catch {
      case e: Exception => println("treeAggregate fail. feat num: " + featNum)
    }

    thisInstances.unpersist()
    (sliceTime, slice2Time, treeTime)
  }


  def main(args: Array[String]) {
    val argsMap = SparkLR.parseArgs(args)

    val mode = argsMap.getOrElse("mode", "yarn-cluster")
    val input = argsMap.getOrElse("input", null)
    val featNum = argsMap.getOrElse("featNum", "100").toInt
    val sampleNum = argsMap.getOrElse("sampleNum", "1000").toInt
    val partitionNum = argsMap.getOrElse("partitionNum", "10").toInt
    val depth = argsMap.getOrElse("depth", "2").toInt
    val slice = argsMap.getOrElse("slice", "10").toInt

    val spark = SparkSession.builder()
      .master(mode)
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    Thread.sleep(20 * 1000)

    val instances = generateData(sampleNum, featNum, partitionNum)
    instances.cache()
    println(s"instance count: ${instances.count()} feat length: ${instances.first().features.size}")


    val n = 10
    val wan = 10000
//    val featLenArray = Array(1 * wan, 10 * wan, 100 * wan, 250 * wan, 500 * wan, 750 * wan, 1000 * wan, 2000 * wan)
    val featLenArray = Array(2000 * wan)

    featLenArray.foreach { thisFeatNum =>
      val thisSlice = if (thisFeatNum > wan) math.ceil(thisFeatNum / wan).toInt else 1
      val thisDepth = if (thisFeatNum < 500 * wan) 2 else if (thisFeatNum < 1000 * wan) 3
        else if (thisFeatNum < 2000 * wan) 4 else 5

      val timeCosts = (0 until n).map { i =>
        println(s"counter: $i")
        compare(instances, thisFeatNum, thisSlice, thisDepth)
      }
      println(s"featNum: $thisFeatNum")
      println(s"slice time: ${timeCosts.map(_._1).sum / n}")
      println(s"slice2 time: ${timeCosts.map(_._2).sum / n}")
      println(s"tree time: ${timeCosts.map(_._3).sum / n}")
    }

//    val wan = 10000
//    val featLenArray = Array(1 * wan, 10 * wan, 100 * wan, 250 * wan, 500 * wan, 750 * wan, 1000 * wan, 2000 * wan)
//    val sliceNumArray = new ArrayBuffer[Int]()
//    val depthNumArray = new ArrayBuffer[Int]()
//    val compareTime = featLenArray.map { featLength =>
//      val thisSlice = if (featLength > wan) math.ceil(featLength / wan).toInt else 1
//      val thisDepth = if (featLength < 500 * wan) 2 else if (featLength < 1000 * wan) 3 else 4
//      sliceNumArray += thisSlice
//      depthNumArray += thisDepth
//      compare(instances, featLength, thisSlice, thisDepth)
//    }
//    println("feat len:\t" + featLenArray.mkString("\t"))
//    println("slice num:\t" + sliceNumArray.mkString("\t"))
//    println("depth num:\t" + depthNumArray.mkString("\t"))
//    println("slice time:\t" + compareTime.map(_._1.toDouble / 1000).mkString("\t"))
//    println("tree time:\t" + compareTime.map(_._2.toDouble / 1000).mkString("\t"))
  }
}
// scalastyle:on