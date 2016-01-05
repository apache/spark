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

import org.apache.log4j.{ Level, Logger }
import scopt.OptionParser

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.Vectors

// han sampler import begin
import java.io._
import org.apache.spark.storage._
import java.lang.System
import java.util.Date
import java.util.concurrent._
import java.text._
import scala.util.Properties
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.hadoop.io.LongWritable
import org.apache.mahout.math.VectorWritable

// han sampler import end

/**
 * An example k-means app. Run with
 * {{{
 * ./bin/run-example org.apache.spark.examples.mllib.DenseKMeans [options] <input>
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object DenseKMeans {

  object InitializationMode extends Enumeration {
    type InitializationMode = Value
    val Random, Parallel = Value
  }

  import InitializationMode._

  case class Params(
    input: String = null,
    k: Int = -1,
    numIterations: Int = 10,

    // han
    storageLevel: String = "NONE",
    minNumPartitions: Int = -1,

    initializationMode: InitializationMode = Parallel) extends AbstractParams[Params]

  def main(args: Array[String]) {

    // han
    println("Application starts:\n" + System.nanoTime())

    val defaultParams = Params()

    val parser = new OptionParser[Params]("DenseKMeans") {
      head("DenseKMeans: an example k-means app for dense data.")
      opt[Int]('k', "k")
        .required()
        .text(s"number of clusters, required")
        .action((x, c) => c.copy(k = x))

      // han
      opt[String]("storageLevel")
        .required()
        .text(s"storage level of RDD caching (NONE, MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY), required")
        .action((x, c) => c.copy(storageLevel = x))
      opt[Int]("minNumPartitions")
        .text(s"minimal number of partitions of the input file")
        .action((x, c) => c.copy(minNumPartitions = x))

      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[String]("initMode")
        .text(s"initialization mode (${InitializationMode.values.mkString(",")}), " +
          s"default: ${defaultParams.initializationMode}")
        .action((x, c) => c.copy(initializationMode = InitializationMode.withName(x)))
      arg[String]("<input>")
        .text("input paths to examples")
        .required()
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }

    // han
    println("Application completes:\n" + System.nanoTime())

  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"DenseKMeans with $params")

    // mayuresh
    //if (params.storageLevel=="MEMORY_ONLY_SER") {
      //  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //  conf.set("spark.kryo.registrationRequired", "true")
      //  conf.registerKryoClasses(Array(classOf[Array[java.lang.Double]],classOf[org.apache.spark.mllib.linalg.DenseVector]))
    //}

    val sc = new SparkContext(conf)

    // han sampler 1 begin
    val SAMPLING_PERIOD: Long = 10
    val TIMESTAMP_PERIOD: Long = 1000

    var dateFormat: DateFormat = new SimpleDateFormat("hh:mm:ss")

    val dirname_application = Properties.envOrElse("SPARK_HOME", "/home/mayuresh/spark-1.5.1") + "/logs/" + sc.applicationId
    val dir_application = new File(dirname_application)
    if (!dir_application.exists())
      dir_application.mkdirs()

    val ex = new ScheduledThreadPoolExecutor(1)
    val task = new Runnable {
      var i: Long = 0
      override def run {

        //        sc.getExecutorStorageStatus.filter(s => s.blockManagerId.host.contains("slave1"))
        sc.getExecutorStorageStatus.foreach {
          es =>
            val filename: String = dirname_application + "/sparkOutput_driver_" + sc.applicationId + "_" + es.blockManagerId + ".txt"
            val file = new File(filename)
            val writer = new FileWriter(file, true)
            if (!file.exists()) {
              file.createNewFile()
              writer.write(sc.applicationId + "_" + es.blockManagerId + "\n")
              writer.flush()
              writer.close()
            }
            var s = es.memUsed.toString()
            //println(s)
            if (i % TIMESTAMP_PERIOD == 0) {
              i = 0
              var time: String = dateFormat.format(new Date())
              s += "\t" + time
            }

            writer.write(s + "\n")
            writer.flush()
            writer.close()
        }
        i = i + SAMPLING_PERIOD
      }
    }
    val f = ex.scheduleAtFixedRate(task, 0, SAMPLING_PERIOD, TimeUnit.MILLISECONDS)
    // han sampler 1 end 

    // han
    //    Logger.getRootLogger.setLevel(Level.WARN)

    // han
    //    val examples = sc.textFile(params.input).map { line =>
    //      Vectors.dense(line.split(' ').map(_.toDouble))
    //    }.cache()

    var storageLevel: StorageLevel = null
    storageLevel = params.storageLevel match {
      case "NONE" => StorageLevel.NONE
      case "MEMORY_ONLY" => StorageLevel.MEMORY_ONLY
      case "MEMORY_AND_DISK" => StorageLevel.MEMORY_AND_DISK
      case "DISK_ONLY" => StorageLevel.DISK_ONLY
      case "MEMORY_ONLY_SER" => StorageLevel.MEMORY_ONLY_SER
    }

    var examples: RDD[Vector] = null
    /*
    if (params.minNumPartitions < 0)
      examples = sc.textFile(params.input).map { line =>
        //      examples = sc.binaryFiles(params.input, 1).map { line =>
        Vectors.dense(line.split(' ').map(_.toDouble))
      }.persist(storageLevel)
    else examples = sc.textFile(params.input, params.minNumPartitions).map { line =>
      Vectors.dense(line.split(' ').map(_.toDouble))
    }.persist(storageLevel)
    */

    val data = sc.sequenceFile[LongWritable, VectorWritable](params.input)
    examples = data.map {
      case (k, v) =>
        var vector: Array[Double] = new Array[Double](v.get().size)
        for (i <- 0 until v.get().size) vector(i) = v.get().get(i)
        Vectors.dense(vector)
    }.persist(storageLevel)

    val numExamples = examples.count()

    println(s"numExamples = $numExamples.")

    val initMode = params.initializationMode match {
      case Random => KMeans.RANDOM
      case Parallel => KMeans.K_MEANS_PARALLEL
    }

    // han
    //    val model = new KMeans()
    //      .setInitializationMode(initMode)
    //      .setK(params.k)
    //      .setMaxIterations(params.numIterations)
    //      .run(examples)
    val model = new KMeans()
      .setInitializationMode(initMode)
      .setK(params.k)
      .setMaxIterations(params.numIterations)
      .setStorageLevel(storageLevel)
      .run(examples)

    val cost = model.computeCost(examples)

    println(s"Total cost = $cost.")

    // han
    // Save and load model
    /*
    val outputPath = "/kmeans-huge/output/" + sc.applicationId
    model.save(sc, outputPath)
    val sameModel = KMeansModel.load(sc, outputPath)
    */
    sc.stop()

    // han sampler 2 begin
    f.cancel(true)
    sys.exit(0)
    // hand sampler 2 end
  }
}
// scalastyle:on println
