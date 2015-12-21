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
package org.apache.spark.examples

import scala.Tuple2
import org.apache.spark.SparkConf

import org.apache.spark._

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

// han sampler import begin
import java.io._
import org.apache.spark.storage._
import java.lang.System
import java.util.Date
import java.util.concurrent._
import java.text._
import scala.util.Properties
// han sampler import end

//case class Counts(word: String, count: Int)

object SortDF {

  val SPACE = Pattern.compile(" ")

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage: WordCount <file>");
      System.exit(1);
    }

    val conf = new SparkConf().setAppName("Sort DF")

    // han register kyro classes begin
    /*
    val tuple3ArrayClass = classOf[Array[Tuple3[Any, Any, Any]]]
    val anonClass = Class.forName("scala.reflect.ClassTag$$anon$1")
    val javaClassClass = classOf[java.lang.Class[Any]]
    val StringClass = classOf[java.lang.String]
    val StringArrayClass = classOf[Array[java.lang.String]]

    conf.registerKryoClasses(Array(tuple3ArrayClass, anonClass, javaClassClass, StringClass, StringArrayClass))
    */
    // han register kyro classes end

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
            val filename: String = dirname_application + "/sparkOutput_driver_"  + sc.applicationId + "_" + es.blockManagerId + ".txt"
            val file = new File(filename)
            val writer = new FileWriter(file, true)
            if (!file.exists()) {
              file.createNewFile()
              writer.write(sc.applicationId + "_" + es.blockManagerId + "\n")
              writer.flush()
              //writer.close()
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

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val lines = sc.textFile(args(0), 1)
    //val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)/2
    val data = lines.map((_, 1)).map(t=>Counts(t._1, t._2)).toDF()
    data.registerTempTable("data")

    //val partitioner = new HashPartitioner(partitions = parallel)
    val sorted = data.sort("word").select("word")

    sorted.save(args(1))

    //output.foreach(t => println(t._1 + ": " + t._2))

    sc.stop()

    // han sampler 2 begin
    f.cancel(true)
    // hand sampler 2 end
    sys.exit(0)
  }
}
// scalastyle:on println
