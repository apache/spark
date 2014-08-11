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

package org.apache.spark.streaming.api.python

import java.io._
import java.io.{ObjectInputStream, IOException}
import java.util.{List => JList, ArrayList => JArrayList, Map => JMap, Collections}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.collection.JavaConversions._


import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java._
import org.apache.spark.api.python._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{StreamingContext, Duration, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.api.java._



class PythonDStream[T: ClassTag](
    parent: DStream[T],
    command: Array[Byte],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    preservePartitoning: Boolean,
    pythonExec: String,
    broadcastVars: JList[Broadcast[Array[Byte]]],
    accumulator: Accumulator[JList[Array[Byte]]])
  extends DStream[Array[Byte]](parent.ssc) {

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  //pythonDStream compute
  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    parent.getOrCompute(validTime) match{
      case Some(rdd) =>
        logInfo("RDD ID in python DStream     ===========")
        logInfo("RDD id " + rdd.id)
        val pythonRDD = new PythonRDD(rdd, command, envVars, pythonIncludes, preservePartitoning, pythonExec, broadcastVars, accumulator)
        Some(pythonRDD.asJavaRDD.rdd)
      case None => None
    }
  }

  def foreachRDD(foreachFunc: PythonRDDFunction) {
    new PythonForeachDStream(this, context.sparkContext.clean(foreachFunc, false)).register()
  }

  val asJavaDStream  = JavaDStream.fromDStream(this)

  /**
   * Print the first ten elements of each PythonRDD generated in this PythonDStream. This is an output
   * operator, so this PythonDStream will be registered as an output stream and there materialized.
   * Since serialized Python object is readable by Python, pyprint writes out binary data to
   * temporary file and run python script to deserialized and print the first ten elements
   */
  private[streaming] def ppyprint() {
    def foreachFunc = (rdd: RDD[Array[Byte]], time: Time) => {
      val iter = rdd.take(11).iterator

      // make a temporary file
      val prefix = "spark"
      val suffix = ".tmp"
      val tempFile = File.createTempFile(prefix, suffix)
      val tempFileStream = new DataOutputStream(new FileOutputStream(tempFile.getAbsolutePath))
      //write out serialized python object
      PythonRDD.writeIteratorToStream(iter, tempFileStream)
      tempFileStream.close()

      // This value has to be passed from python
      //val pythonExec = new ProcessBuilder().environment().get("PYSPARK_PYTHON")
      val sparkHome = new ProcessBuilder().environment().get("SPARK_HOME")
      //val pb = new ProcessBuilder(Seq(pythonExec, sparkHome + "/python/pyspark/streaming/pyprint.py", tempFile.getAbsolutePath())) // why this fails to compile???
      //absolute path to the python script is needed to change because we do not use pysparkstreaming
      val pb = new ProcessBuilder(pythonExec, sparkHome + "/python/pysparkstreaming/streaming/pyprint.py", tempFile.getAbsolutePath)
      val workerEnv = pb.environment()

      //envVars also need to be pass
      //workerEnv.putAll(envVars)
      val pythonPath = sparkHome + "/python/" + File.pathSeparator + workerEnv.get("PYTHONPATH")
      workerEnv.put("PYTHONPATH", pythonPath)
      val worker = pb.start()
      val is = worker.getInputStream()
      val isr = new InputStreamReader(is)
      val br = new BufferedReader(isr)

      println ("-------------------------------------------")
      println ("Time: " + time)
      println ("-------------------------------------------")

      //print value from python std out
      var line = ""
      breakable {
        while (true) {
          line = br.readLine()
          if (line == null) break()
          println(line)
        }
      }
      //delete temporary file
      tempFile.delete()
      println()

    }
    new ForEachDStream(this, context.sparkContext.clean(foreachFunc)).register()
  }
}


private class PairwiseDStream(prev:DStream[Array[Byte]]) extends
DStream[(Long, Array[Byte])](prev.ssc){
  override def dependencies = List(prev)

  override def slideDuration: Duration = prev.slideDuration

  override def compute(validTime:Time):Option[RDD[(Long, Array[Byte])]]={
    prev.getOrCompute(validTime) match{
      case Some(rdd)=>Some(rdd)
        val pairwiseRDD = new PairwiseRDD(rdd)
        Some(pairwiseRDD.asJavaPairRDD.rdd)
      case None => None
    }
  }
  val asJavaPairDStream : JavaPairDStream[Long, Array[Byte]]  = JavaPairDStream.fromJavaDStream(this)
}


private class PythonPairwiseDStream(prev:DStream[Array[Byte]], partitioner: Partitioner) extends
DStream[Array[Byte]](prev.ssc){
  override def dependencies = List(prev)

  override def slideDuration: Duration = prev.slideDuration

  override def compute(validTime:Time):Option[RDD[Array[Byte]]]={
    prev.getOrCompute(validTime) match{
      case Some(rdd)=>Some(rdd)
        val pairwiseRDD = new PairwiseRDD(rdd)
        /*
         * Since python operation is executed by Scala after StreamingContext.start.
         * What PythonPairwiseDStream does is equivalent to python code in pySpark.
         *
         * with _JavaStackTrace(self.context) as st:
         *    pairRDD = self.ctx._jvm.PairwiseRDD(keyed._jrdd.rdd()).asJavaPairRDD()
         *    partitioner = self.ctx._jvm.PythonPartitioner(numPartitions,
         *                                                  id(partitionFunc))
         * jrdd = pairRDD.partitionBy(partitioner).values()
         * rdd = RDD(jrdd, self.ctx, BatchedSerializer(outputSerializer))
         */
        Some(pairwiseRDD.asJavaPairRDD.partitionBy(partitioner).values().rdd)
      case None => None
    }
  }

  val asJavaDStream  = JavaDStream.fromDStream(this)
}

class PythonForeachDStream(
    prev: DStream[Array[Byte]],
    foreachFunction: PythonRDDFunction
  ) extends ForEachDStream[Array[Byte]](
    prev,
    (rdd: RDD[Array[Byte]], time: Time) => {
      foreachFunction.call(rdd.toJavaRDD(), time.milliseconds)
    }
  ) {

  this.register()
}

/*
This does not work. Ignore this for now. -TD
class PythonTransformedDStream(
    prev: DStream[Array[Byte]],
    transformFunction: PythonRDDFunction
  ) extends DStream[Array[Byte]](prev.ssc) {

  override def dependencies = List(prev)

  override def slideDuration: Duration = prev.slideDuration

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    prev.getOrCompute(validTime).map(rdd => {
      transformFunction.call(rdd.toJavaRDD(), validTime.milliseconds).rdd
    })
  }

  val asJavaDStream  = JavaDStream.fromDStream(this)
  //val asJavaPairDStream : JavaPairDStream[Long, Array[Byte]]  = JavaPairDStream.fromJavaDStream(this)
}
*/
