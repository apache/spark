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

package org.apache.spark.mllib.optimization

import java.util.Random

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{SchemaRDD, SQLContext}


/**
 * :: DeveloperApi ::
 * Trait for Sampler(Used by SGD).
 */
@DeveloperApi
trait Sampler {

  /**
   *  Set input data for SGD. RDD of the set of data examples, each of
   *  the form (label, [feature values]).
   */
  def setData(data: RDD[(Double, Vector)]): Unit

  /**
   * Set the sampling fraction.
   */
  def setMiniBatchFraction(miniBatchFraction: Double): Unit

  /**
   * Get the next sampling batches.
   * @param iter - Iteration number
   * @return  Sampled data
   */
  def nextBatchSample(iter: Int): RDD[(Double, Vector)]
}

class SimpleSampler(var data: RDD[(Double, Vector)] = null,
  var miniBatchFraction: Double = 1D) extends Sampler {

  def setData(data: RDD[(Double, Vector)]): Unit = {
    this.data = data
  }

  def setMiniBatchFraction(fraction: Double): Unit = {
    this.miniBatchFraction = fraction
  }

  def nextBatchSample(iterNum: Int): RDD[(Double, Vector)] = {
    data.sample(withReplacement = false, miniBatchFraction, 42 + iterNum)
  }
}

private[mllib] case class SamplerData(rand: Double, label: Double, features: Vector)

class ExternalStorageSampler(val dataDir: String) extends Sampler {
  require(dataDir != null && dataDir.nonEmpty, "dataDir is empty!")

  var data: SchemaRDD = null
  var miniBatchFraction: Double = 0.1D

  def setData(data: RDD[(Double, Vector)]): Unit = {
    val path = new Path(dataDir, "samplerData-" + data.id)
    val sqlContext = new SQLContext(data.context)
    import sqlContext.createSchemaRDD

    data.mapPartitionsWithIndex((pid, iter) => {
      val rand: Random = new Random(pid + 37)
      iter.map { data =>
        SamplerData(rand.nextDouble(), data._1, data._2)
      }
    }).saveAsParquetFile(path.toString)
    this.data = sqlContext.parquetFile(path.toString)
    this.data.registerTempTable("sampler_data")
  }

  def setMiniBatchFraction(fraction: Double): Unit = {
    this.miniBatchFraction = fraction
    require(this.miniBatchFraction > 0 && this.miniBatchFraction < 1,
      "miniBatchFraction must be greater than 0 and less than 1!")
  }

  def nextBatchSample(iter: Int): RDD[(Double, Vector)] = {
    var s = iter * miniBatchFraction
    s = s - s.floor
    var e = (iter + 1) * miniBatchFraction
    e = e - e.floor
    val w = if (e < s) {
      s"(rand >= 0 AND rand < $e) OR (rand >= $s AND rand < 1)"

    } else {
      s"rand >= $s AND rand < $e"
    }
    data.sqlContext.sql(s"SELECT label,features FROM sampler_data WHERE $w").map(r => {
      (r.getDouble(0), r.getAs[Vector](1))
    })
  }
}
