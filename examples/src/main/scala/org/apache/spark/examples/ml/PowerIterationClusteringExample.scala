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
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.clustering.PowerIterationClustering
import org.apache.spark.sql.SparkSession
// $example off$

 /**
  * An example demonstrating power iteration clustering.
  * Run with
  * {{{
  * bin/run-example ml.PowerIterationClusteringExample
  * }}}
  */

object PowerIterationClusteringExample {

   def main(args: Array[String]): Unit = {
     val spark = SparkSession
       .builder
       .appName(s"${this.getClass.getSimpleName}")
       .getOrCreate()

     // $example on$
     val dataset = spark.createDataFrame(Seq(
       (0L, Array(1L, 2L, 4L), Array(0.9, 0.9, 0.1)),
       (1L, Array(0L, 2L), Array(0.9, 0.9)),
       (2L, Array(0L, 1L), Array(0.9, 0.9)),
       (3L, Array(4L), Array(0.9)),
       (4L, Array(0L, 3L), Array(0.1, 0.9))
     )).toDF("id", "neighbors", "similarities")

     // Trains a PIC model.
     val model = new PowerIterationClustering().
       setK(2).
       setInitMode("degree").
       setMaxIter(20)

     val prediction = model.transform(dataset).select("id", "prediction")

     //  Shows the cluster assignment
     prediction.show()
     // $example off$

     spark.stop()
   }
 }
