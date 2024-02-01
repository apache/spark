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
// $example off$
import org.apache.spark.sql.SparkSession

object PowerIterationClusteringExample {
   def main(args: Array[String]): Unit = {
     val spark = SparkSession
       .builder()
       .appName(s"${this.getClass.getSimpleName}")
       .getOrCreate()

     // $example on$
     val dataset = spark.createDataFrame(Seq(
       (0L, 1L, 1.0),
       (0L, 2L, 1.0),
       (1L, 2L, 1.0),
       (3L, 4L, 1.0),
       (4L, 0L, 0.1)
     )).toDF("src", "dst", "weight")

     val model = new PowerIterationClustering().
       setK(2).
       setMaxIter(20).
       setInitMode("degree").
       setWeightCol("weight")

     val prediction = model.assignClusters(dataset).select("id", "cluster")

     //  Shows the cluster assignment
     prediction.show(false)
     // $example off$

     spark.stop()
   }
 }
