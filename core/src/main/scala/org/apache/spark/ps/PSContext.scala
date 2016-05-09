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

package org.apache.spark.ps

import scala.collection.mutable

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Parameter Server Context
 */
class PSContext(sc : SparkContext) extends Logging {

  initialize()

  private def initialize() {
    sc.startLatch.await()
  }

  def registerPSTable[T](model: RDD[T]): Int = {
    -1
  }

  def loadPSModel(model: RDD[(String, Array[Double])]): Unit = {
    model.runWithPS[Unit](1, (array, psClient) => {
      array.foreach(e => {
        psClient.set(e._1, e._2)
      })
    })
  }

  def downloadPSModel(keys: Array[String], numPartition: Int): Array[Array[Double]] = {
    sc.parallelize(keys, numPartition)
      .runWithPS[Array[Array[Double]]](1, (array, psClient) => {
        val res = new mutable.ArrayBuffer[Array[Double]]
        array.foreach(e => {
          val value = psClient.get(e)
          res.+=(value)
        })
      res.toArray
    }).flatMap(arrs => arrs.toIterator)
  }
}
