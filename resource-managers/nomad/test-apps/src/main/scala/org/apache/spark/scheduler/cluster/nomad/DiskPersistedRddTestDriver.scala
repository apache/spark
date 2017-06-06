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

package org.apache.spark.scheduler.cluster.nomad

import scala.util.Random

import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

private[spark] object DiskPersistedRddTestDriver extends TestApplication {

  def main(args: Array[String]): Unit = {
    checkArgs(args)("result_url")
    val Array(resultUrl) = args

    val sc = new SparkContext()
    try {
      httpPut(resultUrl) {
        sc.parallelize(Seq(Random.nextString(123456), Random.nextString(234567)), 2)
          .persist(StorageLevel.DISK_ONLY)
          .map(string => string + string)
          .persist(StorageLevel.DISK_ONLY)
          .groupBy(_.length) // force a shuffle
          .persist(StorageLevel.DISK_ONLY)
          .map(_._2.map(_.length).sum)
          .reduce(_ + _)
          .toString
      }
    } finally sc.stop()
  }

}
