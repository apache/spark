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

package org.apache.spark.sql.sources

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

object CombineSmallFile {
  def combineWithFiles[T](rdd: RDD[T], sqlContext: SQLContext, inputFiles: Array[FileStatus])
      : RDD[T] = {
    if (sqlContext.conf.combineSmallFile) {
      val totalLen = inputFiles.map { file =>
        if (file.isDir) 0L else file.getLen
      }.sum
      val numPartitions = (totalLen / sqlContext.conf.splitSize + 1).toInt
      rdd.coalesce(numPartitions)
    } else {
      rdd
    }
  }

  def combineWithPath[T](rdd: RDD[T], sqlContext: SQLContext, path: String): RDD[T] = {
    val fs = FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)
    val inputFiles = fs.listStatus(new Path(path))
    combineWithFiles[T](rdd, sqlContext, inputFiles)
  }
}
