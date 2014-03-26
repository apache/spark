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

package org.apache.spark.mllib

import org.apache.hadoop.io.Text

import org.apache.spark.mllib.input.WholeTextFileInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Extra functions available on SparkContext of mllib through an implicit conversion. Import
 * `org.apache.spark.mllib.MLContext._` at the top of your program to use these functions.
 */
class MLContext(self: SparkContext) {

  /**
   * Read a directory of text files from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI. Each file is read as a single record and returned in a
   * key-value pair, where the key is the path of each file, and the value is the content of each
   * file.
   */
  def wholeTextFile(path: String): RDD[(String, String)] = {
    self.newAPIHadoopFile(
      path,
      classOf[WholeTextFileInputFormat],
      classOf[String],
      classOf[Text]).mapValues(_.toString)
  }
}


/**
 * The MLContext object contains a number of implicit conversions and parameters for use with
 * various mllib features.
 */
object MLContext {
  implicit def sparkContextToMLContext(sc: SparkContext) = new MLContext(sc)
}
