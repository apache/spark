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
package org.apache.spark.io

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

/**
 * This class extends the MultipleTextOutputFormat, allowing to write RDD
 * data to different text files by key.
 */
class RDDMultipleTextOutputFormat[K, V] extends MultipleTextOutputFormat[K, V] {
  override def generateActualKey(key: K, value: V): K =
    NullWritable.get().asInstanceOf[K]

  override def generateFileNameForKeyValue(key: K, value: V, name: String): String =
    new Path(key.toString, name).toString
}
