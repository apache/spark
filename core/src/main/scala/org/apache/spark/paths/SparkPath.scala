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

package org.apache.spark.paths

import java.net.URI

import org.apache.hadoop.fs.Path

/**
 * A Spark Path is the result of a URI.toString call.
 */
case class SparkPath private (private val underlying: String) {
  def uriEncoded: String = underlying
  def uri: URI = new URI(underlying)
  def hadoopPath: Path = new Path(uri)
}

object SparkPath {
  def fromPathString(str: String): SparkPath = fromPath(new Path(str))
  def fromPath(path: Path): SparkPath = fromUri(path.toUri)
  def fromUri(uri: URI): SparkPath = fromUriString(uri.toString)
  def fromUriString(str: String): SparkPath = SparkPath(str)
}
