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

import org.apache.hadoop.fs.{FileStatus, Path}

/**
 * A canonical representation of a file path. This class is intended to provide
 * type-safety to the way that Spark handles Paths. Paths can be represented as
 * Strings in multiple ways, which are not always compatible. Spark regularly uses
 * two ways: 1. hadoop Path.toString and java URI.toString.
 */
case class SparkPath private (private val underlying: String) {
  def urlEncoded: String = underlying
  def toUri: URI = new URI(underlying)
  def toPath: Path = new Path(toUri)
  override def toString: String = underlying
}

object SparkPath {
  /**
   * Creates a SparkPath from a hadoop Path string.
   * Please be very sure that the provided string is encoded (or not encoded) in the right way.
   *
   * Please see the hadoop Path documentation here:
   * https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/Path.html#Path-java.lang.String-
   */
  def fromPathString(str: String): SparkPath = fromPath(new Path(str))
  def fromPath(path: Path): SparkPath = fromUri(path.toUri)
  def fromFileStatus(fs: FileStatus): SparkPath = fromPath(fs.getPath)

  /**
   * Creates a SparkPath from a url-encoded string.
   * Note: It is the responsibility of the caller to ensure that str is a valid url-encoded string.
   */
  def fromUrlString(str: String): SparkPath = SparkPath(str)
  def fromUri(uri: URI): SparkPath = fromUrlString(uri.toString)
}
