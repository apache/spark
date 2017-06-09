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

package org.apache.spark.util

/**
 * Utilities for working with Spark version strings
 */
private[spark] object VersionUtils {

  private val majorMinorRegex = """^(\d+)\.(\d+)(\..*)?$""".r

  /**
   * Given a Spark version string, return the major version number.
   * E.g., for 2.0.1-SNAPSHOT, return 2.
   */
  def majorVersion(sparkVersion: String): Int = majorMinorVersion(sparkVersion)._1

  /**
   * Given a Spark version string, return the minor version number.
   * E.g., for 2.0.1-SNAPSHOT, return 0.
   */
  def minorVersion(sparkVersion: String): Int = majorMinorVersion(sparkVersion)._2

  /**
   * Given a Spark version string, return the (major version number, minor version number).
   * E.g., for 2.0.1-SNAPSHOT, return (2, 0).
   */
  def majorMinorVersion(sparkVersion: String): (Int, Int) = {
    majorMinorRegex.findFirstMatchIn(sparkVersion) match {
      case Some(m) =>
        (m.group(1).toInt, m.group(2).toInt)
      case None =>
        throw new IllegalArgumentException(s"Spark tried to parse '$sparkVersion' as a Spark" +
          s" version string, but it could not find the major and minor version numbers.")
    }
  }
}
