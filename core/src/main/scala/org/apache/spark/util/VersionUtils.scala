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

import org.apache.hadoop.util.VersionInfo

/**
 * Utilities for working with Spark version strings
 */
private[spark] object VersionUtils {

  private val majorMinorRegex = """^(\d+)\.(\d+)(\..*)?$""".r
  private val shortVersionRegex = """^(\d+\.\d+\.\d+)(.*)?$""".r
  private val majorMinorPatchRegex = """^(\d+)(?:\.(\d+)(?:\.(\d+)(?:[.-].*)?)?)?$""".r

  /**
   * Whether the Hadoop version used by Spark is 3.x
   */
  def isHadoop3: Boolean = majorVersion(VersionInfo.getVersion) == 3

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
   * Given a Spark version string, return the short version string.
   * E.g., for 3.0.0-SNAPSHOT, return '3.0.0'.
   */
  def shortVersion(sparkVersion: String): String = {
    shortVersionRegex.findFirstMatchIn(sparkVersion) match {
      case Some(m) => m.group(1)
      case None =>
        throw new IllegalArgumentException(s"Spark tried to parse '$sparkVersion' as a Spark" +
          s" version string, but it could not find the major/minor/maintenance version numbers.")
    }
  }

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

  /**
   * Extracts the major, minor and patch parts from the input `version`. Note that if minor or patch
   * version is missing from the input, this will return 0 for these parts. Returns `None` if the
   * input is not of a valid format.
   *
   * Examples of valid version:
   *  - 1   (extracts to (1, 0, 0))
   *  - 2.4   (extracts to (2, 4, 0))
   *  - 3.2.2   (extracts to (3, 2, 2))
   *  - 3.2.2.4   (extracts to 3, 2, 2))
   *  - 3.3.1-SNAPSHOT   (extracts to (3, 3, 1))
   *  - 3.2.2.4SNAPSHOT   (extracts to (3, 2, 2), only the first 3 components)
   *
   * Examples of invalid version:
   *  - ABC
   *  - 1X
   *  - 2.4XYZ
   *  - 2.4-SNAPSHOT
   *  - 3.4.5ABC
   *
   *  @return A non-empty option containing a 3-value tuple (major, minor, patch) iff the
   *          input is a valid version. `None` otherwise.
   */
  def majorMinorPatchVersion(version: String): Option[(Int, Int, Int)] = {
    majorMinorPatchRegex.findFirstMatchIn(version).map { m =>
      val major = m.group(1).toInt
      val minor = Option(m.group(2)).map(_.toInt).getOrElse(0)
      val patch = Option(m.group(3)).map(_.toInt).getOrElse(0)
      (major, minor, patch)
    }
  }
}
