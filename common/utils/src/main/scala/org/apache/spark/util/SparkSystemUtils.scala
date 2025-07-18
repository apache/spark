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

private[spark] trait SparkSystemUtils {
  /**
   * The `os.name` system property.
   */
  val osName = System.getProperty("os.name")

  /**
   * The `os.arch` system property.
   */
  val osArch = System.getProperty("os.arch")

  /**
   * Whether the underlying operating system is Windows.
   */
  val isWindows = osName.regionMatches(true, 0, "Windows", 0, 7)

  /**
   * Whether the underlying operating system is Mac OS X.
   */
  val isMac = osName.regionMatches(true, 0, "Mac OS X", 0, 8)

  /**
   * Whether the underlying operating system is Mac OS X and processor is Apple Silicon.
   */
  val isMacOnAppleSilicon = isMac && osArch.equals("aarch64")

  /**
   * Whether the underlying operating system is Linux.
   */
  val isLinux = osName.regionMatches(true, 0, "Linux", 0, 5)

  /**
   * Whether the underlying operating system is UNIX.
   */
  val isUnix = Seq("AIX", "HP-UX", "Irix", "Linux", "Mac OS X", "Solaris", "SunOS", "FreeBSD",
      "OpenBSD", "NetBSD").exists(prefix => osName.regionMatches(true, 0, prefix, 0, prefix.length))
}

object SparkSystemUtils extends SparkSystemUtils
