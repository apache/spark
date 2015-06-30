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

package org.apache.spark.api.r

import java.io.File

import org.apache.spark.SparkException

private[spark] object RUtils {
  /**
   * Get the SparkR package path in the local spark distribution.
   */
  def localSparkRPackagePath: Option[String] = {
    val sparkHome = sys.env.get("SPARK_HOME")
    sparkHome.map(
      Seq(_, "R", "lib").mkString(File.separator)
    )
  }

  /**
   * Get the SparkR package path in various deployment modes.
   */
  def sparkRPackagePath(driver: Boolean): String = {
    val yarnMode = sys.env.get("SPARK_YARN_MODE")
    if (!yarnMode.isEmpty && yarnMode.get == "true" &&
        !(driver && System.getProperty("spark.master") == "yarn-client")) {
      // For workers in YARN modes and driver in yarn cluster mode,
      // the SparkR package distributed as an archive resource should be pointed to
      // by a symbol link "sparkr" in the current directory.
      new File("sparkr").getAbsolutePath
    } else {
      // TBD: add support for MESOS
      val rPackagePath = localSparkRPackagePath
      if (rPackagePath.isEmpty) {
        throw new SparkException("SPARK_HOME not set. Can't locate SparkR package.")
      } 
      rPackagePath.get  
    }
  }
}
