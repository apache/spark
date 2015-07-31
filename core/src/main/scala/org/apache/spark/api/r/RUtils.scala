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

import scala.collection.JavaConversions._

import org.apache.spark.{SparkEnv, SparkException}

private[spark] object RUtils {
  /**
   * Get the SparkR package path in the local spark distribution.
   */
  def localSparkRPackagePath: Option[String] = {
    val sparkHome = sys.env.get("SPARK_HOME").orElse(sys.props.get("spark.test.home"))
    sparkHome.map(
      Seq(_, "R", "lib").mkString(File.separator)
    )
  }

  /**
   * Get the SparkR package path in various deployment modes.
   * This assumes that Spark properties `spark.master` and `spark.submit.deployMode`
   * and environment variable `SPARK_HOME` are set.
   */
  def sparkRPackagePath(isDriver: Boolean): String = {
    val (master, deployMode) =
      if (isDriver) {
        (sys.props("spark.master"), sys.props("spark.submit.deployMode"))
      } else {
        val sparkConf = SparkEnv.get.conf
        (sparkConf.get("spark.master"), sparkConf.get("spark.submit.deployMode"))
      }

    val isYarnCluster = master != null && master.contains("yarn") && deployMode == "cluster"
    val isYarnClient = master != null && master.contains("yarn") && deployMode == "client"

    // In YARN mode, the SparkR package is distributed as an archive symbolically
    // linked to the "sparkr" file in the current directory. Note that this does not apply
    // to the driver in client mode because it is run outside of the cluster.
    if (isYarnCluster || (isYarnClient && !isDriver)) {
      new File("sparkr").getAbsolutePath
    } else {
      // Otherwise, assume the package is local
      // TODO: support this for Mesos
      localSparkRPackagePath.getOrElse {
        throw new SparkException("SPARK_HOME not set. Can't locate SparkR package.")
      }
    }
  }

  /** Check if R is installed before running tests that use R commands. */
  def isRInstalled: Boolean = {
    val builder = new ProcessBuilder(Seq("R", "--version"))
    builder.start().waitFor() == 0
  }
}
