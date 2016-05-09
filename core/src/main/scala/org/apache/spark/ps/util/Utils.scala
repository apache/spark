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

package org.apache.spark.ps.util

import java.io.{IOException, File}

import org.apache.spark.util.Utils._
import org.apache.spark.{SparkConf, Logging}

/**
 * some common interface
 */
private[ps] object Utils extends Logging {

  /** Get parameter server local directories **/
  def getOrCreatePSLocalDirs(conf: SparkConf):Array[String] = {
    conf.get("SPARK_PS_DIRS", conf.getOption("spark.ps.dir")
      .getOrElse(System.getProperty("java.io.tmpdir")))
      .split(",")
      .flatMap { dir =>
      try {
        val file = new File(dir)
        if (file.exists || file.mkdirs()) {
          val tmpDir: File = createDirectory(dir)
          chmod700(tmpDir)
          Some(tmpDir.getAbsolutePath)
        } else {
          logError(s"Failed to create ps dir in $dir. Ignoring this directory.")
          None
        }
      } catch {
        case e: IOException =>
          logError(s"Failed to create ps root dir in $dir. Ignoring this directory.")
          None
      }
    }
  }

}
