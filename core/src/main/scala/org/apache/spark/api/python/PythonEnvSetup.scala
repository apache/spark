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

package org.apache.spark.api.python

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.SparkFiles
import org.apache.spark.internal.config.{PYSPARK_DRIVER_PYTHON, PYSPARK_PYTHON}


object PythonEnvSetupUtils {

  def getAllSparkNodesReadablePythonEnvRootDir(): Option[String] = {
    val sparkContext = SparkContext.getActive.get
    val sparkConf = sparkContext.conf

    // For databricks runtime,
    // we need to set `allSparkNodesReadableTempDir` to be REPL temp directory
    val allSparkNodesReadableTempDir = sparkConf.getOption("spark.nfs.rootDir")
      .orElse {
        if (sparkContext.isLocal || sparkContext.master.startsWith("local-cluster[")) {
          // For local / local-cluster mode spark
          Some(SparkFiles.getRootDirectory())
        } else None
      }

    allSparkNodesReadableTempDir.map { path =>
      val rootEnvDir = new File(path, "spark-udf-python-env-root")
      rootEnvDir.mkdirs()
      rootEnvDir.getPath
    }
  }


  def setupPythonEnvOnSparkDriverIfAvailable(pipDependencies: Seq[String],
                                             pipConstraints: Seq[String]): Unit = {

    val allSparkNodesReadablePythonEnvRootDirOpt = getAllSparkNodesReadablePythonEnvRootDir()

    if (allSparkNodesReadablePythonEnvRootDirOpt.isDefined) {
      // If we have python-env root directory that is accessible to all spark nodes,
      // we can create python environment with provided dependencies in driver side
      // and spark exectuor can directly use it.
      val sparkContext = SparkContext.getActive.get
      val sparkConf = sparkContext.conf

      sparkContext.setLocalProperty(
        "pythonEnv.allSparkNodesReadableRootEnvDir",
        allSparkNodesReadablePythonEnvRootDirOpt.get
      )

      val pythonExec = sparkConf.get(PYSPARK_DRIVER_PYTHON)
        .orElse(sparkConf.get(PYSPARK_PYTHON))
        .orElse(sys.env.get("PYSPARK_DRIVER_PYTHON"))
        .orElse(sys.env.get("PYSPARK_PYTHON"))
        .getOrElse("python3")

      PythonEnvManager.getOrCreatePythonEnvironment(
        pythonExec,
        allSparkNodesReadablePythonEnvRootDirOpt.get,
        pipDependencies,
        pipConstraints
      )
    }
  }

  def getPipRequirementsFromChainedPythonFuncs(
                                                funcs: Seq[ChainedPythonFunctions]
                                              ): (Seq[String], Seq[String]) = {
    val headPipDeps = funcs.head.funcs.head.pipDependencies
    val headPipConstraints = funcs.head.funcs.head.pipConstraints

    for (chainedFunc <- funcs) {
      for (simpleFunc <- chainedFunc.funcs) {
        if (simpleFunc.pipDependencies != headPipDeps ||
          simpleFunc.pipConstraints != headPipConstraints
        ) {
          // TODO: For this case, we should split current python runner
          //  into multiple python runners.
          throw new RuntimeException(
            "We cannot support the case that a python runner contains functions with " +
              "different python dependency requirements."
          )
        }
      }
    }
    (headPipDeps, headPipConstraints)
  }

}
