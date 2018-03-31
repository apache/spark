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
package org.apache.spark.deploy.k8s.submit.steps

import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.KubernetesUtils
import org.apache.spark.deploy.k8s.submit.KubernetesDriverSpec

/**
 * Step that configures the classpath, spark.jars, and spark.files for the driver given that the
 * user may provide remote files or files with local:// schemes.
 */
private[spark] class DependencyResolutionStep(
    sparkJars: Seq[String],
    sparkFiles: Seq[String]) extends DriverConfigurationStep {

  override def configureDriver(driverSpec: KubernetesDriverSpec): KubernetesDriverSpec = {
    val sparkConf = driverSpec.driverSparkConf.clone()
    val resolvedSparkFiles = KubernetesUtils.resolveFileUrisAndPath(
      sparkFiles, sparkConf.get(FILES_DOWNLOAD_LOCATION))

    if (resolvedSparkFiles.nonEmpty) {
      sparkConf.set("spark.files", resolvedSparkFiles.mkString(","))
    }

    if (sparkJars.nonEmpty) {
      sparkConf.set("spark.jars",
        sparkJars.map(jar => KubernetesUtils.resolveLocalFile(jar)).mkString(","))
    }

    driverSpec.copy(driverSparkConf = sparkConf)
  }
}
