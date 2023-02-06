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

package org.apache.spark.internal.io.cloud

import java.io.File

import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory

import org.apache.spark.sql.sources.PartitionedWriteSuite
import org.apache.spark.SparkConf
import org.apache.spark.internal.io.cloud.CommitterBindingSuite.COMMITTER_OPTIONS
import org.apache.spark.internal.io.cloud.PathOutputCommitProtocol.{MANIFEST_COMMITTER_FACTORY, REPORT_DIR}

/**
 * Extend the sql core PartitionedWriteSuite with a binding to
 * the FileOutputCommitter through the PathOutputCommitter.
 * This verifies consistency of job commit with the original Hadoop
 * binding.
 */
class PathOutputPartitionedWriteSuite extends PartitionedWriteSuite {

  private val OPT_PREFIX = "spark.hadoop.mapreduce.manifest.committer."

  /**
   * Directory for saving job summary reports.
   * These are the _SUCCESS files, but are saved even on
   * job failures.
   */
  private val OPT_SUMMARY_REPORT_DIR: String = OPT_PREFIX +
    "summary.report.directory"

  /**
   * Create a job configuration using the PathOutputCommitterProtocol
   * through Parquet.
   * If the test is running on a Hadoop release with the ManifestCommitter
   * available, it switches to this committer, which works on file:// repositories
   * scales better on azure and google cloud stores, and
   * collects and reports IOStatistics from task commit IO as well
   * as Job commit Operations.
   *
   * @return the spark configuration to use.
   */
  override protected def sparkConf: SparkConf = {

    val reportsDir = "./target/reports/"
    val conf = super.sparkConf
      .setAll(COMMITTER_OPTIONS)
      .set(REPORT_DIR,
        new File(reportsDir + "iostats").getCanonicalFile.toURI.toString)

    // look for the manifest committer exactly once.
    val loader = getClass.getClassLoader
    try {
      loader.loadClass(MANIFEST_COMMITTER_FACTORY)
      // manifest committer class was found so bind to and configure it.
      logInfo("Using Manifest Committer")
      conf.set(PathOutputCommitterFactory.COMMITTER_FACTORY_CLASS,
        MANIFEST_COMMITTER_FACTORY)
      // save full _SUCCESS files for the curious; this includes timings
      // of operations in task as well as job commit.
      conf.set(OPT_SUMMARY_REPORT_DIR,
        new File(reportsDir + "summary").getCanonicalFile.toURI.toString)
    } catch {
      case e: ClassNotFoundException =>
        val mapredJarUrl = loader.getResource(
          "org/apache/hadoop/mapreduce/lib/output/PathOutputCommitterFactory.class")
        logInfo(s"Manifest Committer not found in JAR $mapredJarUrl")
    }
    conf
  }
}
