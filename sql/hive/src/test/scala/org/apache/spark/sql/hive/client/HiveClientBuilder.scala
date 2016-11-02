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

package org.apache.spark.sql.hive.client

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.VersionInfo

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

private[client] class HiveClientBuilder {
  private val sparkConf = new SparkConf()

  // In order to speed up test execution during development or in Jenkins, you can specify the path
  // of an existing Ivy cache:
  private val ivyPath: Option[String] = {
    sys.env.get("SPARK_VERSIONS_SUITE_IVY_PATH").orElse(
      Some(new File(sys.props("java.io.tmpdir"), "hive-ivy-cache").getAbsolutePath))
  }

  private def buildConf() = {
    lazy val warehousePath = Utils.createTempDir()
    lazy val metastorePath = Utils.createTempDir()
    metastorePath.delete()
    Map(
      "javax.jdo.option.ConnectionURL" -> s"jdbc:derby:;databaseName=$metastorePath;create=true",
      "hive.metastore.warehouse.dir" -> warehousePath.toString)
  }

  def buildClient(version: String, hadoopConf: Configuration): HiveClient = {
    IsolatedClientLoader.forVersion(
      hiveMetastoreVersion = version,
      hadoopVersion = VersionInfo.getVersion,
      sparkConf = sparkConf,
      hadoopConf = hadoopConf,
      config = buildConf(),
      ivyPath = ivyPath).createClient()
  }
}
