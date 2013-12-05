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

package org.apache.spark.deploy.yarn

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.conf.Configuration

/**
 * Contains util methods to interact with Hadoop from spark.
 */
class YarnSparkHadoopUtil extends SparkHadoopUtil {

  // Note that all params which start with SPARK are propagated all the way through, so if in yarn mode, this MUST be set to true.
  override def isYarnMode(): Boolean = { true }

  // Return an appropriate (subclass) of Configuration. Creating config can initializes some hadoop subsystems
  // Always create a new config, dont reuse yarnConf.
  override def newConfiguration(): Configuration = new YarnConfiguration(new Configuration())

  // add any user credentials to the job conf which are necessary for running on a secure Hadoop cluster
  override def addCredentials(conf: JobConf) {
    val jobCreds = conf.getCredentials()
    jobCreds.mergeAll(UserGroupInformation.getCurrentUser().getCredentials())
  }
}
