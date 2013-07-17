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

package spark.deploy

import collection.mutable.HashMap
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import java.security.PrivilegedExceptionAction

/**
 * Contains util methods to interact with Hadoop from spark.
 */
object SparkHadoopUtil {

  val yarnConf = newConfiguration()

  def getUserNameFromEnvironment(): String = {
    // defaulting to env if -D is not present ...
    val retval = System.getProperty(Environment.USER.name, System.getenv(Environment.USER.name))

    // If nothing found, default to user we are running as
    if (retval == null) System.getProperty("user.name") else retval
  }

  def runAsUser(func: (Product) => Unit, args: Product) {
    runAsUser(func, args, getUserNameFromEnvironment())
  }

  def runAsUser(func: (Product) => Unit, args: Product, user: String) {
    func(args)
  }

  // Note that all params which start with SPARK are propagated all the way through, so if in yarn mode, this MUST be set to true.
  def isYarnMode(): Boolean = {
    val yarnMode = System.getProperty("SPARK_YARN_MODE", System.getenv("SPARK_YARN_MODE"))
    java.lang.Boolean.valueOf(yarnMode)
  }

  // Set an env variable indicating we are running in YARN mode.
  // Note that anything with SPARK prefix gets propagated to all (remote) processes
  def setYarnMode() {
    System.setProperty("SPARK_YARN_MODE", "true")
  }

  def setYarnMode(env: HashMap[String, String]) {
    env("SPARK_YARN_MODE") = "true"
  }

  // Return an appropriate (subclass) of Configuration. Creating config can initializes some hadoop subsystems
  // Always create a new config, dont reuse yarnConf.
  def newConfiguration(): Configuration = new YarnConfiguration(new Configuration())

  // add any user credentials to the job conf which are necessary for running on a secure Hadoop cluster
  def addCredentials(conf: JobConf) {
    val jobCreds = conf.getCredentials();
    jobCreds.mergeAll(UserGroupInformation.getCurrentUser().getCredentials())
  }
}
