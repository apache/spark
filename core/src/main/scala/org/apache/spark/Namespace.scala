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

package org.apache.spark

/**
 * A simple trait that manages namespaces in Spark configuration properties and env variables.
 */
sealed private[spark] trait Namespace {
  /**
   * Namespace name for env variables
   */
  def envNamespace: String

  /**
   * Namespace name for conf property
   */
  def confNamespace: String

  /**
   * Adds this namespace to the given env variable name. Assuming the given `envKey` is
   * of the form `SPARK_XXX`, this method will return `SPARK_envNamespace_XXX`.
   */
  def envName(envKey: String): String = {
    if (envKey != null && envKey.startsWith("SPARK_") && !isBlank) {
      s"SPARK_${envNamespace }_" + envKey.substring("SPARK_".length)
    } else {
      envKey
    }
  }

  /**
   * Adds this namespace to the given configuration property name. Assuming the given
   * `confKey` is of the form `spark.xxx`, this method will return `spark.confNamespace.xxx`.
   */
  def confName(confKey: String): String = {
    if (confKey != null && confKey.startsWith("spark.") && !isBlank) {
      s"spark.$confNamespace." + confKey.substring("spark.".length)
    } else {
      confKey
    }
  }

  /**
   * Whether this is an empty namespace or not.
   */
  def isBlank: Boolean = this == Namespace.Blank

  /**
   * Returns a copy of this configuration with all the properties at `spark.srcNamespace.xxx`
   * copied to `spark.xxx`
   */
  def removeFrom(conf: SparkConf): SparkConf = {
    val updates = if (!isBlank) {
      val srcNS = s"spark.$confNamespace."
      val destNS = "spark."
      conf.getAll.collect {
        case (key, value) if key.startsWith(srcNS) =>
          (destNS + key.substring(srcNS.length), value)
      }
    } else {
      Array.empty[(String, String)]
    }
    conf.clone.setAll(updates)
  }

}

private[spark] object Namespace {

  private case class NamespaceImpl(envNamespace: String, confNamespace: String) extends Namespace

  /**
   * Submission namespace is used for network and security options related to submission
   * of applications to a standalone Spark cluster.
   */
  val Submission: Namespace = NamespaceImpl("SUBMISSION", "submission")

  /**
   * Application namespace is used for network and security options for internal communication
   * inside the application.
   */
  val Application: Namespace = NamespaceImpl("APP", "application")

  /**
   * Blank namespace makes invocations of all the methods perform ID transformation,
   * so that no changes are made.
   */
  val Blank: Namespace = NamespaceImpl("", "")
}
