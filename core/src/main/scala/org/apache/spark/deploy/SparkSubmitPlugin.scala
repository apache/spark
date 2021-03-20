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

package org.apache.spark.deploy

import org.apache.spark.SparkConf

private[spark] trait SparkSubmitPlugin {

  /**
   * Prepare the environment for submitting an application. User should implement this when they
   * want to customize the submit environments. And user should set the config
   * "spark.deploy.sparkSubmitPlugin" to the implementation class.
   *
   * @param args the parsed SparkSubmitArguments used for environment preparation.
   * @param sparkConf the SparkConf instance
   * @param childProcessArgs the child process arguments with default preparing
   * @param childProcessClassPaths the extra classpath of child process with default preparing
   * @param childProcessMainClass the main class of child process with default preparing
   * @return a 4-tuple:
   *        (1) the arguments for the child process,
   *        (2) a list of classpath entries for the child process
   *        (3) the updated SparkConf
   *        (4) the main class for the child
   */
  def prepareSubmitEnvironment(
      args: SparkSubmitArguments,
      sparkConf: SparkConf,
      childProcessArgs: Seq[String],
      childProcessClassPaths: Seq[String],
      childProcessMainClass: String
  ): (Seq[String], Seq[String], SparkConf, String)

}
