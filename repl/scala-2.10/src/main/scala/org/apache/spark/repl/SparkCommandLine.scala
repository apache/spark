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

package org.apache.spark.repl

import scala.tools.nsc.{Settings, CompilerCommand}
import scala.Predef._
import org.apache.spark.annotation.DeveloperApi

/**
 * Command class enabling Spark-specific command line options (provided by
 * <i>org.apache.spark.repl.SparkRunnerSettings</i>).
 *
 * @example new SparkCommandLine(Nil).settings
 *
 * @param args The list of command line arguments
 * @param settings The underlying settings to associate with this set of
 *                 command-line options
 */
@DeveloperApi
class SparkCommandLine(args: List[String], override val settings: Settings)
    extends CompilerCommand(args, settings) {
  def this(args: List[String], error: String => Unit) {
    this(args, new SparkRunnerSettings(error))
  }

  def this(args: List[String]) {
    // scalastyle:off println
    this(args, str => Console.println("Error: " + str))
    // scalastyle:on println
  }
}
