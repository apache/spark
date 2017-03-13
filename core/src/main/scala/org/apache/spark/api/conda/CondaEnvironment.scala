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
package org.apache.spark.api.conda

import java.nio.file.Path
import java.nio.file.Paths
import java.util.{Map => JMap}

import scala.sys.process.Process

import org.apache.spark.internal.Logging

final case class CondaEnvironment(condaEnvDir: Path) extends Logging {
  def this(condaEnvDir: String) = this(Paths.get(condaEnvDir))

  def envRoot: Path = condaEnvDir.getParent

  def envName: String = condaEnvDir.getFileName.toString

  def activatedEnvironment(startEnv: Map[String, String] = Map.empty): Map[String, String] = {
    val newStartEnv = (startEnv + ("CONDA_PREFIX" -> condaEnvDir.toString)).toSeq
    // Activate the conda environment, then capture the environment
    val env0sep = Process(List("bash", "-c",
      s"source $$CONDA_PREFIX/bin/activate $$CONDA_PREFIX && env -0"),
      None, newStartEnv: _*).!!
    env0sep.split('\u0000').iterator.filter(_.trim.nonEmpty).map(_.split("=", 2) match {
      case Array(k, v) => (k, v)
      case arr => sys.error(s"Unparseable env var: ${arr.toSeq}")
    }).toMap
  }

  /**
   * Clears the given java environment and replaces all variables with the environment
   * produced after calling `activate` inside this conda environment.
   */
  def initializeJavaEnvironment(env: JMap[String, String]): Unit = {
    env.clear()
    val activatedEnv = activatedEnvironment()
    activatedEnv.foreach { case (k, v) => env.put(k, v) }
    logDebug(s"Initialised environment from conda: $activatedEnv")
  }
}

object CondaEnvironment {
  private[spark] val CONDA_ENVIRONMENT_PREFIX = "spark.conda.environmentPrefix"

  /** Returns the environment if one was already set up in this JVM */
  def fromSystemProperty(): Option[CondaEnvironment] = {
    sys.props.get(CONDA_ENVIRONMENT_PREFIX).map(new CondaEnvironment(_))
  }
}
