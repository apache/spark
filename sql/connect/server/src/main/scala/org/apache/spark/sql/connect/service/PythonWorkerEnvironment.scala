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

package org.apache.spark.sql.connect.service

import java.nio.charset.StandardCharsets

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.internal.SQLConf

/**
 * The environment variables that Python worker processes launched for a session's Python functions
 * should inherit.
 *
 * The environment is carried by session configurations under a reserved prefix, one configuration
 * per variable: `spark.pythonWorkerEnv.FOO=bar` makes `FOO` visible as `bar` in `os.environ`
 * inside a Python UDF. The configurations are the authoritative session state; nothing is cached
 * outside them, so the environment follows the session wherever ordinary session configurations
 * follow it, including into a session created by `cloneSession`.
 *
 * Names are case-sensitive, matching POSIX.
 */
private[connect] object PythonWorkerEnvironment {

  /** Prefix of the session configurations that carry the environment. */
  val confPrefix: String = "spark.pythonWorkerEnv."

  /** Environment variable names accepted under [[confPrefix]]. */
  val namePattern: String = "^[A-Za-z_][A-Za-z0-9_]*$"

  private val compiledNamePattern = namePattern.r

  // A rejected name can be arbitrarily long, so messages carry a bounded prefix of it rather than
  // the whole name.
  private val maxNameCharsInMessage = 32

  /**
   * The environment carried by `conf`, without validation.
   *
   * Used where only a change in the environment matters rather than its validity, so that an
   * invalid entry fails the queries that would install it rather than every query in the session.
   */
  def read(conf: SQLConf): Map[String, String] = {
    conf.getAllConfs.iterator
      // A null value is indistinguishable from an unset configuration once installed, so it is
      // read as absent rather than as a variable with an empty value. An empty string is kept: it
      // is a valid value, matching `FOO=` in a POSIX shell.
      .filter { case (key, value) => key.startsWith(confPrefix) && value != null }
      .map { case (key, value) => key.substring(confPrefix.length) -> value }
      .toMap
  }

  /**
   * The environment carried by `conf`, rejecting a malformed or oversized one.
   *
   * Validation happens here, on read, rather than where the configuration is set: an ordinary
   * configuration write has no interception point on this path, so there is nowhere earlier to
   * refuse it. A rejection therefore fails the query that would have installed the environment,
   * which keeps the failure loud instead of silently running a Python function without the
   * variables it expects.
   *
   * A message may name a variable but never carries its value, and a long name is truncated rather
   * than echoed whole, so a rejection cannot copy a credential into a log or a stack trace.
   */
  def readValidated(conf: SQLConf): Map[String, String] = {
    val variables = read(conf)
    if (variables.nonEmpty) {
      validate(variables)
    }
    variables
  }

  /**
   * A fresh mutable copy of `variables` for a single Python function.
   *
   * A copy is required rather than a shared map: the Python runners add their own entries to the
   * map they are given before launching a worker, so sharing one map across functions would leak
   * those entries between functions, and an immutable map would fail the same assignment.
   */
  def toMutableJavaMap(variables: Map[String, String]): java.util.HashMap[String, String] = {
    val result = new java.util.HashMap[String, String](variables.size)
    variables.foreach { case (name, value) => result.put(name, value) }
    result
  }

  private def validate(variables: Map[String, String]): Unit = {
    val conf = SparkEnv.get.conf
    val maxCount = conf.get(Connect.CONNECT_PYTHON_WORKER_ENV_MAX_VARIABLES)
    val maxNameLength = conf.get(Connect.CONNECT_PYTHON_WORKER_ENV_MAX_NAME_LENGTH)
    val maxTotalSizeBytes = conf.get(Connect.CONNECT_PYTHON_WORKER_ENV_MAX_TOTAL_SIZE_BYTES)

    if (variables.size > maxCount) {
      throw new SparkException(
        errorClass = "INVALID_SPARK_CONFIG.PYTHON_WORKER_ENV_TOO_MANY_VARIABLES",
        messageParameters = Map(
          "count" -> variables.size.toString,
          "prefix" -> confPrefix,
          "maxCount" -> maxCount.toString),
        cause = null)
    }

    var totalSizeBytes = 0L
    variables.foreach { case (name, value) =>
      // `matches` requires the whole name to match. Searching for the pattern instead would accept
      // a name with a trailing newline, because `$` also matches before a terminating line break.
      if (name.length > maxNameLength || !compiledNamePattern.matches(name)) {
        throw new SparkException(
          errorClass = "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_NAME",
          messageParameters = Map(
            "name" -> abbreviate(name),
            "prefix" -> confPrefix,
            "pattern" -> namePattern,
            "maxLength" -> maxNameLength.toString),
          cause = null)
      }
      totalSizeBytes += utf8Length(name) + utf8Length(value)
    }

    if (totalSizeBytes > maxTotalSizeBytes) {
      throw new SparkException(
        errorClass = "INVALID_SPARK_CONFIG.PYTHON_WORKER_ENV_TOO_LARGE",
        messageParameters = Map(
          "prefix" -> confPrefix,
          "size" -> totalSizeBytes.toString,
          "maxSize" -> maxTotalSizeBytes.toString),
        cause = null)
    }
  }

  private def abbreviate(name: String): String = {
    if (name.length <= maxNameCharsInMessage) name
    else s"${name.take(maxNameCharsInMessage)}..."
  }

  private def utf8Length(s: String): Long = s.getBytes(StandardCharsets.UTF_8).length.toLong
}
