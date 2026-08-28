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

package org.apache.spark.sql.execution.python

import java.nio.charset.StandardCharsets

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.api.python.{PythonFunction, SimplePythonFunction}
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

/**
 * The environment variables that Python worker processes launched for a session's Python
 * functions should inherit.
 *
 * The environment is carried by session configurations under a reserved prefix, one configuration
 * per variable: `spark.pythonWorkerEnv.FOO=bar` makes `FOO` visible as `bar` in `os.environ`
 * inside a Python UDF. The configurations are the authoritative session state -- no second copy
 * of the environment is maintained as session state -- so the environment follows the session
 * wherever ordinary session configurations follow it, including into a session created by
 * `cloneSession`.
 *
 * This is shared by every front end that builds Python functions, so that the prefix, the
 * accepted names, the limits and the merge precedence are defined in exactly one place rather
 * than once per front end.
 *
 * Names are preserved case-sensitively by Spark. On a case-sensitive operating system `FOO` and
 * `foo` are therefore distinct variables; Windows process environments are case-insensitive, so
 * what a worker observes there is the platform's business rather than Spark's.
 */
private[sql] object PythonWorkerEnvironment {

  /** Prefix of the session configurations that carry the environment. */
  val confPrefix: String = "spark.pythonWorkerEnv."

  /**
   * Environment variable names accepted under [[confPrefix]].
   *
   * This is deliberately stricter than the operating system requires. A POSIX environment permits
   * any byte except `=` and NUL in a name, and container platforms accept their own broader sets,
   * but a name outside this pattern cannot be referenced portably from a shell, so accepting one
   * would let a session install a variable that some consumers can never read. It is a
   * portability policy, not a description of what a process environment can hold.
   */
  val namePattern: String = "^[A-Za-z_][A-Za-z0-9_]*$"

  private val compiledNamePattern = namePattern.r

  // A rejected name can be arbitrarily long, so messages carry a bounded prefix of it rather than
  // the whole name.
  private val maxNameCharsInMessage = 32

  /**
   * The environment carried by `conf`, without validation.
   *
   * Callers that need one stable snapshot for a whole request read once and pass the result
   * around. Validation is separate so that a caller can tell two environments apart without
   * rejecting an invalid one: an invalid entry has to fail the queries that would install it in a
   * worker, not every query in the session.
   */
  def read(conf: SQLConf): Map[String, String] = extract(conf.getAllConfs)

  /** The environment carried by `conf`, rejected if it is malformed or oversized. */
  def readValidated(conf: SQLConf): Map[String, String] = {
    val variables = read(conf)
    validate(variables)
    variables
  }

  /** The environment carried by the configurations in `allConfs`. */
  private def extract(allConfs: Map[String, String]): Map[String, String] = {
    allConfs.iterator
      .filter { case (key, _) => key.startsWith(confPrefix) }
      .map { case (key, value) => key.substring(confPrefix.length) -> value }
      .toMap
  }

  /**
   * Rejects a malformed or oversized environment.
   *
   * This runs when a Python function is built, which is the one point that every way of writing a
   * configuration reaches: a front end's own configuration API, SQL `SET`, and the
   * application-level configurations merged into a new session all arrive here.
   * [[validateConfigChange]] rejects a write earlier and more helpfully where a front end can
   * intercept one, but it cannot see the other paths, so this is the check that makes an invalid
   * environment unable to reach a worker at all.
   *
   * A message may name a variable but never carries its value, so a rejection cannot copy a value
   * into a log or a stack trace. Note that the name is chosen by the user, so a name is only as
   * safe as what the user put in it.
   *
   * @throws SparkException
   *   if a name is malformed or too long, a value cannot be carried by a process environment, or
   *   the collection exceeds a limit.
   */
  def validate(variables: Map[String, String]): Unit = {
    // Read from the `SparkConf`, not the session configurations: these are cluster-level bounds,
    // and a session must not be able to raise its own.
    val conf = SparkEnv.get.conf
    val maxCount = conf.get(StaticSQLConf.PYTHON_WORKER_ENV_MAX_VARIABLES)
    val maxNameLength = conf.get(StaticSQLConf.PYTHON_WORKER_ENV_MAX_NAME_LENGTH)
    val maxTotalSizeBytes = conf.get(StaticSQLConf.PYTHON_WORKER_ENV_MAX_TOTAL_SIZE_BYTES)

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
            "name" -> describeName(name),
            "prefix" -> confPrefix,
            "pattern" -> namePattern,
            "maxLength" -> maxNameLength.toString),
          cause = null)
      }
      // A process environment cannot carry NUL. Rejecting it here rather than letting the worker
      // launch fail matters for more than the error message: the launch failure is an
      // `IllegalArgumentException` from the JDK whose own message embeds the offending value.
      if (value.indexOf(0) >= 0) {
        throw new SparkException(
          errorClass = "INVALID_SPARK_CONFIG.INVALID_PYTHON_WORKER_ENV_VAR_VALUE",
          messageParameters = Map("name" -> describeName(name), "prefix" -> confPrefix),
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

  /**
   * Rejects a configuration write that would leave the session with an invalid environment.
   *
   * A no-op for a key outside [[confPrefix]]. For a key under it, the environment that the write
   * would produce is validated before the write happens, so an invalid environment never enters
   * the session at all and the failure points at the call that caused it.
   *
   * This is for a front end that can intercept a configuration write, such as the Spark Connect
   * config RPC. It does not cover SQL `SET` or the application-level configurations merged into a
   * new session, which reach the session configurations without passing through any such
   * interception, and that is why [[validate]] at build time stays as the check that no invalid
   * environment can reach a worker.
   *
   * Removing a variable is deliberately not validated. A removal can only shrink the environment,
   * and it is how a session recovers from an environment that one of those unchecked paths left
   * invalid; validating a removal would leave such a session with no way back.
   *
   * @throws SparkException
   *   if the environment the write would produce is malformed or oversized.
   */
  def validateConfigChange(conf: RuntimeConfig, key: String, value: Option[String]): Unit = {
    if (key.startsWith(confPrefix)) {
      // An absent value is rejected by `SQLConf` itself. Leave that failure where it is instead of
      // reporting a missing value as an invalid environment.
      value.foreach { newValue =>
        validate(extract(conf.getAll) + (key.substring(confPrefix.length) -> newValue))
      }
    }
  }

  /**
   * `original` with the session's environment installed in it.
   *
   * @throws SparkException
   *   if `original` is not an implementation this can rewrite.
   */
  def merge(original: PythonFunction, sessionEnv: Map[String, String]): PythonFunction = {
    original match {
      case function: SimplePythonFunction =>
        function.copy(envVars = mergeToJavaMap(function.envVars, sessionEnv))
      case other =>
        // Returning `other` unchanged would drop the environment silently, which is the failure
        // mode this feature has to avoid: a UDF would run without a variable it was told to have.
        throw SparkException.internalError(
          s"Cannot install a Python worker environment in a ${other.getClass.getName}.")
    }
  }

  /**
   * A fresh mutable map holding `originalEnv` with `sessionEnv` applied over it, for a single
   * Python function.
   *
   * The session's environment wins a conflict. A variable in `originalEnv` comes from a broader
   * scope -- for a classic session, from the application-wide `spark.executorEnv.*` -- and the
   * session's own configuration is the more specific statement of intent. This also matches what
   * a worker observes anyway: `PythonWorkerFactory` starts from the executor process environment
   * and applies this map over it.
   *
   * A fresh map is required rather than a shared one: the Python runners add their own entries to
   * the map they are given before launching a worker, so sharing one map across functions would
   * leak those entries between functions, and an immutable map would fail the same assignment.
   */
  def mergeToJavaMap(
      originalEnv: java.util.Map[String, String],
      sessionEnv: Map[String, String]): java.util.HashMap[String, String] = {
    val result = if (originalEnv == null) {
      new java.util.HashMap[String, String](sessionEnv.size)
    } else {
      new java.util.HashMap[String, String](originalEnv)
    }
    sessionEnv.foreach { case (name, value) => result.put(name, value) }
    result
  }

  /**
   * A variable name as it may appear in a message.
   *
   * Truncation alone is not enough. The name comes from a configuration key, so it can hold
   * newlines, tabs and terminal escape sequences that would let a rejected name forge log lines;
   * they are escaped rather than passed through.
   */
  private def describeName(name: String): String = {
    val truncated =
      if (name.length <= maxNameCharsInMessage) name
      else s"${name.take(maxNameCharsInMessage)}..."
    truncated.flatMap {
      case c if c < 0x20 || (c >= 0x7f && c <= 0x9f) =>
        f"\\x${c.toInt}%02x"
      case c => c.toString
    }
  }

  private def utf8Length(s: String): Long = s.getBytes(StandardCharsets.UTF_8).length.toLong
}
