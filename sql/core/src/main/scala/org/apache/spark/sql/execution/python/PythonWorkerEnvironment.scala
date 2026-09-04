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
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

/**
 * The environment variables that Python worker processes launched for a session's Python
 * functions should inherit.
 *
 * The environment is carried by session configurations under a reserved prefix, one configuration
 * per variable: `spark.pythonWorkerEnv.FOO=bar` makes `FOO` visible as `bar` in `os.environ`
 * inside a Python UDF. Those configurations are the only copy of this state, so the environment
 * follows the session wherever ordinary session configurations do, `cloneSession` included.
 *
 * It is installed when a worker is launched rather than when a function is built, so a worker
 * receives the session's values as of the query that launched it and a cached plan cannot pin an
 * older set.
 *
 * Names are case-sensitive, so `FOO` and `foo` are distinct variables. Windows process
 * environments are case-insensitive, so what a worker observes there is the platform's business.
 */
private[sql] object PythonWorkerEnvironment {

  /** Prefix of the session configurations that carry the environment. */
  val confPrefix: String = "spark.pythonWorkerEnv."

  /**
   * Environment variable names accepted under [[confPrefix]]. Stricter than POSIX requires: a name
   * outside this pattern cannot be referenced portably from a shell, so accepting one would let a
   * session install a variable some consumers could never read.
   */
  val namePattern: String = "^[A-Za-z_][A-Za-z0-9_]*$"

  private val compiledNamePattern = namePattern.r

  /**
   * Names Spark uses for the variables it sets in a Python worker itself, which a session may not
   * set.
   *
   * Write order is the first line of defence: the runners and `PythonWorkerFactory` apply their own
   * variables after the session's, so anything they set unconditionally already wins --
   * `PYTHONUNBUFFERED` and `PYTHON_UDF_BATCH_SIZE` among them, which a session may still set
   * harmlessly. `PYTHONPATH` is a third case rather than a fourth reserved name:
   * `PythonWorkerFactory` folds the session's value into the path it computes instead of discarding
   * it, so a session adds to the worker's import path. It is left settable because a session
   * already chooses the code its own worker runs, not because Spark's entries are guaranteed to
   * come first -- `PythonUtils.sparkPythonPath` is empty when `SPARK_HOME` is unset and Spark's
   * classes did not come from a jar, and `mergePythonPaths` drops empty entries, which can leave
   * the session's path first.
   *
   * These are the ones write order does not protect, because Spark sets them only when a condition
   * holds and a session's value survives when it does not. Two of them would desynchronize the JVM
   * and the worker rather than merely change a setting: the worker reads `SPARK_PIPELINED_UDF` to
   * choose its wire protocol and `PYTHON_UNIX_DOMAIN_ENABLED` to choose its transport. Rejecting
   * the name, rather than dropping it silently, means the failure says so.
   *
   * The `SPARK_`, `PYSPARK_` and `PYTHON_WORKER_FACTORY_` prefixes are reserved wholesale because
   * every variable Spark sets under them is its own. The last is a prefix rather than a list
   * because `PythonWorkerFactory` sets one name per socket branch and enumerating them missed
   * `PYTHON_WORKER_FACTORY_SOCK_DIR`, which only the daemon path sets. The `PYTHON` prefix
   * deliberately is not reserved, so a session keeps names such as `PYTHONWARNINGS`. The cost is
   * that a conditional variable added under that prefix later has to be added here too.
   */
  val reservedNamePrefixes: Seq[String] = Seq("SPARK_", "PYSPARK_", "PYTHON_WORKER_FACTORY_")

  /** Reserved names outside the reserved prefixes. */
  val reservedNames: Set[String] = Set(
    "OMP_NUM_THREADS",
    "PYTHON_DAEMON_KILL_WORKER_ON_FLUSH_FAILURE",
    "PYTHON_FAULTHANDLER_DIR",
    "PYTHON_TRACEBACK_DUMP_INTERVAL_SECONDS",
    "PYTHON_UNIX_DOMAIN_ENABLED")

  private def isReserved(name: String): Boolean =
    reservedNames.contains(name) || reservedNamePrefixes.exists(name.startsWith)

  // A rejected name can be arbitrarily long, so messages carry a bounded prefix of it rather than
  // the whole name.
  private val maxNameCharsInMessage = 32

  /**
   * The environment carried by `conf`, without validation. Kept separate from [[readValidated]] so
   * that reading stays usable on an invalid environment: only the queries that would install it in
   * a worker may fail, not every query in the session.
   */
  def read(conf: SQLConf): Map[String, String] = extract(conf.getAllConfs)

  /** The environment carried by `conf`, rejected if it is malformed or oversized. */
  private[python] def readValidated(conf: SQLConf): Map[String, String] = {
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
   * This runs when a worker is launched, which every way of writing a configuration reaches, so it
   * is what makes an invalid environment unable to reach a worker at all. It therefore surfaces as
   * a task failure; [[validateConfigChange]] reports the same problem earlier where a write can be
   * intercepted.
   *
   * A message may name a variable but never carries its value, so a rejection cannot copy a secret
   * into a log or a stack trace.
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
      if (isReserved(name)) {
        throw new SparkException(
          errorClass = "INVALID_SPARK_CONFIG.RESERVED_PYTHON_WORKER_ENV_VAR_NAME",
          messageParameters = Map(
            "name" -> describeName(name),
            "prefix" -> confPrefix,
            "reservedPrefixes" -> reservedNamePrefixes.mkString(", "),
            "reservedNames" -> reservedNames.toSeq.sorted.mkString(", ")),
          cause = null)
      }
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
   * Called from `RuntimeConfig.set`, which is the write path every user-facing way of setting a
   * configuration reaches: a classic `spark.conf.set` goes straight there, the Spark Connect config
   * RPC writes through a classic `RuntimeConfig` on the server, and SQL `SET` reaches it through
   * `SetCommand`. So one call site covers both front ends and all three surfaces.
   *
   * What still bypasses it is a write straight to `SQLConf` -- `SparkSession.builder`'s
   * configurations are merged into a session that way, and internal code can call `setConfString`
   * directly -- which is why [[validate]] at worker launch remains the authoritative check.
   *
   * Removing a variable is deliberately not validated: a removal can only shrink the environment,
   * and it is how a session recovers from one of those unchecked paths leaving it invalid.
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
   * Whether a Python function with this evaluation type receives the session's environment.
   *
   * Scoped to the regular scalar Python UDF, which reaches a worker under three evaluation types
   * and is one thing to the user in all of them. All three are supported rather than all three
   * being active at once: `spark.sql.execution.pythonUDF.arrow.enabled` selects one of the batched
   * pair, which are alternatives, and the element-wise type appears only after
   * `ExtractPythonUDFFromLambda` lifts a UDF out of the lambda of a higher-order function such as
   * `transform`. Each is reachable without changing a default, so omitting any one would leave the
   * feature inert for some ordinary UDF.
   *
   * The pandas and Arrow families have their own element-wise types and stay out of scope, as do
   * UDTFs and the streaming paths, until those runners are tested.
   */
  def appliesTo(evalType: Int): Boolean = evalType match {
    case PythonEvalType.SQL_BATCHED_UDF | PythonEvalType.SQL_ARROW_BATCHED_UDF |
        PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF =>
      true
    case _ => false
  }

  /**
   * `originalEnv` with the session's validated environment applied over it, in a fresh mutable map
   * ready to hand to a Python worker. This is what an execution path calls.
   */
  def mergeValidated(
      originalEnv: java.util.Map[String, String],
      conf: SQLConf): java.util.HashMap[String, String] = {
    mergeToJavaMap(originalEnv, readValidated(conf))
  }

  /**
   * A fresh mutable map holding `originalEnv` with `sessionEnv` applied over it.
   *
   * The session's environment wins a conflict: `originalEnv` comes from a broader scope -- on
   * classic, the application-wide `spark.executorEnv.*` -- and the session's own configuration is
   * the more specific statement of intent.
   *
   * The map must be fresh and mutable: a runner adds its own entries before launching a worker, so
   * a shared map would leak them between workers and an immutable one would not accept them.
   */
  private[python] def mergeToJavaMap(
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
   * A variable name as it may appear in a message. Truncated, and control characters are escaped
   * rather than passed through: the name comes from a configuration key, so it could otherwise
   * forge log lines.
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
