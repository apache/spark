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

import scala.collection.immutable.{Map => IMap}
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.util.matching.Regex

import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.{ApplicationAccessType, ContainerId, Priority}

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.launcher.YarnCommandBuilderUtils
import org.apache.spark.resource.ExecutorResourceRequest
import org.apache.spark.util.Utils

object YarnSparkHadoopUtil {

  // Additional memory overhead for application masters in client mode.
  // 10% was arrived at experimentally. In the interest of minimizing memory waste while covering
  // the common cases. Memory overhead tends to grow with container size.
  val AM_MEMORY_OVERHEAD_FACTOR = 0.10

  val ANY_HOST = "*"

  // All RM requests are issued with same priority : we do not (yet) have any distinction between
  // request types (like map/reduce in hadoop for example)
  val RM_REQUEST_PRIORITY = Priority.newInstance(1)

  /**
   * Add a path variable to the given environment map.
   * If the map already contains this key, append the value to the existing value instead.
   */
  def addPathToEnvironment(env: HashMap[String, String], key: String, value: String): Unit = {
    val newValue =
      if (env.contains(key)) {
        env(key) + ApplicationConstants.CLASS_PATH_SEPARATOR  + value
      } else {
        value
      }
    env.put(key, newValue)
  }

  /**
   * Regex pattern to match the name of an environment variable. Note that Unix variable naming
   * conventions (alphanumeric plus underscore, case-sensitive, can't start with a digit)
   * are used for both Unix and Windows, following the convention of Hadoop's `Shell` class
   * (see specifically [[org.apache.hadoop.util.Shell.getEnvironmentVariableRegex]]).
   */
  private val envVarNameRegex: String = "[A-Za-z_][A-Za-z0-9_]*"

  // scalastyle:off line.size.limit
  /**
   * Replace environment variables in a string according to the same rules as
   * [[org.apache.hadoop.yarn.api.ApplicationConstants.Environment]]:
   * `$VAR_NAME` for Unix, `%VAR_NAME%` for Windows, and `{{VAR_NAME}}` for all OS.
   * The `${VAR_NAME}` syntax is also supported for Unix.
   * This support escapes for `$` and `\` (on Unix) and `%` and `^` characters (on Windows), e.g.
   * `\$FOO`, `^%FOO^%`, and `%%FOO%%` will be resolved to `$FOO`, `%FOO%`, and `%FOO%`,
   * respectively, instead of being treated as variable names.
   *
   * @param unresolvedString The unresolved string which may contain variable references.
   * @param env The System environment
   * @param isWindows True iff running in a Windows environment
   * @return The input string with variables replaced with their values from `env`
   * @see [[https://pubs.opengroup.org/onlinepubs/000095399/basedefs/xbd_chap08.html Environment Variables (IEEE Std 1003.1-2017)]]
   * @see [[https://en.wikibooks.org/wiki/Windows_Batch_Scripting#Quoting_and_escaping Windows Batch Scripting | Quoting and Escaping]]
   */
  // scalastyle:on line.size.limit
  def replaceEnvVars(
      unresolvedString: String,
      env: IMap[String, String],
      isWindows: Boolean = Utils.isWindows): String = {
    val osResolvedString = if (isWindows) {
      // ^% or %% can both be used as escapes for Windows
      val windowsPattern = ("""(?i)(?:\^\^|\^%|%%|%(""" + envVarNameRegex + ")%)").r
      windowsPattern.replaceAllIn(unresolvedString, m =>
        Regex.quoteReplacement(m.matched match {
          case "^^" => "^"
          case "^%" => "%"
          case "%%" => "%"
          case _ => env.getOrElse(m.group(1), "")
        })
      )
    } else {
      val unixPattern =
        ("""(?i)(?:\\\\|\\\$|\$(""" + envVarNameRegex + """)|\$\{(""" + envVarNameRegex + ")})").r
      unixPattern.replaceAllIn(unresolvedString, m =>
        Regex.quoteReplacement(m.matched match {
          case """\\""" => """\"""
          case """\$""" => """$"""
          case str if str.startsWith("${") => env.getOrElse(m.group(2), "")
          case _ => env.getOrElse(m.group(1), "")
        })
      )
    }

    // YARN uses `{{...}}` to represent OS-agnostic variable expansion strings. Normally the
    // NodeManager would replace this string with an OS-specific replacement before launching
    // the container. Here, it gets directly treated as an additional expansion string, which
    // has the same net result.
    // Ref: Javadoc for org.apache.hadoop.yarn.api.ApplicationConstants.Environment.$$()
    val yarnPattern = ("""(?i)\{\{(""" + envVarNameRegex + ")}}").r
    yarnPattern.replaceAllIn(osResolvedString,
      m => Regex.quoteReplacement(env.getOrElse(m.group(1), "")))
  }

  /**
   * Kill if OOM is raised - leverage yarn's failure handling to cause rescheduling.
   * Not killing the task leaves various aspects of the executor and (to some extent) the jvm in
   * an inconsistent state.
   * TODO: If the OOM is not recoverable by rescheduling it on different node, then do
   * 'something' to fail job ... akin to unhealthy trackers in mapred ?
   *
   * The handler if an OOM Exception is thrown by the JVM must be configured on Windows
   * differently: the 'taskkill' command should be used, whereas Unix-based systems use 'kill'.
   *
   * As the JVM interprets both %p and %%p as the same, we can use either of them. However,
   * some tests on Windows computers suggest, that the JVM only accepts '%%p'.
   *
   * Furthermore, the behavior of the character '%' on the Windows command line differs from
   * the behavior of '%' in a .cmd file: it gets interpreted as an incomplete environment
   * variable. Windows .cmd files escape a '%' by '%%'. Thus, the correct way of writing
   * '%%p' in an escaped way is '%%%%p'.
   */
  private[yarn] def addOutOfMemoryErrorArgument(javaOpts: ListBuffer[String]): Unit = {
    if (!javaOpts.exists(_.contains("-XX:OnOutOfMemoryError"))) {
      if (Utils.isWindows) {
        javaOpts += escapeForShell("-XX:OnOutOfMemoryError=taskkill /F /PID %%%%p")
      } else {
        javaOpts += "-XX:OnOutOfMemoryError='kill %p'"
      }
    }
  }

  /**
   * Escapes a string for inclusion in a command line executed by Yarn. Yarn executes commands
   * using either
   *
   * (Unix-based) `bash -c "command arg1 arg2"` and that means plain quoting doesn't really work.
   * The argument is enclosed in single quotes and some key characters are escaped.
   *
   * (Windows-based) part of a .cmd file in which case windows escaping for each argument must be
   * applied. Windows is quite lenient, however it is usually Java that causes trouble, needing to
   * distinguish between arguments starting with '-' and class names. If arguments are surrounded
   * by ' java takes the following string as is, hence an argument is mistakenly taken as a class
   * name which happens to start with a '-'. The way to avoid this, is to surround nothing with
   * a ', but instead with a ".
   *
   * @param arg A single argument.
   * @return Argument quoted for execution via Yarn's generated shell script.
   */
  def escapeForShell(arg: String): String = {
    if (arg != null) {
      if (Utils.isWindows) {
        YarnCommandBuilderUtils.quoteForBatchScript(arg)
      } else {
        val escaped = new StringBuilder("'")
        arg.foreach {
          case '$' => escaped.append("\\$")
          case '"' => escaped.append("\\\"")
          case '\'' => escaped.append("'\\''")
          case c => escaped.append(c)
        }
        escaped.append("'").toString()
      }
    } else {
      arg
    }
  }

  // YARN/Hadoop acls are specified as user1,user2 group1,group2
  // Users and groups are separated by a space and hence we need to pass the acls in same format
  def getApplicationAclsForYarn(securityMgr: SecurityManager)
      : Map[ApplicationAccessType, String] = {
    Map[ApplicationAccessType, String] (
      ApplicationAccessType.VIEW_APP -> (securityMgr.getViewAcls + " " +
        securityMgr.getViewAclsGroups),
      ApplicationAccessType.MODIFY_APP -> (securityMgr.getModifyAcls + " " +
        securityMgr.getModifyAclsGroups)
    )
  }

  def getContainerId: ContainerId = {
    val containerIdString = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name())
    ContainerId.fromString(containerIdString)
  }

  /**
   * Get offHeap memory size from [[ExecutorResourceRequest]]
   * return 0 if MEMORY_OFFHEAP_ENABLED is false.
   */
  def executorOffHeapMemorySizeAsMb(sparkConf: SparkConf,
    execRequest: ExecutorResourceRequest): Long = {
    Utils.checkOffHeapEnabled(sparkConf, execRequest.amount)
  }
}
