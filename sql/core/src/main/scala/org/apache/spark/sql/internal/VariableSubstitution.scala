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

package org.apache.spark.sql.internal

import java.util.regex.Pattern

import org.apache.spark.sql.AnalysisException

/**
 * A helper class that enables substitution using syntax like
 * `${var}`, `${system:var}` and `${env:var}`.
 *
 * Variable substitution is controlled by [[SQLConf.variableSubstituteEnabled]].
 */
class VariableSubstitution(conf: SQLConf) {

  private val pattern = Pattern.compile("\\$\\{[^\\}\\$ ]+\\}")

  /**
   * Given a query, does variable substitution and return the result.
   */
  def substitute(input: String): String = {
    // Note that this function is mostly copied from Hive's SystemVariables, so the style is
    // very Java/Hive like.
    if (input eq null) {
      return null
    }

    if (!conf.variableSubstituteEnabled) {
      return input
    }

    var eval = input
    val depth = conf.variableSubstituteDepth
    val builder = new StringBuilder
    val m = pattern.matcher("")

    var s = 0
    while (s <= depth) {
      m.reset(eval)
      builder.setLength(0)

      var prev = 0
      var found = false
      while (m.find(prev)) {
        val group = m.group()
        var substitute = substituteVariable(group.substring(2, group.length - 1))
        if (substitute.isEmpty) {
          substitute = group
        } else {
          found = true
        }
        builder.append(eval.substring(prev, m.start())).append(substitute)
        prev = m.end()
      }

      if (!found) {
        return eval
      }

      builder.append(eval.substring(prev))
      eval = builder.toString
      s += 1
    }

    if (s > depth) {
      throw new AnalysisException(
        "Variable substitution depth is deeper than " + depth + " for input " + input)
    } else {
      return eval
    }
  }

  /**
   * Given a variable, replaces with the substitute value (default to "").
   */
  private def substituteVariable(variable: String): String = {
    var value: String = null

    if (variable.startsWith("system:")) {
      value = System.getProperty(variable.substring("system:".length()))
    }

    if (value == null && variable.startsWith("env:")) {
      value = System.getenv(variable.substring("env:".length()))
    }

    if (value == null && conf != null && variable.startsWith("hiveconf:")) {
      value = conf.getConfString(variable.substring("hiveconf:".length()), "")
    }

    if (value == null && conf != null && variable.startsWith("sparkconf:")) {
      value = conf.getConfString(variable.substring("sparkconf:".length()), "")
    }

    if (value == null && conf != null && variable.startsWith("spark:")) {
      value = conf.getConfString(variable.substring("spark:".length()), "")
    }

    if (value == null && conf != null) {
      value = conf.getConfString(variable, "")
    }

    value
  }
}
