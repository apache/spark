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

package org.apache.spark.kafka010

import scala.util.matching.Regex

import org.apache.kafka.common.config.SaslConfigs

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SECRET_REDACTION_PATTERN
import org.apache.spark.util.Utils.{redact, REDACTION_REPLACEMENT_TEXT}

object KafkaRedactionUtil extends Logging {
  def redactParams(params: Seq[(String, Object)]): Seq[(String, String)] = {
    val redactionPattern = Some(Option(SparkEnv.get).map(_.conf)
      .getOrElse(new SparkConf()).get(SECRET_REDACTION_PATTERN))
    params.map { case (key, value) =>
      if (value != null) {
        if (key.equalsIgnoreCase(SaslConfigs.SASL_JAAS_CONFIG)) {
          (key, redactJaasParam(value.asInstanceOf[String]))
        } else {
          val (_, newValue) = redact(redactionPattern, Seq((key, value.toString))).head
          (key, newValue)
        }
      } else {
        (key, value.asInstanceOf[String])
      }
    }
  }

  // Matches a single `key=value` option inside a JAAS configuration entry. The value may be
  // double-quoted, single-quoted, or unquoted (up to the next whitespace or ';'). The quoted
  // alternatives are escape-aware -- an escaped quote (`\"`) does not terminate the value -- so the
  // whole value is captured rather than being cut short at an embedded quote. Group 1 is the option
  // name, group 2 the value.
  private val jaasOptionPattern =
    """([\w.$-]+)\s*=\s*("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|[^\s;]+)""".r

  // The JAAS credential option names that are always redacted, independent of any user
  // configuration: the SASL `password` and the OAUTHBEARER `clientSecret`. Matched
  // case-insensitively as a substring of the option name. Keeping this built-in guarantees the
  // credentials are masked even when `spark.redaction.regex` is set to a pattern that does not
  // cover them (that config replaces the default rather than extending it, so relying on it alone
  // would be fail-open) -- mirroring the unconditional password redaction this code used to do.
  private val alwaysRedactedOptions = "(?i)password|clientSecret".r

  // Redacts the value of a JAAS credential option, regardless of quoting style (double-quoted,
  // single-quoted, or unquoted), while leaving non-secret options (the login-module class, control
  // flag, `username`, `serviceName`, `clientId`, ...) intact so the entry stays readable in logs.
  // An option is redacted if its name matches the always-redacted set above OR the configured
  // `spark.redaction.regex`, so a user-set pattern can widen coverage but never disable the
  // built-in credential masking. Note: the default `spark.redaction.regex` includes `token`, so a
  // non-secret `tokenauth` option is redacted too -- a minor debuggability cost accepted as safe.
  def redactJaasParam(param: String): String = {
    if (param != null && !param.isEmpty) {
      val redactionPattern = Option(SparkEnv.get).map(_.conf)
        .getOrElse(new SparkConf()).get(SECRET_REDACTION_PATTERN)
      jaasOptionPattern.replaceAllIn(param, m => {
        val name = m.group(1)
        val isSecret = alwaysRedactedOptions.findFirstMatchIn(name).isDefined ||
          redactionPattern.findFirstMatchIn(name).isDefined
        val replacement = if (isSecret) {
          s"""$name="$REDACTION_REPLACEMENT_TEXT""""
        } else {
          m.matched
        }
        Regex.quoteReplacement(replacement)
      })
    } else {
      param
    }
  }
}
