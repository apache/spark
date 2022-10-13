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

package org.apache.spark.sql.util

import org.apache.spark.sql.internal.SQLConf

object SqlConfigKeyUtils {
  def ansiModeConfigKey: String = getSqlConfigKey(SQLConf.ANSI_ENABLED.key)
  def legacyTimeParserPolicyConfigName: String =
    getSqlConfigKey(SQLConf.LEGACY_TIME_PARSER_POLICY.key)

  /**
   * Used to find a SQL configuration.
   *
   * @param sparkConfigKey Spark configuration key
   * @return Returns the SQL configuration key.
   */
  def getSqlConfigKey(sparkConfigKey: String): String = {
    sparkConfigKey
  }
}
