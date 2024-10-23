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

import java.util.concurrent.atomic.AtomicReference

/**
 * SqlApiConfHelper is created to avoid a deadlock during a concurrent access to SQLConf and
 * SqlApiConf, which is because SQLConf and SqlApiConf tries to load each other upon
 * initializations. SqlApiConfHelper is private to sql package and is not supposed to be
 * accessed by end users. Variables and methods within SqlApiConfHelper are defined to
 * be used by SQLConf and SqlApiConf only.
 */
private[sql] object SqlApiConfHelper {
  // Shared keys.
  val ANSI_ENABLED_KEY: String = "spark.sql.ansi.enabled"
  val LEGACY_TIME_PARSER_POLICY_KEY: String = "spark.sql.legacy.timeParserPolicy"
  val CASE_SENSITIVE_KEY: String = "spark.sql.caseSensitive"
  val SESSION_LOCAL_TIMEZONE_KEY: String = "spark.sql.session.timeZone"
  val LOCAL_RELATION_CACHE_THRESHOLD_KEY: String = "spark.sql.session.localRelationCacheThreshold"

  val confGetter: AtomicReference[() => SqlApiConf] = {
    new AtomicReference[() => SqlApiConf](() => DefaultSqlApiConf)
  }

  def getConfGetter: AtomicReference[() => SqlApiConf] = confGetter

  /**
   * Sets the active config getter.
   */
  def setConfGetter(getter: () => SqlApiConf): Unit = {
    confGetter.set(getter)
  }
}
