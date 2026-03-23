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

package org.apache.spark.internal.config

/**
 * Defines how a configuration value is bound when used within SQL views, UDFs, or procedures.
 *
 * This enum controls whether a config value propagates from the active session or uses the value
 * saved during view/UDF/procedure creation. If the policy is PERSISTED, but there is no saved
 * value, a Spark default value is used.
 *
 * This is particularly important for configs that affect query behavior and where views/UDFs/
 * procedures should change their behavior based on the caller's session settings. If the policy
 * is PERSISTED, session-level config changes will not apply to views/UDFs/procedures but only
 * to outer queries. In order for session-level changes to propagate correctly, this value must
 * be explicitly set to SESSION.
 */
object ConfigBindingPolicy extends Enumeration {
  type ConfigBindingPolicy = Value

  /**
   * The config value propagates from the active session to views/UDFs/procedures.
   * This is important for queries that should have uniform behavior across the entire query.
   */
  val SESSION: Value = Value("SESSION")

  /**
   * The config uses the value saved on view/UDF/procedure creation if it exists,
   * or Spark default value for that config if it doesn't.
   */
  val PERSISTED: Value = Value("PERSISTED")

  /**
   * The config does not apply to views/UDFs/procedures. If this config is accessed during
   * view/UDF/procedure resolution, the value will be read from the active session (same as
   * [[SESSION]]).
   */
  val NOT_APPLICABLE: Value = Value("NOT_APPLICABLE")
}
