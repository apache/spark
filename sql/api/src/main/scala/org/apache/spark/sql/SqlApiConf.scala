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
package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicReference

import scala.util.Try

import org.apache.spark.util.SparkClassUtils

/**
 * Configuration for all objects that are placed in the `sql/api` project. The normal way of
 * accessing this class is through `SqlApiConf.get`. If this code is being used with sql/core
 * then its values are bound to the currently set SQLConf. With Spark Connect, it will default to
 * hardcoded values.
 */
private[sql] trait SqlApiConf {
  def ansiEnabled: Boolean
  def caseSensitiveAnalysis: Boolean
  def maxToStringFields: Int
  def setOpsPrecedenceEnforced: Boolean
  def exponentLiteralAsDecimalEnabled: Boolean
  def enforceReservedKeywords: Boolean
  def doubleQuotedIdentifiers: Boolean
}

private[sql] object SqlApiConf {
  /**
   * Defines a getter that returns the [[SqlApiConf]] within scope.
   */
  private val confGetter = new AtomicReference[() => SqlApiConf](() => DefaultSqlApiConf)

  /**
   * Sets the active config getter.
   */
  private[sql] def setConfGetter(getter: () => SqlApiConf): Unit = {
    confGetter.set(getter)
  }

  def get: SqlApiConf = confGetter.get()()

  // Force load SQLConf. This will trigger the installation of a confGetter that points to SQLConf.
  Try(SparkClassUtils.classForName("org.apache.spark.sql.internal.SQLConf$"))
}

/**
 * Defaults configurations used when no other [[SqlApiConf]] getter is set.
 */
private[sql] object DefaultSqlApiConf extends SqlApiConf {
  override def ansiEnabled: Boolean = false
  override def caseSensitiveAnalysis: Boolean = false
  override def maxToStringFields: Int = 50
  override def setOpsPrecedenceEnforced: Boolean = false
  override def exponentLiteralAsDecimalEnabled: Boolean = false
  override def enforceReservedKeywords: Boolean = false
  override def doubleQuotedIdentifiers: Boolean = false
}
