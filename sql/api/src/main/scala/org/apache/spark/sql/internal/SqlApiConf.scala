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

import java.util.TimeZone

import scala.util.Try

import org.apache.spark.sql.types.{AtomicType, StringType, TimestampType}
import org.apache.spark.util.SparkClassUtils

/**
 * Configuration for all objects that are placed in the `sql/api` project. The normal way of
 * accessing this class is through `SqlApiConf.get`. If this code is being used with sql/core then
 * its values are bound to the currently set SQLConf. With Spark Connect, it will default to
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
  def timestampType: AtomicType
  def allowNegativeScaleOfDecimalEnabled: Boolean
  def charVarcharAsString: Boolean
  def datetimeJava8ApiEnabled: Boolean
  def sessionLocalTimeZone: String
  def legacyTimeParserPolicy: LegacyBehaviorPolicy.Value
  def defaultStringType: StringType
  def stackTracesInDataFrameContext: Int
  def dataFrameQueryContextEnabled: Boolean
  def legacyAllowUntypedScalaUDFs: Boolean
  def allowReadingUnknownCollations: Boolean
}

private[sql] object SqlApiConf {
  // Shared keys.
  val ANSI_ENABLED_KEY: String = SqlApiConfHelper.ANSI_ENABLED_KEY
  val LEGACY_TIME_PARSER_POLICY_KEY: String = SqlApiConfHelper.LEGACY_TIME_PARSER_POLICY_KEY
  val CASE_SENSITIVE_KEY: String = SqlApiConfHelper.CASE_SENSITIVE_KEY
  val SESSION_LOCAL_TIMEZONE_KEY: String = SqlApiConfHelper.SESSION_LOCAL_TIMEZONE_KEY
  val LOCAL_RELATION_CACHE_THRESHOLD_KEY: String = {
    SqlApiConfHelper.LOCAL_RELATION_CACHE_THRESHOLD_KEY
  }
  val DEFAULT_COLLATION: String = SqlApiConfHelper.DEFAULT_COLLATION
  val ALLOW_READING_UNKNOWN_COLLATIONS: String = SqlApiConfHelper.ALLOW_READING_UNKNOWN_COLLATIONS

  def get: SqlApiConf = SqlApiConfHelper.getConfGetter.get()()

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
  override def timestampType: AtomicType = TimestampType
  override def allowNegativeScaleOfDecimalEnabled: Boolean = false
  override def charVarcharAsString: Boolean = false
  override def datetimeJava8ApiEnabled: Boolean = false
  override def sessionLocalTimeZone: String = TimeZone.getDefault.getID
  override def legacyTimeParserPolicy: LegacyBehaviorPolicy.Value = LegacyBehaviorPolicy.CORRECTED
  override def defaultStringType: StringType = StringType
  override def stackTracesInDataFrameContext: Int = 1
  override def dataFrameQueryContextEnabled: Boolean = true
  override def legacyAllowUntypedScalaUDFs: Boolean = false
  override def allowReadingUnknownCollations: Boolean = false
}
