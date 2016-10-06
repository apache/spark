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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.analysis._

/**
 * Interface for configuration options used in the catalyst module.
 */
trait CatalystConf {
  def caseSensitiveAnalysis: Boolean

  def orderByOrdinal: Boolean
  def groupByOrdinal: Boolean

  def optimizerMaxIterations: Int
  def optimizerInSetConversionThreshold: Int
  def maxCaseBranchesForCodegen: Int

  def maxDepthForCNFNormalization: Int
  def maxPredicateNumberForCNFNormalization: Int

  def runSQLonFile: Boolean

  def warehousePath: String

  /** If true, cartesian products between relations will be allowed for all
   * join types(inner, (left|right|full) outer).
   * If false, cartesian products will require explicit CROSS JOIN syntax.
   */
  def crossJoinEnabled: Boolean

  /**
   * Returns the [[Resolver]] for the current configuration, which can be used to determine if two
   * identifiers are equal.
   */
  def resolver: Resolver = {
    if (caseSensitiveAnalysis) caseSensitiveResolution else caseInsensitiveResolution
  }
}


/** A CatalystConf that can be used for local testing. */
case class SimpleCatalystConf(
    caseSensitiveAnalysis: Boolean,
    orderByOrdinal: Boolean = true,
    groupByOrdinal: Boolean = true,
    optimizerMaxIterations: Int = 100,
    optimizerInSetConversionThreshold: Int = 10,
    maxCaseBranchesForCodegen: Int = 20,
    maxDepthForCNFNormalization: Int = 10,
    maxPredicateNumberForCNFNormalization: Int = 20,
    runSQLonFile: Boolean = true,
    crossJoinEnabled: Boolean = false,
    warehousePath: String = "/user/hive/warehouse")
  extends CatalystConf
