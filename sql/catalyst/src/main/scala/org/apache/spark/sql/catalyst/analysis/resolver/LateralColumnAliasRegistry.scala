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

package org.apache.spark.sql.catalyst.analysis.resolver

import java.util.ArrayList

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}

/**
 * Base class for lateral column alias registry. This class is extended by 2 implementations:
 *  1. [[LateralColumnAliasRegistryImpl]] - When [[SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED]]
 *  is enabled, this class implements logic for LCA resolution.
 *  2. [[LateralColumnAliasProhibitedRegistry]] - Dummy class whose methods throw exceptions when
 *  LCA resolution is disabled by [[SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED]].
 */
abstract class LateralColumnAliasRegistry {
  def withNewLcaScope(body: => Alias): Alias

  def getAttribute(attributeName: String): Option[Attribute]

  def getAliasDependencyLevels(): ArrayList[ArrayList[Alias]]

  def markAttributeLaterallyReferenced(attribute: Attribute): Unit

  def isAttributeLaterallyReferenced(attribute: Attribute): Boolean

  protected def throwLcaResolutionNotEnabled(): Nothing = {
    throw SparkException.internalError("Lateral column alias resolution is not enabled.")
  }
}
