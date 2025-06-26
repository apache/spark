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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}

/**
 * Dummy implementation of [[LateralColumnAliasRegistry]] used when
 * [[SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED]] is disabled. Getter methods throw an exception
 * as they should never be called on a dummy implementation. Non-getter methods must remain
 * idempotent.
 */
class LateralColumnAliasProhibitedRegistry extends LateralColumnAliasRegistry {
  def withNewLcaScope(body: => Alias): Alias = body

  def getAttribute(attributeName: String): Option[Attribute] =
    throwLcaResolutionNotEnabled()

  def getAliasDependencyLevels(): ArrayList[ArrayList[Alias]] =
    throwLcaResolutionNotEnabled()

  def markAttributeLaterallyReferenced(attribute: Attribute): Unit =
    throwLcaResolutionNotEnabled()

  def isAttributeLaterallyReferenced(attribute: Attribute): Boolean =
    throwLcaResolutionNotEnabled()
}
