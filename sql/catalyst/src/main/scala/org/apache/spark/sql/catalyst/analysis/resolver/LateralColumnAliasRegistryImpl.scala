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

import java.util.{ArrayList, HashSet}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.resolver.AliasKind._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, NamedExpression}
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * [[LateralColumnAliasRegistryImpl]] is a utility class that contains structures required for
 * lateral column alias resolution. Here we store:
 *  - [[currentDependencyLevel]] - Current attribute dependency level while resolving a top-level
 *  alias. Dependency level is defined as the maximum dependency in that attribute's expression
 *  tree. For example, in a query like:
 *
 *  {{{ SELECT a, b, a + b AS c, a + c AS d}}}
 *
 * Dependency levels will be as follows:
 * level 0: a, b
 * level 1: c
 * level 2: d
 *
 *  Set to [[Some]] only while resolving a top-level alias (direct child of the project/aggregate
 *  list). Nested aliases (e.g., struct field aliases inside [[CreateNamedStruct]] or inner aliases
 *  from variant path extraction) share the outer value so that [[getAttribute]] correctly
 *  accumulates the maximum dependency level across the entire expression tree.
 *
 *  - [[availableAttributes]] - All attributes that can be laterally referenced. This map is
 *  indexed by name, but contains a list of attributes with the same name. This is because it is
 *  possible to have multiple attributes with the same name in the scope, but they can't be
 *  laterally referenced. Handling ambiguous references is done in the [[getAttribute]] method.
 *  For the following query:
 *
 *  {{{ SELECT 0 AS a, 1 AS b, 2 AS c, b AS d, a AS e, d AS f, a AS g, g AS h, h AS i }}}
 *
 *  [[availableAttributes]] will be: {a, b, c, d, e, f, g, h, i}
 *  - [[referencedAliases]] - Aliases that have been laterally referenced. For the given query
 *  example, [[referencedAliases]] will be: {a, b, d, g, h}
 *  - [[aliasDependencyLevels]] - Dependency levels of all aliases, indexed by dependency level.
 *  For the given query example, dependency levels will be as follows:
 *
 *  level 0: a, b, c
 *  level 1: d, e, g
 *  level 2: f, h
 *  level 3: i
 *
 */
class LateralColumnAliasRegistryImpl extends LateralColumnAliasRegistry with SQLConfHelper {
  private case class AliasReference(
      attribute: Attribute,
      dependencyLevel: Int,
      aliasKind: AliasKind
  )

  private var currentDependencyLevel: Option[Int] = None

  private val availableAttributes = new IdentifierMap[ArrayList[AliasReference]]

  private val referencedAliases = new HashSet[Attribute]
  private val aliasDependencyLevels = new ArrayList[ArrayList[Alias]]

  /**
   * Creates an LCA resolution scope for [[Alias]] resolution. Only top-level aliases (those where
   * [[currentDependencyLevel]] is [[None]] on entry) initialize the dependency level; nested
   * aliases share the outer value so that [[getAttribute]] accumulates the correct maximum
   * dependency level. On exit, only top-level aliases reset and register the resolved alias for
   * later LCA resolution.
   */
  def withNewLcaScope[T <: NamedExpression](
      aliasKind: AliasKind = AliasKind.Explicit)(body: => T): T = {
    val isTopLevelAlias = currentDependencyLevel.isEmpty

    if (isTopLevelAlias) {
      currentDependencyLevel = Some(0)
    }

    try {
      val resolvedExpression = body
      resolvedExpression match {
        case resolvedAlias: Alias if isTopLevelAlias =>
          registerAlias(alias = resolvedAlias, aliasKind = aliasKind)
        case _ =>
      }
      resolvedExpression
    } finally {
      if (isTopLevelAlias) {
        currentDependencyLevel = None
      }
    }
  }

  /**
   * Gets the attribute needed for LCA resolution by given name from the set of available
   * attributes. If there are multiple matches, throws [[ambiguousLateralColumnAliasError]] error.
   * If the method is called while resolving an [[Alias]], updates the dependency level in the
   * current scope.
   */
  def getAttribute(attributeName: String): Option[Attribute] = {
    availableAttributes.get(attributeName) match {
      case None => None
      case Some(aliasReferenceList: ArrayList[AliasReference]) =>
        val aliasReference = pickAliasReference(aliasReferenceList, attributeName)
        currentDependencyLevel = currentDependencyLevel.map { current =>
          Math.max(current, aliasReference.dependencyLevel + 1)
        }

        Some(aliasReference.attribute)
    }
  }

  /**
   * Returns the dependency levels of all aliases.
   */
  def getAliasDependencyLevels(): ArrayList[ArrayList[Alias]] = aliasDependencyLevels

  /**
   * Adds an attribute to the set of attributes that have been laterally referenced.
   */
  def markAttributeLaterallyReferenced(attribute: Attribute): Unit =
    referencedAliases.add(attribute)

  /**
   * Returns true if the attribute has been laterally referenced, false otherwise.
   */
  def isAttributeLaterallyReferenced(attribute: Attribute): Boolean =
    referencedAliases.contains(attribute)

  /**
   * Returns all laterally referenced attributes in the current scope.
   */
  def getAllLaterallyReferencedAttributes: Seq[Attribute] =
    referencedAliases.asScala.toSeq

  /**
   * Registers an alias for LCA resolution by adding it to correct dependency level. Additionally,
   * register a reference to the alias for further LCA chaining.
   */
  private def registerAlias(alias: Alias, aliasKind: AliasKind): Unit = {
    addAliasDependency(alias)
    registerAttribute(
      attribute = alias.toAttribute,
      dependencyLevel = currentDependencyLevel.get,
      aliasKind = aliasKind
    )
  }

  /**
   * Returns the [[AliasReference]] if there is only one match or throws an ambiguous LCA error
   * otherwise.
   */
  private def pickAliasReference(
      aliasReferenceList: ArrayList[AliasReference],
      attributeName: String) = {
    if (aliasReferenceList.size() > 1) {
      throw QueryCompilationErrors.ambiguousLateralColumnAliasError(
        attributeName,
        aliasReferenceList.size()
      )
    }

    aliasReferenceList.get(0)
  }

  private def registerAttribute(
      attribute: Attribute,
      dependencyLevel: Int,
      aliasKind: AliasKind
  ): Unit = {
    availableAttributes
      .computeIfAbsent(attribute.name, _ => new ArrayList[AliasReference])
      .add(
        AliasReference(
          attribute = attribute,
          dependencyLevel = dependencyLevel,
          aliasKind = aliasKind
        )
      )
  }

  private def addAliasDependency(alias: Alias): Unit = {
    val dependencyLevel = currentDependencyLevel.get
    if (aliasDependencyLevels.size() <= dependencyLevel) {
      aliasDependencyLevels.add(new ArrayList[Alias])
    }
    aliasDependencyLevels.get(dependencyLevel).add(alias)
  }
}
