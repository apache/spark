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

import java.util.{ArrayDeque, ArrayList, HashSet}

import scala.jdk.CollectionConverters._

import com.databricks.sql.DatabricksSQLConf

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.resolver.AliasKind._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * [[LateralColumnAliasRegistryImpl]] is a utility class that contains structures required for
 * lateral column alias resolution. Here we store:
 *  - [[currentAttributeDependencyLevelStack]] - Current attribute dependency level in the scope.
 *  Dependency level is defined as a maximum dependency in that attribute's expression tree. For
 *  example, in a query like:
 *
 *  {{{ SELECT a, b, a + b AS c, a + c AS d}}}
 *
 * Dependency levels will be as follows:
 * level 0: a, b
 * level 1: c
 * level 2: d
 *
 *  We add a new entry to the stack for each new [[Alias]] resolution. This is needed because we
 *  can have nesting Aliases in the plan, that do not belong to the same LCA scope. For example,
 *  in the following query:
 *
 *  {{{ SELECT STRUCT('alpha' AS A, 'beta' AS B) ST }}}
 *
 *  ST, A and B would be aliases in the same expression tree, but they do not belong in the same
 *  LCA scope.
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

  private val currentAttributeDependencyLevelStack: ArrayDeque[Int] = new ArrayDeque[Int]

  private val availableAttributes = new IdentifierMap[ArrayList[AliasReference]]

  private val referencedAliases = new HashSet[Attribute]
  private val aliasDependencyLevels = new ArrayList[ArrayList[Alias]]

  /**
   * Creates a new LCA resolution scope for each [[Alias]] resolution. Executes the lambda and
   * registers top-level resolved aliases for later LCA resolution.
   */
  def withNewLcaScope(aliasKind: AliasKind = AliasKind.Explicit)(body: => Alias): Alias = {
    val isTopLevelAlias = currentAttributeDependencyLevelStack.isEmpty
    currentAttributeDependencyLevelStack.push(0)

    try {
      val resolvedAlias = body
      if (isTopLevelAlias) {
        registerAlias(alias = resolvedAlias, aliasKind = aliasKind)
      }
      resolvedAlias
    } finally {
      currentAttributeDependencyLevelStack.pop()
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
        if (!currentAttributeDependencyLevelStack.isEmpty) {
          // compute new dependency as a maximum of current dependency and dependency of the
          // referenced attribute incremented by 1.
          val maxDependencyLevel = Math.max(
            currentAttributeDependencyLevelStack.pop(),
            aliasReference.dependencyLevel + 1
          )
          currentAttributeDependencyLevelStack.push(maxDependencyLevel)
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
  private def registerAlias(alias: Alias, aliasKind: AliasKind = AliasKind.Explicit): Unit = {
    addAliasDependency(alias)
    registerAttribute(
      attribute = alias.toAttribute,
      dependencyLevel = currentAttributeDependencyLevelStack.peek(),
      aliasKind = aliasKind
    )
  }

  /**
   * Returns the [[AliasReference]] if there is only one match or throws an ambiguous LCA error
   * otherwise.
   *
   * If the resulting LCA is referencing an implicit alias and
   * `spark.databricks.sql.singlePassResolver.allowLcaOnImplicitAlias` is false, throw an error.
   * We throw this error in single-pass in order to avoid ambiguity when resolving LCA on implicit
   * aliases vs. variables and outer references. Currently, fixed-point does not throw but attempts
   * to resolve the reference.
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

    val aliasReference = aliasReferenceList.get(0)

    if (aliasReference.aliasKind == AliasKind.Implicit &&
      !conf.getConf(DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ALLOW_LCA_ON_IMPLICIT_ALIAS)) {
      throw QueryCompilationErrors.lateralColumnAliasOnImplicitlyGeneratedAlias(
        name = attributeName,
        implicitAlias = aliasReference.attribute
      )
    } else {
      aliasReference
    }
  }

  private def registerAttribute(
      attribute: Attribute,
      dependencyLevel: Int = 0,
      aliasKind: AliasKind = AliasKind.Explicit
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
    val dependencyLevel = currentAttributeDependencyLevelStack.peek()
    // If targeted dependency level does not exist yet, create it now.
    if (aliasDependencyLevels.size() <= dependencyLevel) {
      aliasDependencyLevels.add(new ArrayList[Alias])
    }
    val dependencyLevelList = aliasDependencyLevels.get(dependencyLevel)
    dependencyLevelList.add(alias)
    aliasDependencyLevels.set(dependencyLevel, dependencyLevelList)
  }
}
