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

import org.apache.spark.sql.catalyst.analysis.MultiAlias
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  Generator,
  GeneratorOuter,
  NamedExpression
}
import org.apache.spark.sql.catalyst.types.DataTypeUtils

/**
 * Trait that provides utility methods for converting [[Alias]] and [[MultiAlias]] over
 * [[Generator]] into [[AliasedGenerator]].
 */
trait ConvertsToAliasedGenerator {
  protected def scopes: NameScopeStack
  protected def traversals: ExpressionTreeTraversalStack
  protected def expressionIdAssigner: ExpressionIdAssigner

  /**
   * Utility method that converts [[Alias]] and [[MultiAlias]] over [[Generator]] or
   * [[AliasedGenerator]] into [[AliasedGenerator]] if applicable and does nothing if not.
   *
   * Handles two cases:
   *  - Creating new [[AliasedGenerator]] from [[Generator]] wrapped in alias
   *  - Updating existing [[AliasedGenerator]] with new names (collapsing nested aliases)
   *
   * At the end, registers the generator output names in the current scope for LCA detection. Note,
   * that it's a temporary solution untill proper LCA resolution for generator is implemented. See
   * [[NameScope.tryResolveMultipartNameAsGeneratorOutput]]
   */
  protected def tryCreateAliasedGenerator(expression: NamedExpression): NamedExpression = {
    val result = expression match {
      case Alias(GeneratorOuter(generator: Generator), name) =>
        createAliasedGenerator(generator, Seq(name), outer = true)
      case Alias(generator: Generator, name) =>
        createAliasedGenerator(generator, Seq(name), outer = false)
      case MultiAlias(GeneratorOuter(generator: Generator), names) =>
        createAliasedGenerator(generator, names, outer = true)
      case MultiAlias(generator: Generator, names) =>
        createAliasedGenerator(generator, names, outer = false)
      case Alias(aliasedGenerator: AliasedGenerator, name) =>
        aliasedGenerator.copy(names = Seq(name))
      case MultiAlias(aliasedGenerator: AliasedGenerator, names) =>
        aliasedGenerator.copy(names = names)
      case other => other
    }
    result match {
      case aliasedGenerator: AliasedGenerator =>
        registerGeneratorOutputNames(aliasedGenerator)
        aliasedGenerator
      case other => other
    }
  }

  private def registerGeneratorOutputNames(aliasedGenerator: AliasedGenerator): Unit = {
    aliasedGenerator.names.foreach { name =>
      scopes.current.generatorOutputNames.put(name, true)
    }
  }

  /**
   * Factory method for creating [[AliasedGenerator]] by gathering all necessary info from
   * the context.
   *
   * Creates output attributes with default names from the generator's element schema and assigns
   * `exprId`s to them. The user-provided names are stored separately and applied lazily when
   * [[AliasedGenerator.generatorOutput]] is accessed.
   *
   * Note, that here we create output of the future [[Generate]] node. This is done to allow LCA
   * referencing the output of the generator. This is a point where we introduce new attributes so
   * we need to register it by calling [[ExpressionIdAssigner.mapExpression]] with
   * `allowUpdatesForAttributeReferences = true`.
   */
  private def createAliasedGenerator(
      generator: Generator,
      names: Seq[String],
      outer: Boolean): AliasedGenerator = {
    val generatorOutputWithDefaultNames =
      DataTypeUtils.toAttributes(generator.elementSchema).map { attr =>
        expressionIdAssigner
          .mapExpression(attr, allowUpdatesForAttributeReferences = true)
          .asInstanceOf[Attribute]
      }
    val missingInput = scopes.current.resolveMissingAttributesByHiddenOutput(
      traversals.current.referencedAttributes
    )
    AliasedGenerator(
      child = generator,
      generatorOutputWithDefaultNames = generatorOutputWithDefaultNames,
      names = names,
      outer = outer,
      missingInput = missingInput
    )
  }
}
