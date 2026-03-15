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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  Expression,
  ExprId,
  Generator,
  GeneratorOuter,
  NamedExpression,
  UnaryExpression,
  Unevaluable
}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.DataType

/**
 * Represents an aliased generator expression with all information needed for creating [[Generate]].
 * This is an intermediate state used during single-pass analysis - it will be transformed
 * into a [[Generate]] operator by [[GeneratorResolver.resolveProjectListWithGenerators]].
 *
 * Similar to [[MultiAlias]], this expression throws on methods that don't make sense
 * for a temporary entity that could have multiple names.
 *
 * The generator output is computed lazily from [[generatorOutputWithDefaultNames]] and [[names]].
 * If [[names]] is empty, the default names from the generator's element schema are used.
 * If [[names]] is provided, they are applied via `withName` to preserve the original `exprId`s.
 * If the number of names doesn't match the number of output attributes, an error is thrown
 * when [[generatorOutput]] is accessed.
 *
 * @param child The generator expression (e.g., [[Explode]], [[PosExplode]]).
 * @param generatorOutputWithDefaultNames Output attributes with default names and assigned
 *                                        `exprId`s. Created at construction time.
 * @param names User-provided names from [[Alias]] or [[MultiAlias]]. Empty means use defaults.
 * @param outer Whether this is an outer generator (e.g., `explode_outer`).
 * @param missingInput Attributes referenced by the generator that are in hidden output.
 */
case class AliasedGenerator(
    child: Generator,
    generatorOutputWithDefaultNames: Seq[Attribute],
    names: Seq[String] = Seq.empty,
    outer: Boolean = false,
    missingInput: Seq[Attribute] = Seq.empty
) extends UnaryExpression
    with NamedExpression
    with Unevaluable {

  if (!child.resolved) {
    throw SparkException.internalError("AliasedGenerator requires a resolved generator")
  }
  if (child.isInstanceOf[GeneratorOuter]) {
    throw SparkException.internalError("Use outer flag instead of GeneratorOuter wrapper")
  }

  /**
   * Computes the generator output attributes by applying user-provided names to the default
   * output attributes. If names is empty, returns default attributes. If names count doesn't
   * match, throws [[UDTF_ALIAS_NUMBER_MISMATCH]] error.
   */
  lazy val generatorOutput: Seq[Attribute] = {
    if (names.isEmpty) {
      generatorOutputWithDefaultNames
    } else if (names.length == generatorOutputWithDefaultNames.length) {
      names.zip(generatorOutputWithDefaultNames).map { case (name, attr) =>
        attr.withName(name)
      }
    } else {
      throw QueryCompilationErrors.aliasesNumberNotMatchUDTFOutputError(
        generatorOutputWithDefaultNames.size,
        names.mkString(",")
      )
    }
  }

  /**
   * Attributes with nullability that respects the outer flag. Mirrors [[Generate.nullableOutput]].
   */
  def nullableGeneratorOutput: Seq[Attribute] = {
    generatorOutput.map { attribute =>
      attribute.withNullability(outer || attribute.nullable)
    }
  }

  override def name: String = throw new UnresolvedException("name")

  override def exprId: ExprId = throw new UnresolvedException("exprId")

  override def dataType: DataType = throw new UnresolvedException("dataType")

  override def nullable: Boolean = throw new UnresolvedException("nullable")

  override def qualifier: Seq[String] = throw new UnresolvedException("qualifier")

  override def toAttribute: Attribute = throw new UnresolvedException("toAttribute")

  override def newInstance(): NamedExpression = throw new UnresolvedException("newInstance")

  /**
   * AliasedGenerator should never be resolved, due to the fact that it is a temporary
   * expression that will be transformed into a [[Generate]] operator during the analysis phase.
   * It should never appear in the final plan.
   */
  override lazy val resolved: Boolean = false

  override def toString: String =
    s"${child}${if (outer) "_outer" else ""}" +
    s"AS ${generatorOutput.map(_.name).mkString("(", ", ", ")")}"

  override protected def withNewChildInternal(newChild: Expression): AliasedGenerator =
    copy(child = newChild.asInstanceOf[Generator])
}
