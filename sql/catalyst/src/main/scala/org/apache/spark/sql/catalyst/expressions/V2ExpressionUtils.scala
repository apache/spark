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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{FunctionCatalog, Identifier}
import org.apache.spark.sql.connector.catalog.functions._
import org.apache.spark.sql.connector.expressions.{BucketTransform, Expression => V2Expression, FieldReference, IdentityTransform, NamedReference, NamedTransform, NullOrdering => V2NullOrdering, SortDirection => V2SortDirection, SortOrder => V2SortOrder, SortValue, Transform}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.Utils.sequenceToOption

/**
 * A utility class that converts public connector expressions into Catalyst expressions.
 */
object V2ExpressionUtils extends SQLConfHelper with Logging {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper

  def resolveRef[T <: NamedExpression](ref: NamedReference, plan: LogicalPlan): T = {
    plan.resolve(ref.fieldNames, conf.resolver) match {
      case Some(namedExpr) =>
        namedExpr.asInstanceOf[T]
      case None =>
        val name = ref.fieldNames.toSeq.quoted
        val outputString = plan.output.map(_.name).mkString(",")
        throw QueryCompilationErrors.cannotResolveAttributeError(name, outputString)
    }
  }

  def resolveRefs[T <: NamedExpression](refs: Seq[NamedReference], plan: LogicalPlan): Seq[T] = {
    refs.map(ref => resolveRef[T](ref, plan))
  }

  /**
   * Converts the array of input V2 [[V2SortOrder]] into their counterparts in catalyst.
   */
  def toCatalystOrdering(ordering: Array[V2SortOrder], query: LogicalPlan): Seq[SortOrder] = {
    sequenceToOption(ordering.map(toCatalyst(_, query))).asInstanceOf[Option[Seq[SortOrder]]]
      .getOrElse(Seq.empty)
  }

  def toCatalyst(
      expr: V2Expression,
      query: LogicalPlan,
      funCatalogOpt: Option[FunctionCatalog] = None): Option[Expression] = {
    expr match {
      case t: Transform =>
        toCatalystTransform(t, query, funCatalogOpt)
      case SortValue(child, direction, nullOrdering) =>
        toCatalyst(child, query, funCatalogOpt).map { catalystChild =>
          SortOrder(catalystChild, toCatalyst(direction), toCatalyst(nullOrdering), Seq.empty)
        }
      case ref: FieldReference =>
        Some(resolveRef[NamedExpression](ref, query))
      case _ =>
        throw new AnalysisException(s"$expr is not currently supported")
    }
  }

  def toCatalystTransform(
      trans: Transform,
      query: LogicalPlan,
      funCatalogOpt: Option[FunctionCatalog] = None): Option[Expression] = trans match {
    case IdentityTransform(ref) =>
      Some(resolveRef[NamedExpression](ref, query))
    case BucketTransform(numBuckets, refs, sorted)
        if sorted.isEmpty && refs.length == 1 && refs.forall(_.isInstanceOf[NamedReference]) =>
      val resolvedRefs = refs.map(r => resolveRef[NamedExpression](r, query))
      // Create a dummy reference for `numBuckets` here and use that, together with `refs`, to
      // look up the V2 function.
      val numBucketsRef = AttributeReference("numBuckets", IntegerType, nullable = false)()
      funCatalogOpt.flatMap { catalog =>
        loadV2Function(catalog, "bucket", Seq(numBucketsRef) ++ resolvedRefs).map { bound =>
          TransformExpression(bound, resolvedRefs, Some(numBuckets))
        }
      }
    case NamedTransform(name, refs)
        if refs.length == 1 && refs.forall(_.isInstanceOf[NamedReference]) =>
      val resolvedRefs = refs.map(_.asInstanceOf[NamedReference]).map { r =>
        resolveRef[NamedExpression](r, query)
      }
      funCatalogOpt.flatMap { catalog =>
        loadV2Function(catalog, name, resolvedRefs).map { bound =>
          TransformExpression(bound, resolvedRefs)
        }
      }
    case _ =>
      throw new AnalysisException(s"Transform $trans is not currently supported")
  }

  private def loadV2Function(
      catalog: FunctionCatalog,
      name: String,
      args: Seq[Expression]): Option[BoundFunction] = {
    val inputType = StructType(args.zipWithIndex.map {
      case (exp, pos) => StructField(s"_$pos", exp.dataType, exp.nullable)
    })
    try {
      val unbound = catalog.loadFunction(Identifier.of(Array.empty, name))
      Some(unbound.bind(inputType))
    } catch {
      case _: NoSuchFunctionException =>
        val parameterString = args.map(_.dataType.typeName).mkString("(", ", ", ")")
        logWarning(s"V2 function $name with parameter types $parameterString is used in " +
            "partition transforms, but its definition couldn't be found in the function catalog " +
            "provided")
        None
      case _: UnsupportedOperationException =>
        None
    }
  }

  private def toCatalyst(direction: V2SortDirection): SortDirection = direction match {
    case V2SortDirection.ASCENDING => Ascending
    case V2SortDirection.DESCENDING => Descending
  }

  private def toCatalyst(nullOrdering: V2NullOrdering): NullOrdering = nullOrdering match {
    case V2NullOrdering.NULLS_FIRST => NullsFirst
    case V2NullOrdering.NULLS_LAST => NullsLast
  }
}
