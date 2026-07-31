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

package org.apache.spark.sql.execution.externalUDF

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.PythonUDF.isScalarPythonUDF
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXTERNAL_UDF, PYTHON_UDF}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.udf.worker.UDFWorkerSpecification

/**
 * Extracts scalar external UDF expressions into logical evaluation nodes.
 *
 * Each [[EvalExternalUDF]] represents one worker session. A worker that does
 * not advertise UDF chaining receives one UDF call per node. A worker that
 * supports chaining may evaluate unary nested chains and fuse independent
 * UDF roots that use the same worker specification.
 *
 * When unified UDF execution is enabled, scalar [[PythonUDF]] expressions are
 * converted to [[ExternalUserDefinedFunction]] expressions before extraction.
 */
private[sql] class ExtractExternalUDFs(sparkConf: SparkConf) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // A correlated subquery is rewritten as a join and revisits this rule later.
    case subquery: Subquery if subquery.correlated => plan
    case _ =>
      val externalPlan = convertPythonUDFs(plan)
      externalPlan.transformUpWithPruning(_.containsPattern(EXTERNAL_UDF)) {
        // These nodes already own their external UDF expressions.
        case udfPlan: ExternalUDF => udfPlan
        case other => extract(other)
      }
  }

  private def convertPythonUDFs(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.UNIFIED_UDF_EXECUTION_ENABLED)) {
      plan
    } else {
      plan.transformUpWithPruning(_.containsPattern(PYTHON_UDF)) { operator =>
        operator.transformExpressionsUpWithPruning(_.containsPattern(PYTHON_UDF)) {
          case udf: PythonUDF if isScalarPythonUDF(udf) =>
            ExternalUserDefinedFunction(
              name = Some(udf.name),
              workerSpec = PythonUDFWorkerSpecification.fromPythonFunction(udf.func, sparkConf),
              payload = udf.func.command.toArray,
              dataType = udf.dataType,
              children = udf.children,
              inputTypes = None,
              udfDeterministic = udf.udfDeterministic,
              udfNullable = udf.nullable,
              resultId = udf.resultId)
        }
      }
    }
  }

  private def supportsUDFChaining(workerSpec: UDFWorkerSpecification): Boolean = {
    workerSpec.hasCapabilities &&
      workerSpec.getCapabilities.hasSupportsUdfChaining &&
      workerSpec.getCapabilities.getSupportsUdfChaining
  }

  private def containsExternalUDF(expression: Expression): Boolean = {
    expression.exists(_.isInstanceOf[ExternalUserDefinedFunction])
  }

  /** Returns whether `udf` can be evaluated in one worker session. */
  @scala.annotation.tailrec
  private def isEvaluable(udf: ExternalUserDefinedFunction): Boolean = {
    udf.children match {
      case Seq(child: ExternalUserDefinedFunction)
          if supportsUDFChaining(udf.workerSpec) && child.workerSpec == udf.workerSpec =>
        isEvaluable(child)
      case children =>
        !children.exists(containsExternalUDF)
    }
  }

  private def sameUDF(
      left: ExternalUserDefinedFunction,
      right: ExternalUserDefinedFunction): Boolean = {
    if (left.deterministic && right.deterministic) {
      left.semanticEquals(right)
    } else {
      left.resultId == right.resultId
    }
  }

  private def collectEvaluableUDFs(expression: Expression): Seq[ExternalUserDefinedFunction] = {
    expression match {
      case udf: ExternalUserDefinedFunction if isEvaluable(udf) => Seq(udf)
      case other => other.children.flatMap(collectEvaluableUDFs)
    }
  }

  /** Selects the UDF roots that can share the next worker session. */
  private def collectSessionUDFs(plan: LogicalPlan): Seq[ExternalUserDefinedFunction] = {
    val candidates = plan.expressions
      .flatMap(collectEvaluableUDFs)
      .filter(_.references.subsetOf(plan.inputSet))

    val distinct = ArrayBuffer.empty[ExternalUserDefinedFunction]
    candidates.foreach { candidate =>
      if (!distinct.exists(sameUDF(_, candidate))) {
        distinct += candidate
      }
    }

    distinct.headOption.toSeq.flatMap { first =>
      if (supportsUDFChaining(first.workerSpec)) {
        distinct.filter(_.workerSpec == first.workerSpec)
      } else {
        Seq(first)
      }
    }
  }

  /** Extracts one worker session and recursively extracts the remaining UDFs. */
  private def extract(plan: LogicalPlan): LogicalPlan = {
    val udfs = collectSessionUDFs(plan)
    if (udfs.isEmpty) {
      plan
    } else {
      val udfToAttribute = ArrayBuffer.empty[(ExternalUserDefinedFunction, Attribute)]

      def resultFor(udf: ExternalUserDefinedFunction): Option[Attribute] = {
        udfToAttribute.collectFirst {
          case (candidate, result) if sameUDF(candidate, udf) => result
        }
      }

      val newChildren = plan.children.map { child =>
        val validUdfs = udfs.filter(_.references.subsetOf(child.outputSet))
        if (validUdfs.isEmpty) {
          child
        } else {
          val resultAttrs = validUdfs.zipWithIndex.map { case (udf, index) =>
            AttributeReference(s"externalUDF$index", udf.dataType, udf.nullable)()
          }
          val evaluation = EvalExternalUDF(
            validUdfs.head.workerSpec, validUdfs, resultAttrs, child)
          udfToAttribute ++= validUdfs.zip(resultAttrs)
          evaluation
        }
      }

      udfs.filter(resultFor(_).isEmpty).foreach { udf =>
        throw SparkException.internalError(
          s"Invalid external UDF $udf, requires attributes from more than one child.")
      }

      val rewritten = plan.withNewChildren(newChildren).transformExpressions {
        case udf: ExternalUserDefinedFunction => resultFor(udf).getOrElse(udf)
      }

      val newPlan = extract(rewritten)
      if (newPlan.output != plan.output) {
        Project(plan.output, newPlan)
      } else {
        newPlan
      }
    }
  }
}
