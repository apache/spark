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

package org.apache.spark.sql.execution.python

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkException
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.REASON
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.types._


/**
 * Extracts all the Python UDFs in logical aggregate, which depends on aggregate expression or
 * grouping key, or doesn't depend on any above expressions, evaluate them after aggregate.
 */
object ExtractPythonUDFFromAggregate extends Rule[LogicalPlan] {

  /**
   * Returns whether the expression could only be evaluated within aggregate.
   */
  private def belongAggregate(e: Expression, agg: Aggregate): Boolean = {
    e.isInstanceOf[AggregateExpression] ||
      agg.groupingExpressions.exists(_.semanticEquals(e))
  }

  private def hasPythonUdfOverAggregate(expr: Expression, agg: Aggregate): Boolean = {
    expr.exists {
      e => PythonUDF.isScalarPythonUDF(e) &&
        (e.references.isEmpty || e.exists(belongAggregate(_, agg)))
    }
  }

  private def extract(agg: Aggregate): LogicalPlan = {
    val projList = new ArrayBuffer[NamedExpression]()
    val aggExpr = new ArrayBuffer[NamedExpression]()
    agg.aggregateExpressions.foreach { expr =>
      if (hasPythonUdfOverAggregate(expr, agg)) {
        // Python UDF can only be evaluated after aggregate
        val newE = expr transformDown {
          case e: Expression if belongAggregate(e, agg) =>
            val alias = e match {
              case a: NamedExpression => a
              case o => Alias(e, "agg")()
            }
            aggExpr += alias
            alias.toAttribute
        }
        projList += newE.asInstanceOf[NamedExpression]
      } else {
        aggExpr += expr
        projList += expr.toAttribute
      }
    }
    // There is no Python UDF over aggregate expression
    Project(projList.toSeq, agg.copy(aggregateExpressions = aggExpr.toSeq))
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsAllPatterns(PYTHON_UDF, AGGREGATE)) {
    case agg: Aggregate if agg.aggregateExpressions.exists(hasPythonUdfOverAggregate(_, agg)) =>
      extract(agg)
  }
}

/**
 * Extracts PythonUDFs in logical aggregate, which are used in grouping keys, evaluate them
 * before aggregate.
 * This must be executed after `ExtractPythonUDFFromAggregate` rule and before `ExtractPythonUDFs`.
 */
object ExtractGroupingPythonUDFFromAggregate extends Rule[LogicalPlan] {
  private def hasScalarPythonUDF(e: Expression): Boolean = {
    e.exists(PythonUDF.isScalarPythonUDF)
  }

  private def extract(agg: Aggregate): LogicalPlan = {
    val projList = new ArrayBuffer[NamedExpression]()
    val groupingExpr = new ArrayBuffer[Expression]()
    val attributeMap = mutable.HashMap[PythonUDF, NamedExpression]()

    agg.groupingExpressions.foreach { expr =>
      if (hasScalarPythonUDF(expr)) {
        val newE = expr transformDown {
          case p: PythonUDF =>
            // This is just a sanity check, the rule PullOutNondeterministic should
            // already pull out those nondeterministic expressions.
            assert(p.udfDeterministic, "Non-deterministic PythonUDFs should not appear " +
              "in grouping expression")
            val canonicalized = p.canonicalized.asInstanceOf[PythonUDF]
            if (attributeMap.contains(canonicalized)) {
              attributeMap(canonicalized)
            } else {
              val alias = Alias(p, "groupingPythonUDF")()
              projList += alias
              attributeMap += ((canonicalized, alias.toAttribute))
              alias.toAttribute
            }
        }
        groupingExpr += newE
      } else {
        groupingExpr += expr
      }
    }
    val aggExpr = agg.aggregateExpressions.map { expr =>
      expr.transformUp {
        // PythonUDF over aggregate was pull out by ExtractPythonUDFFromAggregate.
        // PythonUDF here should be either
        // 1. Argument of an aggregate function.
        //    CheckAnalysis guarantees the arguments are deterministic.
        // 2. PythonUDF in grouping key. Grouping key must be deterministic.
        // 3. PythonUDF not in grouping key. It is either no arguments or with grouping key
        // in its arguments. Such PythonUDF was pull out by ExtractPythonUDFFromAggregate, too.
        case p: PythonUDF if p.udfDeterministic =>
          val canonicalized = p.canonicalized.asInstanceOf[PythonUDF]
          attributeMap.getOrElse(canonicalized, p)
      }.asInstanceOf[NamedExpression]
    }
    agg.copy(
      groupingExpressions = groupingExpr.toSeq,
      aggregateExpressions = aggExpr,
      child = Project((projList ++ agg.child.output).toSeq, agg.child))
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsAllPatterns(PYTHON_UDF, AGGREGATE)) {
    case agg: Aggregate if agg.groupingExpressions.exists(hasScalarPythonUDF(_)) =>
      extract(agg)
  }
}

/**
 * Extracts PythonUDFs from operators, rewriting the query plan so that the UDF can be evaluated
 * alone in a batch.
 *
 * Only extracts the PythonUDFs that could be evaluated in Python (the single child is PythonUDFs
 * or all the children could be evaluated in JVM).
 *
 * This has the limitation that the input to the Python UDF is not allowed include attributes from
 * multiple child operators.
 */
object ExtractPythonUDFs extends Rule[LogicalPlan] with Logging {

  private type EvalType = Int
  private type EvalTypeChecker = EvalType => Boolean

  private def hasScalarPythonUDF(e: Expression): Boolean = {
    e.exists(PythonUDF.isScalarPythonUDF)
  }

  @scala.annotation.tailrec
  private def canEvaluateInPython(e: PythonUDF): Boolean = {
    e.children match {
      // single PythonUDF child could be chained and evaluated in Python
      case Seq(u: PythonUDF) => e.evalType == u.evalType && canEvaluateInPython(u)
      // Python UDF can't be evaluated directly in JVM
      case children => !children.exists(hasScalarPythonUDF)
    }
  }

  private def collectEvaluableUDFsFromExpressions(expressions: Seq[Expression]): Seq[PythonUDF] = {
    // If first UDF is SQL_SCALAR_PANDAS_ITER_UDF, then only return this UDF,
    // otherwise check if subsequent UDFs are of the same type as the first UDF. (since we can only
    // extract UDFs of the same eval type)

    var firstVisitedScalarUDFEvalType: Option[Int] = None

    def canChainUDF(evalType: Int): Boolean = {
      if (evalType == PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF) {
        false
      } else {
        evalType == firstVisitedScalarUDFEvalType.get
      }
    }

    def collectEvaluableUDFs(expr: Expression): Seq[PythonUDF] = expr match {
      case udf: PythonUDF if PythonUDF.isScalarPythonUDF(udf) && canEvaluateInPython(udf)
        && firstVisitedScalarUDFEvalType.isEmpty =>
        firstVisitedScalarUDFEvalType = Some(udf.evalType)
        Seq(udf)
      case udf: PythonUDF if PythonUDF.isScalarPythonUDF(udf) && canEvaluateInPython(udf)
        && canChainUDF(udf.evalType) =>
        Seq(udf)
      case e => e.children.flatMap(collectEvaluableUDFs)
    }

    expressions.flatMap(collectEvaluableUDFs)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // SPARK-26293: A subquery will be rewritten into join later, and will go through this rule
    // eventually. Here we skip subquery, as Python UDF only needs to be extracted once.
    case s: Subquery if s.correlated => plan

    case _ => plan.transformUpWithPruning(
      // All cases must contain pattern PYTHON_UDF. PythonUDFs are member fields of BatchEvalPython
      // and ArrowEvalPython.
      _.containsPattern(PYTHON_UDF)) {
      // A safe guard. `ExtractPythonUDFs` only runs once, so we will not hit `BatchEvalPython` and
      // `ArrowEvalPython` in the input plan. However if we hit them, we must skip them, as we can't
      // extract Python UDFs from them.
      case p: BatchEvalPython => p
      case p: ArrowEvalPython => p

      case plan: LogicalPlan => extract(plan)
    }
  }

  private def canonicalizeDeterministic(u: PythonUDF) = {
    if (u.deterministic) {
      u.canonicalized.asInstanceOf[PythonUDF]
    } else {
      u
    }
  }

  private def containsUDT(dataType: DataType): Boolean = dataType match {
    case _: UserDefinedType[_] => true
    case ArrayType(elementType, _) => containsUDT(elementType)
    case StructType(fields) => fields.exists(field => containsUDT(field.dataType))
    case MapType(keyType, valueType, _) => containsUDT(keyType) || containsUDT(valueType)
    case _ => false
  }

  /**
   * Extract all the PythonUDFs from the current operator and evaluate them before the operator.
   */
  private def extract(plan: LogicalPlan): LogicalPlan = {
    val udfs = ExpressionSet(collectEvaluableUDFsFromExpressions(plan.expressions))
      // ignore the PythonUDF that come from second/third aggregate, which is not used
      .filter(udf => udf.references.subsetOf(plan.inputSet))
      .toSeq.asInstanceOf[Seq[PythonUDF]]
    if (udfs.isEmpty) {
      // If there aren't any, we are done.
      plan
    } else {
      val attributeMap = mutable.HashMap[PythonUDF, Expression]()
      // Rewrite the child that has the input required for the UDF
      val newChildren = plan.children.map { child =>
        // Pick the UDF we are going to evaluate
        val validUdfs = udfs.filter { udf =>
          // Check to make sure that the UDF can be evaluated with only the input of this child.
          udf.references.subsetOf(child.outputSet)
        }
        if (validUdfs.nonEmpty) {
          require(
            validUdfs.forall(PythonUDF.isScalarPythonUDF),
            "Can only extract scalar vectorized udf or sql batch udf")

          val resultAttrs = validUdfs.zipWithIndex.map { case (u, i) =>
            AttributeReference(s"pythonUDF$i", u.dataType)()
          }

          val evalTypes = validUdfs.map(_.evalType).toSet
          if (evalTypes.size != 1) {
            throw SparkException.internalError(
              "Expected udfs have the same evalType but got different evalTypes: " +
              evalTypes.mkString(","))
          }
          val evalType = evalTypes.head

          val hasUDTInput = validUdfs.exists(_.children.exists(expr => containsUDT(expr.dataType)))
          val hasUDTReturn = validUdfs.exists(udf => containsUDT(udf.dataType))

          val evaluation = evalType match {
            case PythonEvalType.SQL_BATCHED_UDF =>
              BatchEvalPython(validUdfs, resultAttrs, child)
            case PythonEvalType.SQL_SCALAR_PANDAS_UDF | PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF =>
              ArrowEvalPython(validUdfs, resultAttrs, child, evalType)
            case PythonEvalType.SQL_ARROW_BATCHED_UDF =>

              if (hasUDTInput || hasUDTReturn) {
                // Use BatchEvalPython if UDT is detected
                logWarning(log"Arrow optimization disabled due to " +
                  log"${MDC(REASON, "UDT input or return type")}. " +
                  log"Falling back to non-Arrow-optimized UDF execution.")
                BatchEvalPython(validUdfs, resultAttrs, child)
              } else {
                ArrowEvalPython(validUdfs, resultAttrs, child, evalType)
              }
            case _ =>
              throw SparkException.internalError("Unexpected UDF evalType")
          }

          attributeMap ++= validUdfs.map(canonicalizeDeterministic).zip(resultAttrs)
          evaluation
        } else {
          child
        }
      }
      // Other cases are disallowed as they are ambiguous or would require a cartesian
      // product.
      udfs.map(canonicalizeDeterministic).filterNot(attributeMap.contains).foreach { udf =>
        throw SparkException.internalError(
          s"Invalid PythonUDF $udf, requires attributes from more than one child.")
      }

      val rewritten = plan.withNewChildren(newChildren).transformExpressions {
        case p: PythonUDF => attributeMap.getOrElse(canonicalizeDeterministic(p), p)
      }

      // extract remaining python UDFs recursively
      val newPlan = extract(rewritten)
      if (newPlan.output != plan.output) {
        // Trim away the new UDF value if it was only used for filtering or something.
        Project(plan.output, newPlan)
      } else {
        newPlan
      }
    }
  }
}

/**
 * Extracts PythonUDTFs from operators, rewriting the query plan so that UDTFs can be evaluated.
 */
object ExtractPythonUDTFs extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // A correlated subquery will be rewritten into join later, and will go through this rule
    // eventually. Here we skip subquery, as Python UDTFs only need to be extracted once.
    case s: Subquery if s.correlated => plan

    case _ => plan.transformUpWithPruning(_.containsPattern(GENERATE)) {
      case g @ Generate(func: PythonUDTF, _, _, _, _, child) => func.evalType match {
        case PythonEvalType.SQL_TABLE_UDF =>
          BatchEvalPythonUDTF(func, g.requiredChildOutput, g.generatorOutput, child)
        case PythonEvalType.SQL_ARROW_TABLE_UDF =>
          ArrowEvalPythonUDTF(func, g.requiredChildOutput, g.generatorOutput, child, func.evalType)
      }
    }
  }
}
