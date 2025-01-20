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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.analysis.CollationStrength.{Default, Explicit, Implicit, Indeterminate}
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.haveSameType
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Project}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLExpr
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{ArrayType, DataType, IndeterminateStringType, MapType, NullType, StringType, StructType}
import org.apache.spark.sql.util.SchemaUtils

/**
 * Type coercion helper that matches against expressions in order to apply collation type coercion.
 */
object CollationTypeCoercion {

  private val COLLATION_CONTEXT_TAG = new TreeNodeTag[DataType]("collationContext")

  private def hasCollationContextTag(expr: Expression): Boolean = {
    expr.getTagValue(COLLATION_CONTEXT_TAG).isDefined
  }

  def apply(expression: Expression): Expression = expression match {
    case expr if shouldFailWithIndeterminateCollation(expr) =>
      expr.failAnalysis(
        errorClass = "INDETERMINATE_COLLATION_IN_EXPRESSION",
        messageParameters = Map("expr" -> toSQLExpr(expr)))

    case caseWhenExpr: CaseWhen if !haveSameType(caseWhenExpr.inputTypesForMerging) =>
      val outputStringType = findLeastCommonStringType(
        caseWhenExpr.branches.map(_._2) ++ caseWhenExpr.elseValue)
      outputStringType match {
        case Some(st) =>
          val newBranches = caseWhenExpr.branches.map { case (condition, value) =>
            (condition, changeType(value, st))
          }
          val newElseValue =
            caseWhenExpr.elseValue.map(e => changeType(e, st))
          CaseWhen(newBranches, newElseValue)

        case _ =>
          caseWhenExpr
      }

    case mapCreate: CreateMap if mapCreate.children.size % 2 == 0 =>
      // We only take in mapCreate if it has even number of children, as otherwise it should fail
      // with wrong number of arguments
      val newKeys = collateToSingleType(mapCreate, mapCreate.keys)
      val newValues = collateToSingleType(mapCreate, mapCreate.values)
      mapCreate.withNewChildren(newKeys.zip(newValues).flatMap(pair => Seq(pair._1, pair._2)))

    case namedStruct: CreateNamedStruct =>
      // since each child is separate we should not coerce them at all
      namedStruct

    case getMap @ GetMapValue(child, key) if getMap.keyType != key.dataType =>
      key match {
        case Literal(_, _: StringType) =>
          GetMapValue(child, Cast(key, getMap.keyType))
        case _ =>
          getMap
      }

    case otherExpr @ (_: In | _: InSubquery | _: CreateArray | _: ArrayJoin | _: Concat |
        _: Greatest | _: Least | _: Coalesce | _: ArrayContains | _: ArrayExcept | _: ConcatWs |
        _: Mask | _: StringReplace | _: StringTranslate | _: StringTrim | _: StringTrimLeft |
        _: StringTrimRight | _: ArrayAppend | _: ArrayIntersect | _: ArrayPosition |
        _: ArrayRemove | _: ArrayUnion | _: ArraysOverlap | _: Contains | _: EndsWith |
        _: EqualNullSafe | _: EqualTo | _: FindInSet | _: GreaterThan | _: GreaterThanOrEqual |
        _: LessThan | _: LessThanOrEqual | _: StartsWith | _: StringInstr | _: ToNumber |
        _: TryToNumber | _: StringToMap | _: Levenshtein  | _: StringSplitSQL | _: SplitPart |
        _: Lag | _: Lead | _: RegExpReplace | _: StringRPad | _: StringLPad | _: Overlay |
        _: Elt | _: SubstringIndex | _: StringLocate | _: If) =>
      val newChildren = collateToSingleType(otherExpr, otherExpr.children)
      otherExpr.withNewChildren(newChildren)

    case other => other
  }

  /**
   * Returns true if the given data type has any StringType in it.
   */
  private def hasStringType(dt: DataType): Boolean = dt.existsRecursively {
    case _: StringType => true
    case _ => false
  }

  /**
   * Changes the data type of the expression to the given `newType`.
   */
  private def changeType(expr: Expression, newType: DataType): Expression = {
    mergeTypes(expr.dataType, newType) match {
      case Some(newDataType) if newDataType != expr.dataType =>
        assert(!newDataType.existsRecursively(_.isInstanceOf[StringTypeWithContext]))

        expr match {
          case lit: Literal => lit.copy(dataType = newDataType)
          case cast: Cast => cast.copy(dataType = newDataType)
          case subquery: SubqueryExpression =>
            changeTypeInSubquery(subquery, newType)

          case _ => Cast(expr, newDataType)
        }

      case _ =>
        expr
    }
  }

  /**
   * Changes the data type of the expression in the subquery to the given `newType`.
   * Currently only supports subqueries with [[Project]] and [[Aggregate]] plan.
   */
  private def changeTypeInSubquery(
      subqueryExpression: SubqueryExpression,
      newType: DataType): SubqueryExpression = {

    def transformNamedExpressions(ex: NamedExpression): NamedExpression = {
      changeType(ex, newType) match {
        case named: NamedExpression => named
        case other => Alias(other, ex.name)()
      }
    }

    val newPlan = subqueryExpression.plan match {
      case project: Project =>
        val newProjectList = project.projectList.map(transformNamedExpressions)
        project.copy(projectList = newProjectList)

      case agg: Aggregate =>
        val newAggregateExpressions = agg.aggregateExpressions.map(transformNamedExpressions)
        agg.copy(aggregateExpressions = newAggregateExpressions)

      case other => other
    }

    subqueryExpression.withNewPlan(newPlan)
  }

  /**
   * If possible, returns the new data type from `inType` by applying
   * the collation of `castType`.
   */
  private def mergeTypes(inType: DataType, castType: DataType): Option[DataType] = {
    val outType = mergeStructurally(inType, castType) {
      case (_: StringType, right: StringTypeWithContext) =>
        right.stringType
    }

    outType
  }

  /**
   * Merges two data types structurally according to the given base case.
   */
  private def mergeStructurally(
      leftType: DataType,
      rightType: DataType)
      (baseCase: PartialFunction[(DataType, DataType), DataType]): Option[DataType] = {
    (leftType, rightType) match {

      // handle the base cases first
      case _ if baseCase.isDefinedAt((leftType, rightType)) =>
        Option(baseCase(leftType, rightType))

      case _ if leftType == rightType =>
        Some(leftType)

      case (ArrayType(leftElemType, nullable), ArrayType(rightElemType, _)) =>
        mergeStructurally(leftElemType, rightElemType)(baseCase).map(ArrayType(_, nullable))

      case (MapType(leftKey, leftValue, nullable), MapType(rightKey, rightValue, _)) =>
        for {
          newKeyType <- mergeStructurally(leftKey, rightKey)(baseCase)
          newValueType <- mergeStructurally(leftValue, rightValue)(baseCase)
        } yield MapType(newKeyType, newValueType, nullable)

      case (ArrayType(elementType, nullable), right) =>
        mergeStructurally(elementType, right)(baseCase).map(ArrayType(_, nullable))

      case (left, ArrayType(elementType, _)) =>
        mergeStructurally(left, elementType)(baseCase)

      case (StructType(leftFields), StructType(rightFields)) =>
        if (leftFields.length != rightFields.length) {
          return None
        }
        val newFields = leftFields.zip(rightFields).map {
          case (leftField, rightField) =>
            val newType = mergeStructurally(leftField.dataType, rightField.dataType)(baseCase)
            if (newType.isEmpty) {
              return None
            }
            leftField.copy(dataType = newType.get)
        }
        Some(StructType(newFields))

      case _ => None
    }
  }

  /**
   * Collates input expressions to a single collation.
   */
  def collateToSingleType(expression: Expression, children: Seq[Expression]): Seq[Expression] = {
    val stringExpressions = children.filter(e => hasStringType(e.dataType))
    val lctOpt = findLeastCommonStringType(stringExpressions)

    lctOpt match {
      case Some(lct) =>
        checkIndeterminateCollation(expression, lct)

        children.map {
          case e if hasStringType(e.dataType) =>
            changeType(e, lct)

          case e => e
        }
      case _ =>
        children
    }
  }

  /**
   * Tries to find the least common StringType among the given expressions.
   */
  private def findLeastCommonStringType(expressions: Seq[Expression]): Option[DataType] = {
    if (!expressions.exists(e => SchemaUtils.hasNonUTF8BinaryCollation(e.dataType))) {
      // if there are no collated types we don't need to do anything
      return None
    } else if (ResolveDefaultStringTypes.needsResolution(expressions)) {
      // if any of the strings types are still not resolved
      // we need to wait for them to be resolved first
      return None
    }

    val collationContextWinner = expressions.foldLeft(findCollationContext(expressions.head)) {
      case (Some(left), right) =>
        findCollationContext(right).flatMap { ctx =>
          mergeWinner(left, ctx)
        }
      case (None, _) => None
    }
    collationContextWinner
  }

  /**
   * Tries to find the data type with the collation context for the given expression.
   * If found, it will also set the [[COLLATION_CONTEXT_TAG]] on the expression,
   * so that the context can be reused later.
   */
  private def findCollationContext(expr: Expression): Option[DataType] = {
    val contextOpt = expr match {

      case _ if collationStrengthBaseCases.isDefinedAt(expr) =>
        collationStrengthBaseCases(expr)

      case getStruct: GetStructField =>
        val childContext = findCollationContext(getStruct.child)
        childContext match {
          case Some(struct: StructType) =>
            val field = struct.fields(getStruct.ordinal)
            Some(field.dataType)
          case _ => None
        }

      case getMapValue: GetMapValue =>
        findCollationContext(getMapValue.child) match {
          case Some(MapType(_, valueType, _)) =>
            mergeWinner(getMapValue.dataType, valueType)
          case _ =>
            None
        }

      case struct: CreateNamedStruct =>
        val childrenContexts = struct.valExprs.map(findCollationContext)
        if (childrenContexts.isEmpty) {
          return None
        }
        val newFields = struct.dataType.fields.zip(childrenContexts).map {
          case (field, Some(context)) =>
            field.copy(dataType = context)
          case (field, None) => field
        }
        Some(StructType(newFields))

      case map: CreateMap =>
        val keyContexts = map.keys.flatMap(findCollationContext)
        val valueContexts = map.values.flatMap(findCollationContext)
        if (keyContexts.length + valueContexts.length != map.children.length) {
          return None
        }

        val keyContextWinner = mergeWinners(map.dataType.keyType, keyContexts)
        val valueContextWinner = mergeWinners(map.dataType.valueType, valueContexts)
        if (keyContextWinner.isEmpty || valueContextWinner.isEmpty) {
          return None
        }
        Some(MapType(keyContextWinner.get, valueContextWinner.get))

      case _ =>
        val childContexts = expr.children.flatMap(findCollationContext)
        mergeWinners(expr.dataType, childContexts)
    }

    contextOpt.foreach(expr.setTagValue(COLLATION_CONTEXT_TAG, _))
    contextOpt
  }

  /**
   * Base cases for determining the strength of the collation.
   */
  private def collationStrengthBaseCases: PartialFunction[Expression, Option[DataType]] = {
    case expr if hasCollationContextTag(expr) =>
      Some(expr.getTagValue(COLLATION_CONTEXT_TAG).get)

    // if `expr` doesn't have a string in its dataType then it doesn't
    // have the collation context either
    case expr if !expr.dataType.existsRecursively(_.isInstanceOf[StringType]) =>
      None

    case collate: Collate =>
      Some(addContextToStringType(collate.dataType, Explicit))

    case cast: Cast =>
      val castStrength = if (hasStringType(cast.child.dataType)) {
        Implicit
      } else {
        Default
      }

      Some(addContextToStringType(cast.dataType, castStrength))

    case expr @ (_: NamedExpression | _: SubqueryExpression | _: VariableReference) =>
      Some(addContextToStringType(expr.dataType, Implicit))

    case lit: Literal =>
      Some(addContextToStringType(lit.dataType, Default))

    // if it does have a string type but none of its children do
    // then the collation context strength is default
    case expr if !expr.children.exists(_.dataType.existsRecursively(_.isInstanceOf[StringType])) =>
      Some(addContextToStringType(expr.dataType, Default))
  }

  /**
   * Adds collation context to the given string type so we can know its strength.
   */
  private def addContextToStringType(dt: DataType, strength: CollationStrength): DataType = {
    dt.transformRecursively {
      case st: StringType => StringTypeWithContext(st, strength)
    }
  }

  /**
   * Merges multiple data types structurally according to strength of the collations into the
   * data type of the `start`.
   *
   * If any of the data types cannot be merged, it returns None.
   */
  private def mergeWinners(start: DataType, rest: Seq[DataType]): Option[DataType] = {
    rest.foldLeft(Option(start)) {
      case (Some(acc), childContext) =>
        mergeWinner(acc, childContext)
      case (None, _) =>
        None
    }
  }

  /**
   * Merges two data types structurally according to strength of the collations.
   */
  private def mergeWinner(left: DataType, right: DataType): Option[DataType] = {
    mergeStructurally(left, right) {
      case (left: StringTypeWithContext, right: StringTypeWithContext) =>
        getWinningStringType(left, right)

      case (_: StringType, right: StringTypeWithContext) =>
        right
    }
  }

  /** Determines the winning StringTypeWithContext based on the strength of the collation. */
  private def getWinningStringType(
      left: StringTypeWithContext,
      right: StringTypeWithContext): StringTypeWithContext = {
    def handleMismatch(): StringTypeWithContext = {
      if (left.strength == Explicit) {
        throw QueryCompilationErrors.explicitCollationMismatchError(
          Seq(left.stringType, right.stringType))
      } else {
        StringTypeWithContext(IndeterminateStringType, Indeterminate)
      }
    }

    (left.strength.priority, right.strength.priority) match {
      case (leftPriority, rightPriority) if leftPriority == rightPriority =>
        if (left.sameType(right)) left
        else handleMismatch()

      case (leftPriority, rightPriority) =>
        if (leftPriority < rightPriority) left
        else right
    }
  }

  /**
   * Throws an analysis exception if the new data type has indeterminate collation,
   * and the expression is not allowed to have inputs with indeterminate collations.
   */
  private def checkIndeterminateCollation(expression: Expression, newDataType: DataType): Unit = {
    if (shouldFailWithIndeterminateCollation(expression, newDataType)) {
      expression.failAnalysis(
        errorClass = "INDETERMINATE_COLLATION_IN_EXPRESSION",
        messageParameters = Map("expr" -> toSQLExpr(expression)))
    }
  }

  /**
   * Returns whether the given expression has indeterminate collation in case it isn't allowed
   * to have inputs with indeterminate collations, and thus should fail.
   */
  private def shouldFailWithIndeterminateCollation(expression: Expression): Boolean = {
    def getDataTypeSafe(e: Expression): DataType = try {
      e.dataType
    } catch {
      case _: Throwable => NullType
    }

    expression.children.exists(child =>
      shouldFailWithIndeterminateCollation(expression, getDataTypeSafe(child)))
  }

  /**
   * Returns whether the given expression should fail with indeterminate collation if it is cast
   * to the given data type.
   */
  private def shouldFailWithIndeterminateCollation(
      expression: Expression,
      dataType: DataType): Boolean = {
    !canContainIndeterminateCollation(expression) && hasIndeterminateCollation(dataType)
  }

  /**
   * Returns whether the given data type has indeterminate collation.
   */
  private def hasIndeterminateCollation(dataType: DataType): Boolean = {
    dataType.existsRecursively {
      case IndeterminateStringType | StringTypeWithContext(_, Indeterminate) => true
      case _ => false
    }
  }

  /**
   * Returns whether the given expression can contain indeterminate collation.
   */
  private def canContainIndeterminateCollation(expr: Expression): Boolean = expr match {
    // This is not an exhaustive list, and it's fine to miss some expressions. The only difference
    // is that those will fail at runtime once we try to fetch the collator/comparison fn.
    case _: BinaryComparison | _: StringPredicate | _: Upper | _: Lower | _: InitCap |
         _: FindInSet | _: StringInstr | _: StringReplace | _: StringLocate | _: SubstringIndex |
         _: StringTrim | _: StringTrimLeft | _: StringTrimRight | _: StringTranslate |
         _: StringSplitSQL | _: In | _: InSubquery | _: FindInSet => false
    case _ => true
  }
}

/**
 * Represents the strength of collation used for determining precedence in collation resolution.
 */
private sealed trait CollationStrength {
  val priority: Int
}

  private object CollationStrength {
  case object Explicit extends CollationStrength {
    override val priority: Int = 0
  }
  case object Indeterminate extends CollationStrength {
    override val priority: Int = 1
  }
  case object Implicit extends CollationStrength {
    override val priority: Int = 2
  }
  case object Default extends CollationStrength {
    override val priority: Int = 3
  }
}

/**
 * Encapsulates the context for collation, including data type and strength.
 *
 * @param stringType StringType.
 * @param strength The strength level of the collation, which determines its precedence.
 */
private case class StringTypeWithContext(stringType: StringType, strength: CollationStrength)
  extends DataType {

  override def defaultSize: Int = stringType.defaultSize

  override private[spark] def asNullable: DataType = this
}
