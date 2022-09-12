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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.trees.SQLQueryContext
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXTRACT_VALUE, TreePattern}
import org.apache.spark.sql.catalyst.util.{quoteIdentifier, ArrayData, GenericArrayData, MapData, TypeUtils}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines all the expressions to extract values out of complex types.
// For example, getting a field out of an array, map, or struct.
////////////////////////////////////////////////////////////////////////////////////////////////////


object ExtractValue {
  /**
   * Returns the resolved `ExtractValue`. It will return one kind of concrete `ExtractValue`,
   * depend on the type of `child` and `extraction`.
   *
   *   `child`      |    `extraction`    |    concrete `ExtractValue`
   * ----------------------------------------------------------------
   *    Struct      |   Literal String   |        GetStructField
   * Array[Struct]  |   Literal String   |     GetArrayStructFields
   *    Array       |   Integral type    |         GetArrayItem
   *     Map        |   map key type     |         GetMapValue
   */
  def apply(
      child: Expression,
      extraction: Expression,
      resolver: Resolver): Expression = {

    (child.dataType, extraction) match {
      case (StructType(fields), NonNullLiteral(v, StringType)) =>
        val fieldName = v.toString
        val ordinal = findField(fields, fieldName, resolver)
        GetStructField(child, ordinal, Some(fieldName))

      case (ArrayType(StructType(fields), containsNull), NonNullLiteral(v, StringType)) =>
        val fieldName = v.toString
        val ordinal = findField(fields, fieldName, resolver)
        GetArrayStructFields(child, fields(ordinal).copy(name = fieldName),
          ordinal, fields.length, containsNull || fields(ordinal).nullable)

      case (_: ArrayType, _) => GetArrayItem(child, extraction)

      case (MapType(kt, _, _), _) => GetMapValue(child, extraction)

      case (otherType, _) =>
        throw QueryCompilationErrors.dataTypeUnsupportedByExtractValueError(
          otherType, extraction, child)
    }
  }

  /**
   * Find the ordinal of StructField, report error if no desired field or over one
   * desired fields are found.
   */
  private def findField(fields: Array[StructField], fieldName: String, resolver: Resolver): Int = {
    val checkField = (f: StructField) => resolver(f.name, fieldName)
    val ordinal = fields.indexWhere(checkField)
    if (ordinal == -1) {
      throw QueryCompilationErrors.noSuchStructFieldInGivenFieldsError(fieldName, fields)
    } else if (fields.indexWhere(checkField, ordinal + 1) != -1) {
      throw QueryCompilationErrors.ambiguousReferenceToFieldsError(
        fields.filter(checkField).mkString(", "))
    } else {
      ordinal
    }
  }
}

trait ExtractValue extends Expression with NullIntolerant {
  final override val nodePatterns: Seq[TreePattern] = Seq(EXTRACT_VALUE)
}

/**
 * Returns the value of fields in the Struct `child`.
 *
 * No need to do type checking since it is handled by [[ExtractValue]].
 *
 * Note that we can pass in the field name directly to keep case preserving in `toString`.
 * For example, when get field `yEAr` from `<year: int, month: int>`, we should pass in `yEAr`.
 */
case class GetStructField(child: Expression, ordinal: Int, name: Option[String] = None)
  extends UnaryExpression with ExtractValue {

  lazy val childSchema = child.dataType.asInstanceOf[StructType]

  override lazy val canonicalized: Expression = {
    copy(child = child.canonicalized, name = None)
  }

  override def dataType: DataType = childSchema(ordinal).dataType
  override def nullable: Boolean = child.nullable || childSchema(ordinal).nullable

  override def toString: String = {
    val fieldName = if (resolved) childSchema(ordinal).name else s"_$ordinal"
    s"$child.${name.getOrElse(fieldName)}"
  }

  def extractFieldName: String = name.getOrElse(childSchema(ordinal).name)

  override def sql: String =
    child.sql + s".${quoteIdentifier(extractFieldName)}"

  protected override def nullSafeEval(input: Any): Any =
    input.asInstanceOf[InternalRow].get(ordinal, childSchema(ordinal).dataType)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, eval => {
      if (nullable) {
        s"""
          if ($eval.isNullAt($ordinal)) {
            ${ev.isNull} = true;
          } else {
            ${ev.value} = ${CodeGenerator.getValue(eval, dataType, ordinal.toString)};
          }
        """
      } else {
        s"""
          ${ev.value} = ${CodeGenerator.getValue(eval, dataType, ordinal.toString)};
        """
      }
    })
  }

  override protected def withNewChildInternal(newChild: Expression): GetStructField =
    copy(child = newChild)

  def metadata: Metadata = childSchema(ordinal).metadata
}

/**
 * For a child whose data type is an array of structs, extracts the `ordinal`-th fields of all array
 * elements, and returns them as a new array.
 *
 * No need to do type checking since it is handled by [[ExtractValue]].
 */
case class GetArrayStructFields(
    child: Expression,
    field: StructField,
    ordinal: Int,
    numFields: Int,
    containsNull: Boolean) extends UnaryExpression with ExtractValue {

  override def dataType: DataType = ArrayType(field.dataType, containsNull)
  override def toString: String = s"$child.${field.name}"
  override def sql: String = s"${child.sql}.${quoteIdentifier(field.name)}"

  protected override def nullSafeEval(input: Any): Any = {
    val array = input.asInstanceOf[ArrayData]
    val length = array.numElements()
    val result = new Array[Any](length)
    var i = 0
    while (i < length) {
      if (array.isNullAt(i)) {
        result(i) = null
      } else {
        val row = array.getStruct(i, numFields)
        if (row.isNullAt(ordinal)) {
          result(i) = null
        } else {
          result(i) = row.get(ordinal, field.dataType)
        }
      }
      i += 1
    }
    new GenericArrayData(result)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val arrayClass = classOf[GenericArrayData].getName
    nullSafeCodeGen(ctx, ev, eval => {
      val n = ctx.freshName("n")
      val values = ctx.freshName("values")
      val j = ctx.freshName("j")
      val row = ctx.freshName("row")
      val nullSafeEval = if (field.nullable) {
        s"""
         if ($row.isNullAt($ordinal)) {
           $values[$j] = null;
         } else
        """
      } else {
        ""
      }

      s"""
        final int $n = $eval.numElements();
        final Object[] $values = new Object[$n];
        for (int $j = 0; $j < $n; $j++) {
          if ($eval.isNullAt($j)) {
            $values[$j] = null;
          } else {
            final InternalRow $row = $eval.getStruct($j, $numFields);
            $nullSafeEval {
              $values[$j] = ${CodeGenerator.getValue(row, field.dataType, ordinal.toString)};
            }
          }
        }
        ${ev.value} = new $arrayClass($values);
      """
    })
  }

  override protected def withNewChildInternal(newChild: Expression): GetArrayStructFields =
    copy(child = newChild)
}

/**
 * Returns the field at `ordinal` in the Array `child`.
 *
 * We need to do type checking here as `ordinal` expression maybe unresolved.
 */
case class GetArrayItem(
    child: Expression,
    ordinal: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled) extends BinaryExpression
  with GetArrayItemUtil
  with ExpectsInputTypes
  with ExtractValue
  with SupportQueryContext {

  // We have done type checking for child in `ExtractValue`, so only need to check the `ordinal`.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, IntegralType)

  override def toString: String = s"$child[$ordinal]"
  override def sql: String = s"${child.sql}[${ordinal.sql}]"

  override def left: Expression = child
  override def right: Expression = ordinal
  override def nullable: Boolean =
    computeNullabilityFromArray(left, right, failOnError, nullability)
  override def dataType: DataType = child.dataType.asInstanceOf[ArrayType].elementType

  private def nullability(elements: Seq[Expression], ordinal: Int): Boolean = {
    if (ordinal >= 0 && ordinal < elements.length) {
      elements(ordinal).nullable
    } else {
      !failOnError
    }
  }

  protected override def nullSafeEval(value: Any, ordinal: Any): Any = {
    val baseValue = value.asInstanceOf[ArrayData]
    val index = ordinal.asInstanceOf[Number].intValue()
    if (index >= baseValue.numElements() || index < 0) {
      if (failOnError) {
        throw QueryExecutionErrors.invalidArrayIndexError(
          index, baseValue.numElements, getContextOrNull())
      } else {
        null
      }
    } else if (baseValue.isNullAt(index)) {
      null
    } else {
      baseValue.get(index, dataType)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      val index = ctx.freshName("index")
      val childArrayElementNullable = child.dataType.asInstanceOf[ArrayType].containsNull
      val nullCheck = if (childArrayElementNullable) {
        s"""else if ($eval1.isNullAt($index)) {
               ${ev.isNull} = true;
            }
         """
      } else {
        ""
      }

      val indexOutOfBoundBranch = if (failOnError) {
        val errorContext = getContextOrNullCode(ctx)
        // scalastyle:off line.size.limit
        s"throw QueryExecutionErrors.invalidArrayIndexError($index, $eval1.numElements(), $errorContext);"
        // scalastyle:on line.size.limit
      } else {
        s"${ev.isNull} = true;"
      }

      s"""
        final int $index = (int) $eval2;
        if ($index >= $eval1.numElements() || $index < 0) {
          $indexOutOfBoundBranch
        } $nullCheck else {
          ${ev.value} = ${CodeGenerator.getValue(eval1, dataType, index)};
        }
      """
    })
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): GetArrayItem =
    copy(child = newLeft, ordinal = newRight)

  override def initQueryContext(): Option[SQLQueryContext] = if (failOnError) {
    Some(origin.context)
  } else {
    None
  }
}

/**
 * Common trait for [[GetArrayItem]] and [[ElementAt]].
 */
trait GetArrayItemUtil {

  /** `Null` is returned for invalid ordinals. */
  protected def computeNullabilityFromArray(
      child: Expression,
      ordinal: Expression,
      failOnError: Boolean,
      nullability: (Seq[Expression], Int) => Boolean): Boolean = {
    val arrayElementNullable = child.dataType.asInstanceOf[ArrayType].containsNull
    if (ordinal.foldable && !ordinal.nullable) {
      val intOrdinal = ordinal.eval().asInstanceOf[Number].intValue()
      child match {
        case CreateArray(ar, _) =>
          nullability(ar, intOrdinal)
        case GetArrayStructFields(CreateArray(elements, _), field, _, _, _) =>
          nullability(elements, intOrdinal) || field.nullable
        case _ =>
          true
      }
    } else {
      if (failOnError) arrayElementNullable else true
    }
  }
}

/**
 * Common trait for [[GetMapValue]] and [[ElementAt]].
 */
trait GetMapValueUtil extends BinaryExpression with ImplicitCastInputTypes {

  // todo: current search is O(n), improve it.
  def getValueEval(
      value: Any,
      ordinal: Any,
      keyType: DataType,
      ordering: Ordering[Any]): Any = {
    val map = value.asInstanceOf[MapData]
    val length = map.numElements()
    val keys = map.keyArray()
    val values = map.valueArray()

    var i = 0
    var found = false
    while (i < length && !found) {
      if (ordering.equiv(keys.get(i, keyType), ordinal)) {
        found = true
      } else {
        i += 1
      }
    }

    if (!found || values.isNullAt(i)) {
      null
    } else {
      values.get(i, dataType)
    }
  }

  def doGetValueGenCode(
      ctx: CodegenContext,
      ev: ExprCode,
      mapType: MapType): ExprCode = {
    val index = ctx.freshName("index")
    val length = ctx.freshName("length")
    val keys = ctx.freshName("keys")
    val key = ctx.freshName("key")
    val values = ctx.freshName("values")
    val keyType = mapType.keyType
    val nullCheck = if (mapType.valueContainsNull) {
      s" || $values.isNullAt($index)"
    } else {
      ""
    }

    val keyJavaType = CodeGenerator.javaType(keyType)
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      s"""
        final int $length = $eval1.numElements();
        final ArrayData $keys = $eval1.keyArray();
        final ArrayData $values = $eval1.valueArray();

        int $index = 0;
        while ($index < $length) {
          final $keyJavaType $key = ${CodeGenerator.getValue(keys, keyType, index)};
          if (${ctx.genEqual(keyType, key, eval2)}) {
            break;
          } else {
            $index++;
          }
        }

        if ($index == $length$nullCheck) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = ${CodeGenerator.getValue(values, dataType, index)};
        }
      """
    })
  }
}

/**
 * Returns the value of key `key` in Map `child`.
 *
 * We need to do type checking here as `key` expression maybe unresolved.
 */
case class GetMapValue(child: Expression, key: Expression)
  extends GetMapValueUtil with ExtractValue {

  @transient private lazy val ordering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(keyType)

  private[catalyst] def keyType = child.dataType.asInstanceOf[MapType].keyType

  override def checkInputDataTypes(): TypeCheckResult = {
    super.checkInputDataTypes() match {
      case f: TypeCheckResult.TypeCheckFailure => f
      case TypeCheckResult.TypeCheckSuccess =>
        TypeUtils.checkForOrderingExpr(keyType, s"function $prettyName")
    }
  }

  // We have done type checking for child in `ExtractValue`, so only need to check the `key`.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, keyType)

  override def toString: String = s"$child[$key]"
  override def sql: String = s"${child.sql}[${key.sql}]"

  override def left: Expression = child
  override def right: Expression = key

  /**
   * `Null` is returned for invalid ordinals.
   *
   * TODO: We could make nullability more precise in foldable cases (e.g., literal input).
   * But, since the key search is O(n), it takes much time to compute nullability.
   * If we find efficient key searches, revisit this.
   */
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType.asInstanceOf[MapType].valueType

  // todo: current search is O(n), improve it.
  override def nullSafeEval(value: Any, ordinal: Any): Any = {
    getValueEval(value, ordinal, keyType, ordering)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    doGetValueGenCode(ctx, ev, child.dataType.asInstanceOf[MapType])
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): GetMapValue =
    copy(child = newLeft, key = newRight)
}
