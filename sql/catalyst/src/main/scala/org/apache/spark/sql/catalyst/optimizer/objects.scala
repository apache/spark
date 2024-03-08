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

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkException
import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType, UserDefinedType}

/*
 * This file defines optimization rules related to object manipulation (for the Dataset API).
 */

/**
 * Removes cases where we are unnecessarily going between the object and serialized (InternalRow)
 * representation of data item.  For example back to back map operations.
 */
object EliminateSerialization extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAnyPattern(DESERIALIZE_TO_OBJECT, APPEND_COLUMNS, TYPED_FILTER), ruleId) {
    case d @ DeserializeToObject(_, _, s: SerializeFromObject)
      if d.outputObjAttr.dataType == s.inputObjAttr.dataType =>
      // Adds an extra Project here, to preserve the output expr id of `DeserializeToObject`.
      // We will remove it later in RemoveAliasOnlyProject rule.
      val objAttr = Alias(s.inputObjAttr, s.inputObjAttr.name)(exprId = d.outputObjAttr.exprId)
      Project(objAttr :: Nil, s.child)

    case a @ AppendColumns(_, _, _, _, _, s: SerializeFromObject)
      if a.deserializer.dataType == s.inputObjAttr.dataType =>
      AppendColumnsWithObject(a.func, s.serializer, a.serializer, s.child)

    // If there is a `SerializeFromObject` under typed filter and its input object type is same with
    // the typed filter's deserializer, we can convert typed filter to normal filter without
    // deserialization in condition, and push it down through `SerializeFromObject`.
    // e.g. `ds.map(...).filter(...)` can be optimized by this rule to save extra deserialization,
    // but `ds.map(...).as[AnotherType].filter(...)` can not be optimized.
    case f @ TypedFilter(_, _, _, _, s: SerializeFromObject)
      if f.deserializer.dataType == s.inputObjAttr.dataType =>
      s.copy(child = f.withObjectProducerChild(s.child))

    // If there is a `DeserializeToObject` upon typed filter and its output object type is same with
    // the typed filter's deserializer, we can convert typed filter to normal filter without
    // deserialization in condition, and pull it up through `DeserializeToObject`.
    // e.g. `ds.filter(...).map(...)` can be optimized by this rule to save extra deserialization,
    // but `ds.filter(...).as[AnotherType].map(...)` can not be optimized.
    case d @ DeserializeToObject(_, _, f: TypedFilter)
      if d.outputObjAttr.dataType == f.deserializer.dataType =>
      f.withObjectProducerChild(d.copy(child = f.child))
  }
}

/**
 * Combines two adjacent [[TypedFilter]]s, which operate on same type object in condition, into one,
 * merging the filter functions into one conjunctive function.
 */
object CombineTypedFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsPattern(TYPED_FILTER), ruleId) {
    case t1 @ TypedFilter(_, _, _, _, t2 @ TypedFilter(_, _, _, _, child))
        if t1.deserializer.dataType == t2.deserializer.dataType =>
      TypedFilter(
        combineFilterFunction(t2.func, t1.func),
        t1.argumentClass,
        t1.argumentSchema,
        t1.deserializer,
        child)
  }

  private def combineFilterFunction(func1: AnyRef, func2: AnyRef): Any => Boolean = {
    (func1, func2) match {
      case (f1: FilterFunction[_], f2: FilterFunction[_]) =>
        input => f1.asInstanceOf[FilterFunction[Any]].call(input) &&
          f2.asInstanceOf[FilterFunction[Any]].call(input)
      case (f1: FilterFunction[_], f2) =>
        input => f1.asInstanceOf[FilterFunction[Any]].call(input) &&
          f2.asInstanceOf[Any => Boolean](input)
      case (f1, f2: FilterFunction[_]) =>
        input => f1.asInstanceOf[Any => Boolean].apply(input) &&
          f2.asInstanceOf[FilterFunction[Any]].call(input)
      case (f1, f2) =>
        input => f1.asInstanceOf[Any => Boolean].apply(input) &&
          f2.asInstanceOf[Any => Boolean].apply(input)
    }
  }
}

/**
 * Removes MapObjects when the following conditions are satisfied
 *   1. Mapobject(... lambdavariable(..., false) ...), which means types for input and output
 *      are primitive types with non-nullable
 *   2. no custom collection class specified representation of data item.
 */
object EliminateMapObjects extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressionsWithPruning(
    _.containsAllPatterns(MAP_OBJECTS, LAMBDA_VARIABLE), ruleId) {
     case MapObjects(_, LambdaVariable(_, _, false, _), inputData, None) => inputData
  }
}

/**
 * Prunes unnecessary object serializers from query plan. This rule prunes both individual
 * serializer and nested fields in serializers.
 */
object ObjectSerializerPruning extends Rule[LogicalPlan] {

  /**
   * Visible for testing.
   * Collects all struct types from given data type object, recursively.
   */
  def collectStructType(dt: DataType, structs: ArrayBuffer[StructType]): ArrayBuffer[StructType] = {
    dt match {
      case s @ StructType(fields) =>
        structs += s
        fields.map(f => collectStructType(f.dataType, structs))
      case ArrayType(elementType, _) =>
        collectStructType(elementType, structs)
      case MapType(keyType, valueType, _) =>
        collectStructType(keyType, structs)
        collectStructType(valueType, structs)
      // We don't use UserDefinedType in those serializers.
      case _: UserDefinedType[_] =>
      case _ =>
    }
    structs
  }

  /**
   * This method returns pruned `CreateNamedStruct` expression given an original `CreateNamedStruct`
   * and a pruned `StructType`.
   */
  private def pruneNamedStruct(struct: CreateNamedStruct, prunedType: StructType) = {
    // Filters out the pruned fields.
    val resolver = conf.resolver
    val prunedFields = struct.nameExprs.zip(struct.valExprs).filter { case (nameExpr, _) =>
      val name = nameExpr.eval(EmptyRow).toString
      prunedType.fieldNames.exists(resolver(_, name))
    }.flatMap(pair => Seq(pair._1, pair._2))

    CreateNamedStruct(prunedFields)
  }

  /**
   * When we change nested serializer data type, `If` expression will be unresolved because
   * literal null's data type doesn't match now. We need to align it with new data type.
   * Note: we should do `transformUp` explicitly to change data types.
   */
  private def alignNullTypeInIf(expr: Expression) = expr.transformUp {
    case i @ If(IsNullCondition(), Literal(null, dt), ser)
      if !DataTypeUtils.sameType(dt, ser.dataType) =>
      i.copy(trueValue = Literal(null, ser.dataType))
  }

  object IsNullCondition {
    def unapply(expr: Expression): Boolean = expr match {
      case _: IsNull => true
      case i: Invoke if i.functionName == "isNullAt" => true
      case _ => false
    }
  }

  /**
   * This method prunes given serializer expression by given pruned data type. For example,
   * given a serializer creating struct(a int, b int) and pruned data type struct(a int),
   * this method returns pruned serializer creating struct(a int).
   */
  def pruneSerializer(
      serializer: NamedExpression,
      prunedDataType: DataType): NamedExpression = {
    val prunedStructTypes = collectStructType(prunedDataType, ArrayBuffer.empty[StructType])
      .iterator

    def transformer: PartialFunction[Expression, Expression] = {
      case m: ExternalMapToCatalyst =>
        val prunedKeyConverter = m.keyConverter.transformDown(transformer)
        val prunedValueConverter = m.valueConverter.transformDown(transformer)

        m.copy(keyConverter = alignNullTypeInIf(prunedKeyConverter),
          valueConverter = alignNullTypeInIf(prunedValueConverter))

      case s: CreateNamedStruct if prunedStructTypes.hasNext =>
        val prunedType = prunedStructTypes.next()
        pruneNamedStruct(s, prunedType)
    }

    val transformedSerializer = serializer.transformDown(transformer)
    val prunedSerializer = alignNullTypeInIf(transformedSerializer).asInstanceOf[NamedExpression]

    if (DataTypeUtils.sameType(prunedSerializer.dataType, prunedDataType)) {
      prunedSerializer
    } else {
      serializer
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAllPatterns(SERIALIZE_FROM_OBJECT, PROJECT), ruleId) {
    case p @ Project(_, s: SerializeFromObject) =>
      // Prunes individual serializer if it is not used at all by above projection.
      val usedRefs = p.references
      val prunedSerializer = s.serializer.filter(usedRefs.contains)

      val rootFields = SchemaPruning.identifyRootFields(p.projectList, Seq.empty)

      if (conf.serializerNestedSchemaPruningEnabled && rootFields.nonEmpty) {
        // Prunes nested fields in serializers.
        val prunedSchema = SchemaPruning.pruneSchema(
          DataTypeUtils.fromAttributes(prunedSerializer.map(_.toAttribute)), rootFields)
        val nestedPrunedSerializer = prunedSerializer.zipWithIndex.map { case (serializer, idx) =>
          pruneSerializer(serializer, prunedSchema(idx).dataType)
        }

        // Builds new projection.
        val projectionOverSchema = ProjectionOverSchema(prunedSchema, AttributeSet(s.output))
        val newProjects = p.projectList.map(_.transformDown {
          case projectionOverSchema(expr) => expr
        }).map { case expr: NamedExpression => expr }
        p.copy(projectList = newProjects,
          child = SerializeFromObject(nestedPrunedSerializer, s.child))
      } else {
        p.copy(child = SerializeFromObject(prunedSerializer, s.child))
      }
  }
}

/**
 * Reassigns per-query unique IDs to `LambdaVariable`s, whose original IDs are globally unique. This
 * can help Spark to hit codegen cache more often and improve performance.
 */
object ReassignLambdaVariableID extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    // The original LambdaVariable IDs are all positive. To avoid conflicts, the new IDs are all
    // negative and starts from -1.
    var newId = 0L
    val oldIdToNewId = scala.collection.mutable.Map.empty[Long, Long]

    // The `LambdaVariable` IDs in a query should be all positive or negative. Otherwise it's a bug
    // and we should fail earlier.
    var hasNegativeIds = false
    var hasPositiveIds = false

    plan.transformAllExpressionsWithPruning(_.containsPattern(LAMBDA_VARIABLE), ruleId) {
      case lr: LambdaVariable if lr.id == 0 =>
        throw SparkException.internalError("LambdaVariable should never has 0 as its ID.")

      case lr: LambdaVariable if lr.id < 0 =>
        hasNegativeIds = true
        if (hasPositiveIds) {
          throw SparkException.internalError(
            "LambdaVariable IDs in a query should be all positive or negative.")

        }
        lr

      case lr: LambdaVariable if lr.id > 0 =>
        hasPositiveIds = true
        if (hasNegativeIds) {
          throw SparkException.internalError(
            "LambdaVariable IDs in a query should be all positive or negative.")
        }

        if (oldIdToNewId.contains(lr.id)) {
          // This `LambdaVariable` has appeared before, reuse the newly generated ID.
          lr.copy(id = oldIdToNewId(lr.id))
        } else {
          // This is the first appearance of this `LambdaVariable`, generate a new ID.
          newId -= 1
          oldIdToNewId(lr.id) = newId
          lr.copy(id = newId)
        }
    }
  }
}
