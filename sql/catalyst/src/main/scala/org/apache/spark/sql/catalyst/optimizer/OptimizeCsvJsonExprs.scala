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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CREATE_NAMED_STRUCT, CSV_TO_STRUCT, EXTRACT_VALUE, JSON_TO_STRUCT}
import org.apache.spark.sql.types.{ArrayType, StructType}

/**
 * Simplify redundant csv/json related expressions.
 *
 * The optimization includes:
 * 1. JsonToStructs(StructsToJson(child)) => child.
 * 2. Prune unnecessary columns from GetStructField/GetArrayStructFields + JsonToStructs.
 * 3. CreateNamedStruct(JsonToStructs(json).col1, JsonToStructs(json).col2, ...) =>
 *      If(IsNull(json), nullStruct, KnownNotNull(JsonToStructs(prunedSchema, ..., json)))
 *      if JsonToStructs(json) is shared among all fields of CreateNamedStruct. `prunedSchema`
 *      contains all accessed fields in original CreateNamedStruct.
 * 4. Prune unnecessary columns from GetStructField + CsvToStructs.
 * 5. CsvToStructs(StructsToCsv(child)) => child.
 */
object OptimizeCsvJsonExprs extends Rule[LogicalPlan] {
  private def nameOfCorruptRecord = conf.columnNameOfCorruptRecord

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAnyPattern(CREATE_NAMED_STRUCT, CSV_TO_STRUCT,
      EXTRACT_VALUE, JSON_TO_STRUCT), ruleId) {
    case p =>
      val optimized = if (conf.jsonExpressionOptimization) {
        p.transformExpressionsWithPruning(
          _.containsAnyPattern(CREATE_NAMED_STRUCT, EXTRACT_VALUE, JSON_TO_STRUCT)
          )(jsonOptimization)
      } else {
        p
      }

      if (conf.csvExpressionOptimization) {
        optimized.transformExpressionsWithPruning(
          _.containsAnyPattern(EXTRACT_VALUE, CSV_TO_STRUCT))(csvOptimization)
      } else {
        optimized
      }
  }

  private val jsonOptimization: PartialFunction[Expression, Expression] = {
    case c: CreateNamedStruct
        // If we create struct from various fields of the same `JsonToStructs`.
        if c.valExprs.forall { v =>
          v.isInstanceOf[GetStructField] &&
            v.asInstanceOf[GetStructField].child.isInstanceOf[JsonToStructs] &&
            v.children.head.semanticEquals(c.valExprs.head.children.head)
        } =>
      val jsonToStructs = c.valExprs.map(_.children.head)
      val sameFieldName = c.names.zip(c.valExprs).forall {
        case (name, valExpr: GetStructField) =>
          name.toString == valExpr.childSchema(valExpr.ordinal).name
        case _ => false
      }

      // Although `CreateNamedStruct` allows duplicated field names, e.g. "a int, a int",
      // `JsonToStructs` does not support parsing json with duplicated field names.
      val duplicateFields = c.names.map(_.toString).distinct.length != c.names.length

      // If we create struct from various fields of the same `JsonToStructs` and we don't
      // alias field names and there is no duplicated field in the struct.
      if (sameFieldName && !duplicateFields) {
        val fromJson = jsonToStructs.head.asInstanceOf[JsonToStructs].copy(schema = c.dataType)
        val nullFields = c.children.grouped(2).flatMap {
          case Seq(name, value) => Seq(name, Literal(null, value.dataType))
        }.toSeq

        If(IsNull(fromJson.child), c.copy(children = nullFields), KnownNotNull(fromJson))
      } else {
        c
      }

    case jsonToStructs @ JsonToStructs(_, options1,
      StructsToJson(options2, child, timeZoneId2), timeZoneId1)
        if options1.isEmpty && options2.isEmpty && timeZoneId1 == timeZoneId2 &&
          jsonToStructs.dataType == child.dataType =>
      // `StructsToJson` only fails when `JacksonGenerator` encounters data types it
      // cannot convert to JSON. But `StructsToJson.checkInputDataTypes` already
      // verifies its child's data types is convertible to JSON. But in
      // `StructsToJson(JsonToStructs(...))` case, we cannot verify input json string
      // so `JsonToStructs` might throw error in runtime. Thus we cannot optimize
      // this case similarly.
      child

    case g @ GetStructField(j @ JsonToStructs(schema: StructType, _, _, _), ordinal, _)
        if schema.length > 1 && j.options.isEmpty =>
        // Options here should be empty because the optimization should not be enabled
        // for some options. For example, when the parse mode is failfast it should not
        // optimize, and should force to parse the whole input JSON with failing fast for
        // an invalid input.
        // To be more conservative, it does not optimize when any option is set for now.
      val prunedSchema = StructType(Array(schema(ordinal)))
      g.copy(child = j.copy(schema = prunedSchema), ordinal = 0)

    case g @ GetArrayStructFields(j @ JsonToStructs(ArrayType(schema: StructType, _),
        _, _, _), _, ordinal, _, _) if schema.length > 1 && j.options.isEmpty =>
      // Obtain the pruned schema by picking the `ordinal` field of the struct.
      val prunedSchema = ArrayType(StructType(Array(schema(ordinal))), g.containsNull)
      g.copy(child = j.copy(schema = prunedSchema), ordinal = 0, numFields = 1)
  }

  private val csvOptimization: PartialFunction[Expression, Expression] = {
    case g @ GetStructField(c @ CsvToStructs(schema: StructType, _, _, _, None), ordinal, _)
        if schema.length > 1 && c.options.isEmpty && schema(ordinal).name != nameOfCorruptRecord =>
        // When the parse mode is permissive, and corrupt column is not selected, we can prune here
        // from `GetStructField`. To be more conservative, it does not optimize when any option
        // is set.
      val prunedSchema = StructType(Array(schema(ordinal)))
      g.copy(child = c.copy(requiredSchema = Some(prunedSchema)), ordinal = 0)

    case csvToStructs @ CsvToStructs(_, options1,
      StructsToCsv(options2, child, timeZoneId2), timeZoneId1, _)
        if options1.isEmpty && options2.isEmpty && timeZoneId1 == timeZoneId2 &&
          csvToStructs.dataType == child.dataType =>
      child
  }
}
