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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CREATE_NAMED_STRUCT, EXTRACT_VALUE,
  GET_JSON_OBJECT, JSON_TO_STRUCT}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

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
 * 4. Share one MultiGetJsonObject when a Project extracts multiple simple paths from a JSON.
 * 5. Prune unnecessary columns from GetStructField + CsvToStructs.
 */
object OptimizeCsvJsonExprs extends Rule[LogicalPlan] {
  private def nameOfCorruptRecord = conf.columnNameOfCorruptRecord

  private case class SharedJsonFields(
      json: Expression,
      fieldNames: Seq[String],
      alias: Alias) {
    val ordinalMapping: Map[String, Int] = fieldNames.zipWithIndex.toMap
  }

  private def evaluatesLeftFirst(binary: BinaryArithmetic): Boolean = binary match {
    case _: Add | _: Subtract | _: Multiply | _: BitwiseAnd | _: BitwiseOr | _: BitwiseXor => true
    case _ => false
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAnyPattern(CREATE_NAMED_STRUCT, EXTRACT_VALUE, GET_JSON_OBJECT, JSON_TO_STRUCT),
    ruleId) {
    case p =>
      val optimized = if (conf.jsonExpressionOptimization) {
        val withSharedJsonPaths = p match {
          case project: Project
              if conf.getJsonObjectSharedParsingEnabled &&
                !conf.getConf(SQLConf.COLLAPSE_PROJECT_ALWAYS_INLINE) =>
            shareGetJsonObjects(project)
          case _ => p
        }
        withSharedJsonPaths.transformExpressionsWithPruning(
          _.containsAnyPattern(CREATE_NAMED_STRUCT, EXTRACT_VALUE, GET_JSON_OBJECT, JSON_TO_STRUCT)
          )(jsonOptimization)
      } else {
        p
      }

      if (conf.csvExpressionOptimization) {
        optimized.transformExpressionsWithPruning(
          _.containsAnyPattern(EXTRACT_VALUE))(csvOptimization)
      } else {
        optimized
      }
  }

  /**
   * Share simple top-level GetJsonObject paths without changing the Hive-compatible semantics of
   * nested paths, wildcards, or array subscripts. [[MultiGetJsonObject]] preserves the first
   * non-null duplicate-key match used by GetJsonObject, unlike JsonTuple.
   */
  private def shareGetJsonObjects(project: Project): Project = {
    val candidates = project.projectList.flatMap(collectGetJsonObjectFields)
    val groups = mutable.ArrayBuffer.empty[
      (Expression, mutable.ArrayBuffer[(String, String)])]
    val groupsByHash = mutable.HashMap.empty[
      Int, mutable.ArrayBuffer[(Expression, mutable.ArrayBuffer[(String, String)])]]

    candidates.foreach { case (getJsonObject, fieldName, path) =>
      val bucket = groupsByHash.getOrElseUpdate(
        getJsonObject.json.semanticHash(), mutable.ArrayBuffer.empty)
      bucket.find(_._1.semanticEquals(getJsonObject.json)) match {
        case Some((_, fields)) => fields += fieldName -> path
        case None =>
          val group = getJsonObject.json -> mutable.ArrayBuffer(fieldName -> path)
          bucket += group
          groups += group
      }
    }

    val sharedFields = groups.flatMap { case (json, requestedFields) =>
      val fieldsByName = mutable.LinkedHashMap.empty[String, String]
      requestedFields.foreach { case (fieldName, path) =>
        fieldsByName.getOrElseUpdate(fieldName, path)
      }
      val fieldNames = fieldsByName.keys.toSeq
      if (fieldNames.length > 1) {
        val alias = Alias(
          MultiGetJsonObject(json, fieldNames, fieldsByName.values.toSeq),
          "_shared_json_paths")()
        Some(SharedJsonFields(json, fieldNames, alias))
      } else {
        None
      }
    }.toSeq

    if (sharedFields.isEmpty) {
      project
    } else {
      val sharedFieldsByHash = sharedFields.groupBy(_.json.semanticHash())
      val rewrittenProjectList = project.projectList.map { expression =>
        rewriteGetJsonObjectFields(expression, sharedFieldsByHash)
          .asInstanceOf[NamedExpression]
      }
      val innerProjectList = project.child.output ++ sharedFields.map(_.alias)
      Project(rewrittenProjectList, Project(innerProjectList, project.child))
    }
  }

  private def collectGetJsonObjectFields(
      expression: Expression): Seq[(GetJsonObject, String, String)] = {
    expression match {
      case _: ConditionalExpression | _: And | _: Or | _: In | _: TryEval |
          _: LambdaFunction | _: CreateNamedStruct =>
        Nil

      case getJsonObject @ GetJsonObject(_: Attribute, Literal(path: UTF8String, StringType))
          if getJsonObject.deterministic =>
        GetJsonObject.simpleTopLevelField(path)
          .map(fieldName => (getJsonObject, fieldName, path.toString)).toSeq

      case _: GetJsonObject =>
        Nil

      case alias: Alias =>
        collectGetJsonObjectFields(alias.child)

      case getStructField: GetStructField =>
        collectGetJsonObjectFields(getStructField.child)

      case cast: Cast =>
        collectGetJsonObjectFields(cast.child)

      case binary: BinaryArithmetic if evaluatesLeftFirst(binary) =>
        collectGetJsonObjectFields(binary.left)

      case _ =>
        Nil
    }
  }

  private def rewriteGetJsonObjectFields(
      expression: Expression,
      sharedFieldsByHash: Map[Int, Seq[SharedJsonFields]]): Expression = {
    expression match {
      case _: ConditionalExpression | _: And | _: Or | _: In | _: TryEval |
          _: LambdaFunction | _: CreateNamedStruct =>
        expression

      case getJsonObject @ GetJsonObject(json, Literal(path: UTF8String, StringType)) =>
        val replacement = for {
          fieldName <- GetJsonObject.simpleTopLevelField(path)
          shared <- sharedFieldsByHash.getOrElse(json.semanticHash(), Nil).find { candidate =>
            candidate.json.semanticEquals(json) && candidate.ordinalMapping.contains(fieldName)
          }
        } yield GetStructField(shared.alias.toAttribute, shared.ordinalMapping(fieldName))
        replacement.getOrElse(getJsonObject)

      case _: GetJsonObject =>
        expression

      case alias: Alias =>
        alias.mapChildren(rewriteGetJsonObjectFields(_, sharedFieldsByHash))

      case getStructField: GetStructField =>
        getStructField.mapChildren(rewriteGetJsonObjectFields(_, sharedFieldsByHash))

      case cast: Cast =>
        cast.mapChildren(rewriteGetJsonObjectFields(_, sharedFieldsByHash))

      case binary: BinaryArithmetic if evaluatesLeftFirst(binary) =>
        binary.withNewChildren(
          Seq(rewriteGetJsonObjectFields(binary.left, sharedFieldsByHash), binary.right))

      case _ =>
        expression
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
  }
}
