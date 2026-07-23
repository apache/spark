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

  private type SimpleJsonPath = Seq[GetJsonObject.SimpleJsonPathSegment]
  private type SharedJsonCandidate = (GetJsonObject, SimpleJsonPath, String)
  private type SharedJsonCandidateUnit = Seq[SharedJsonCandidate]
  private type RequestedJsonPath = (SimpleJsonPath, String)
  private type RequestedJsonPathUnit = Seq[RequestedJsonPath]

  private case class SharedJsonFields(
      json: Expression,
      paths: Seq[SimpleJsonPath],
      alias: Alias) {
    val ordinalMapping: Map[SimpleJsonPath, Int] = paths.zipWithIndex.toMap
  }

  private final class SelectedJsonPathTrieNode {
    var isTerminal: Boolean = false
    var hasSelectedPathInSubtree: Boolean = false
    val children: mutable.HashMap[GetJsonObject.SimpleJsonPathSegment, SelectedJsonPathTrieNode] =
      mutable.HashMap.empty
  }

  private final class SelectedJsonPathGroup {
    val root = new SelectedJsonPathTrieNode
    val paths = mutable.ArrayBuffer.empty[RequestedJsonPath]

    def tryAdd(unit: RequestedJsonPathUnit): Boolean = {
      val uniquePaths = mutable.LinkedHashMap.empty[SimpleJsonPath, String]
      unit.foreach { case (path, fallbackPath) =>
        uniquePaths.getOrElseUpdate(path, fallbackPath)
      }
      val newPaths = uniquePaths.iterator.filterNot { case (path, _) =>
        containsExactPath(root, path)
      }.toSeq
      val conflictsWithGroup = newPaths.exists { case (path, _) =>
        hasPrefixConflict(root, path)
      }
      if (!conflictsWithGroup) {
        newPaths.foreach { case (path, _) => commitPath(root, path) }
        paths ++= newPaths
        true
      } else {
        false
      }
    }
  }

  // Keep the recursive shared-path traversal comfortably below executor stack limits. Deeper
  // paths retain their existing independent GetJsonObject evaluation.
  private val maxSharedJsonPathDepth = 64

  private def containsExactPath(
      root: SelectedJsonPathTrieNode,
      path: SimpleJsonPath): Boolean = {
    var node = root
    var index = 0
    while (index < path.length) {
      node.children.get(path(index)) match {
        case Some(child) =>
          node = child
          index += 1
        case None =>
          return false
      }
    }
    node.isTerminal
  }

  private def hasPrefixConflict(
      root: SelectedJsonPathTrieNode,
      path: SimpleJsonPath): Boolean = {
    var node = root
    var index = 0
    while (index < path.length) {
      if (node.isTerminal) {
        return true
      }
      node.children.get(path(index)) match {
        case Some(child) =>
          node = child
          index += 1
        case None =>
          return false
      }
    }
    !node.isTerminal && node.hasSelectedPathInSubtree
  }

  private def commitPath(root: SelectedJsonPathTrieNode, path: SimpleJsonPath): Unit = {
    var node = root
    val visited = mutable.ArrayBuffer(root)
    path.foreach { segment =>
      node = node.children.getOrElseUpdate(segment, new SelectedJsonPathTrieNode)
      visited += node
    }
    node.isTerminal = true
    visited.foreach(_.hasSelectedPathInSubtree = true)
  }

  // First-fit builds every prefix-free group in one invocation. Coalesce candidates are added as
  // atomic, prefix-free units so their mutually exclusive object/array paths always use the same
  // shared parse.
  private def groupNonConflictingPaths(
      units: Iterable[RequestedJsonPathUnit]): Seq[Seq[RequestedJsonPath]] = {
    val groups = mutable.ArrayBuffer.empty[SelectedJsonPathGroup]
    units.foreach { unit =>
      var added = false
      val iterator = groups.iterator
      while (!added && iterator.hasNext) {
        added = iterator.next().tryAdd(unit)
      }
      if (!added) {
        val group = new SelectedJsonPathGroup
        require(group.tryAdd(unit))
        groups += group
      }
    }
    groups.map(_.paths.toSeq).toSeq
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
                !conf.getConf(SQLConf.COLLAPSE_PROJECT_ALWAYS_INLINE) &&
                project.projectList.exists(_.exists(_.isInstanceOf[GetJsonObject])) =>
            shareGetJsonObjects(project)
          case _ => p
        }
        withSharedJsonPaths.transformExpressionsWithPruning(
          _.containsAnyPattern(CREATE_NAMED_STRUCT, EXTRACT_VALUE, JSON_TO_STRUCT)
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
   * Share simple named and array-index GetJsonObject paths without changing the Hive-compatible
   * semantics of wildcards. [[MultiGetJsonObject]] preserves the first non-null
   * duplicate-key match used by GetJsonObject, unlike JsonTuple. Prefix-conflicting paths are
   * placed in separate shared parses so each path retains independent legacy evaluation.
   */
  private def shareGetJsonObjects(project: Project): Project = {
    val candidateUnits = project.projectList.flatMap(collectGetJsonObjectFields)
    val groups = mutable.ArrayBuffer.empty[
      (Expression, mutable.ArrayBuffer[RequestedJsonPathUnit])]
    val groupsByHash = mutable.HashMap.empty[
      Int, mutable.ArrayBuffer[(Expression, mutable.ArrayBuffer[RequestedJsonPathUnit])]]

    candidateUnits.foreach { unit =>
      val getJsonObject = unit.head._1
      val bucket = groupsByHash.getOrElseUpdate(
        getJsonObject.json.semanticHash(), mutable.ArrayBuffer.empty)
      bucket.find(_._1.semanticEquals(getJsonObject.json)) match {
        case Some((_, fields)) =>
          fields += unit.map { case (_, pathSegments, path) => pathSegments -> path }
        case None =>
          val requestedUnit = unit.map { case (_, pathSegments, path) => pathSegments -> path }
          val group = getJsonObject.json -> mutable.ArrayBuffer(requestedUnit)
          bucket += group
          groups += group
      }
    }

    val sharedFields = groups.flatMap { case (json, requestedUnits) =>
      groupNonConflictingPaths(requestedUnits).flatMap { nonConflictingPaths =>
        if (nonConflictingPaths.length > 1) {
          val pathSegments = nonConflictingPaths.map(_._1)
          val alias = Alias(
            MultiGetJsonObject(
              json,
              nonConflictingPaths.map(_._2)),
            "_shared_json_paths")()
          Some(SharedJsonFields(json, pathSegments, alias))
        } else {
          None
        }
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
      expression: Expression): Seq[SharedJsonCandidateUnit] = {
    expression match {
      case getJsonObject @ GetJsonObject(_: Attribute, Literal(path: UTF8String, StringType))
          if getJsonObject.deterministic =>
        GetJsonObject.simplePath(path)
          .filter(_.length <= maxSharedJsonPathDepth)
          .map { pathSegments =>
            Seq((getJsonObject, pathSegments, path.toString))
          }.toSeq

      case _: GetJsonObject =>
        Nil

      case coalesce: Coalesce =>
        eligibleCoalesceBranches(coalesce).map(_.map(_._2)).toSeq

      case other =>
        getJsonObjectTraversalChild(other).toSeq.flatMap(collectGetJsonObjectFields)
    }
  }

  private def rewriteGetJsonObjectFields(
      expression: Expression,
      sharedFieldsByHash: Map[Int, Seq[SharedJsonFields]]): Expression = {
    expression match {
      case getJsonObject @ GetJsonObject(json, Literal(path: UTF8String, StringType)) =>
        val replacement = for {
          pathSegments <- GetJsonObject.simplePath(path)
          shared <- sharedFieldsByHash.getOrElse(json.semanticHash(), Nil).find { candidate =>
            candidate.json.semanticEquals(json) && candidate.ordinalMapping.contains(pathSegments)
          }
        } yield GetStructField(shared.alias.toAttribute, shared.ordinalMapping(pathSegments))
        replacement.getOrElse(getJsonObject)

      case _: GetJsonObject =>
        expression

      case coalesce: Coalesce =>
        eligibleCoalesceBranches(coalesce).map { branches =>
          val selectedBranches = branches.toMap
          val firstCandidate = branches.head._2
          val shared = sharedFieldsByHash
            .getOrElse(firstCandidate._1.json.semanticHash(), Nil)
            .find { candidate =>
              candidate.json.semanticEquals(firstCandidate._1.json) &&
                branches.forall { case (_, branchCandidate) =>
                  val (_, pathSegments, _) = branchCandidate
                  candidate.ordinalMapping.contains(pathSegments)
                }
            }
          shared.map { sharedFields =>
            val pairSharedFields = Map(
              firstCandidate._1.json.semanticHash() -> Seq(sharedFields))
            coalesce.withNewChildren(coalesce.children.zipWithIndex.map { case (child, index) =>
              if (selectedBranches.contains(index)) {
                rewriteGetJsonObjectFields(child, pairSharedFields)
              } else {
                child
              }
            })
          }.getOrElse(coalesce)
        }.getOrElse(coalesce)

      case other =>
        getJsonObjectTraversalChild(other).map { child =>
          other.withNewChildren(
            rewriteGetJsonObjectFields(child, sharedFieldsByHash) +: other.children.tail)
        }.getOrElse(other)
    }
  }

  /**
   * Returns the first object-root and first array-root parser calls from a coalesce only when every
   * branch is a GetJsonObject, optionally wrapped in casts, and all branches read the same input
   * attribute. Only one root shape can match a given input. Later same-shape fallbacks and all
   * casts remain in the outer coalesce, preserving lazy evaluation and branch ordering.
   */
  private def eligibleCoalesceBranches(
      coalesce: Coalesce): Option[Seq[(Int, SharedJsonCandidate)]] = {
    def eligibleBranch(expression: Expression): Option[SharedJsonCandidate] = {
      expression match {
        case cast: Cast => eligibleBranch(cast.child)
        case getJsonObject @ GetJsonObject(_: Attribute, Literal(path: UTF8String, StringType))
            if getJsonObject.deterministic =>
          GetJsonObject.simplePath(path)
            .filter(_.length <= maxSharedJsonPathDepth)
            .map(pathSegments => (getJsonObject, pathSegments, path.toString))
        case _ => None
      }
    }

    val branches = coalesce.children.map(eligibleBranch)
    if (branches.nonEmpty && branches.forall(_.isDefined)) {
      val candidates = branches.zipWithIndex.map { case (candidate, index) =>
        index -> candidate.get
      }
      val sameInput = candidates.tail.forall { case (_, candidate) =>
        candidate._1.json.semanticEquals(candidates.head._2._1.json)
      }
      val firstNamed = candidates.find { case (_, candidate) =>
        candidate._2.head.isInstanceOf[GetJsonObject.NamedPathSegment]
      }
      val firstIndexed = candidates.find { case (_, candidate) =>
        candidate._2.head.isInstanceOf[GetJsonObject.IndexedPathSegment]
      }
      if (sameInput && firstNamed.isDefined && firstIndexed.isDefined) {
        Some(Seq(firstNamed.get, firstIndexed.get).sortBy(_._1))
      } else {
        None
      }
    } else {
      None
    }
  }

  private def getJsonObjectTraversalChild(expression: Expression): Option[Expression] = {
    expression match {
      case _: ConditionalExpression | _: And | _: Or | _: In | _: TryEval |
          _: LambdaFunction | _: CreateNamedStruct =>
        None

      case alias: Alias =>
        Some(alias.child)

      case getStructField: GetStructField =>
        Some(getStructField.child)

      case cast: Cast =>
        Some(cast.child)

      case binary: BinaryArithmetic if evaluatesLeftFirst(binary) =>
        Some(binary.left)

      case _ =>
        None
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
