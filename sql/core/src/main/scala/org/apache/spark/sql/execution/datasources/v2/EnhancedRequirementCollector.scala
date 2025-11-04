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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.immutable.{Map, Seq}
import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.expressions.SchemaPruning
import org.apache.spark.sql.catalyst.expressions.SchemaPruning.RootField
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.GeneratorInputAnalyzer.GeneratorInputInfo
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

sealed trait UsageReason

object UsageReason {
  case object DirectValue extends UsageReason
  case object GeneratorInput extends UsageReason
  case object Metadata extends UsageReason
}

object FieldPath {
  sealed trait ContainerKind
  case object StructKind extends ContainerKind
  case object ArrayKind extends ContainerKind
  case object MapKind extends ContainerKind
  case object AtomicKind extends ContainerKind
}

case class FieldPath(segments: Seq[String], container: FieldPath.ContainerKind)

case class RequirementDetail(path: FieldPath, reason: UsageReason)

case class V2Requirements(
    relationId: Long,
    requirements: Seq[RequirementDetail],
    generatorFullStructs: Map[ExprId, StructType])

case class GeneratorContext(output: AttributeReference, input: ExprId)

private case class RequirementNode(
    direct: Boolean = false,
    children: Map[String, RequirementNode] = Map.empty) {

  def add(path: Seq[String], markDirect: Boolean): RequirementNode = {
    if (path.isEmpty) {
      if (markDirect) copy(direct = true) else this
    } else {
      val head = path.head
      val child = children.getOrElse(head, RequirementNode()).add(path.tail, markDirect)
      copy(children = children.updated(head, child))
    }
  }
}

object EnhancedRequirementCollector {
  private def containerKind(dataType: DataType): FieldPath.ContainerKind = dataType match {
    case _: StructType => FieldPath.StructKind
    case _: ArrayType => FieldPath.ArrayKind
    case _: MapType => FieldPath.MapKind
    case _ => FieldPath.AtomicKind
  }

  private def canonicalizeSegments(schema: StructType, segments: Seq[String]): Seq[String] = {
    val resolver = SQLConf.get.resolver

    def loop(currentType: DataType, remaining: List[String], acc: Vector[String]): Option[Vector[String]] = {
      remaining match {
        case Nil => Some(acc)
        case head :: tail =>
          currentType match {
            case struct: StructType =>
              struct.find(field => resolver(field.name, head)) match {
                case Some(field) => loop(field.dataType, tail, acc :+ field.name)
                case None => None
              }
            case ArrayType(elementType, _) =>
              loop(elementType, remaining, acc)
            case MapType(_, valueType, _) =>
              loop(valueType, remaining, acc)
            case _ =>
              None
          }
      }
    }

    loop(schema, segments.toList, Vector.empty).map(_.toSeq).getOrElse(segments)
  }

  def collect(
      plan: LogicalPlan,
      relationId: Long,
      generatorMetadata: Map[ExprId, GeneratorContext],
      generatorInfos: Iterable[GeneratorInputInfo],
      baselineRootFields: Seq[RootField],
      directAttrExprIds: Set[ExprId],
      directGeneratorExprIds: Set[ExprId],
      wholeStructGeneratorExprIds: Set[ExprId],
      relationOutput: Seq[AttributeReference],
      relationSchema: StructType): V2Requirements = {
    val detailMap = mutable.LinkedHashMap[FieldPath, RequirementDetail]()
    val generatorOriginalStructs = mutable.Map[ExprId, StructType]()

    def reasonRank(reason: UsageReason): Int = reason match {
      case UsageReason.DirectValue => 0
      case UsageReason.Metadata => 1
      case UsageReason.GeneratorInput => 2
    }

    def record(detail: RequirementDetail): Unit = {
      detailMap.get(detail.path) match {
        case Some(existing) =>
          if (reasonRank(detail.reason) < reasonRank(existing.reason)) {
            detailMap.update(detail.path, detail)
          }
        case None =>
          detailMap.update(detail.path, detail)
      }
    }

    def recordPath(
        segments: Seq[String],
        dataType: DataType,
        reason: UsageReason): Unit = {
      if (segments.nonEmpty) {
        val canonical = canonicalizeSegments(relationSchema, segments)
        val resolvedType = resolveDataType(relationSchema, canonical).getOrElse(dataType)
        val path = FieldPath(canonical, containerKind(resolvedType))
        record(RequirementDetail(path, reason))
      }
    }

    def collectFieldChildren(field: StructField, prefix: Seq[String]): Unit = {
      field.dataType match {
        case struct: StructType if struct.fields.nonEmpty =>
          struct.fields.foreach { child =>
            collectFieldChildren(child, prefix :+ child.name)
          }
        case struct: StructType =>
          recordPath(prefix, struct, UsageReason.GeneratorInput)
        case ArrayType(elementType, _) =>
          elementType match {
            case childStruct: StructType =>
              childStruct.fields.foreach { child =>
                collectFieldChildren(child, prefix :+ child.name)
              }
            case _ =>
              recordPath(prefix, field.dataType, UsageReason.GeneratorInput)
          }
        case MapType(_, valueType, _) =>
          valueType match {
            case childStruct: StructType =>
              childStruct.fields.foreach { child =>
                collectFieldChildren(child, prefix :+ child.name)
              }
            case _ =>
              recordPath(prefix, field.dataType, UsageReason.GeneratorInput)
          }
        case _ =>
          recordPath(prefix, field.dataType, UsageReason.GeneratorInput)
      }
    }

    val generatorColumnNames = generatorInfos.map(_.inputAttr.name).toSet

    baselineRootFields.foreach { root =>
      val rootSegments = Seq(root.field.name)
      val prunedField =
        SchemaPruning
          .pruneSchema(StructType(Seq(root.field)), Seq(root))
          .headOption
          .getOrElse(root.field)
      val rootType =
        resolveDataType(relationSchema, canonicalizeSegments(relationSchema, rootSegments))
          .getOrElse(prunedField.dataType)
      val isGeneratorRoot = generatorColumnNames.contains(root.field.name)
      if (root.derivedFromAtt && !isGeneratorRoot) {
        recordPath(rootSegments, rootType, UsageReason.DirectValue)
      }
      val originalFieldOpt = relationSchema.find(_.name == root.field.name)
      val shouldCollectChildren =
        !isGeneratorRoot || originalFieldOpt.forall(_.dataType != prunedField.dataType)
      if (shouldCollectChildren) {
        collectFieldChildren(prunedField, rootSegments)
      }
    }

    val relationAttrsByExprId = relationOutput.map(attr => attr.exprId -> attr).toMap
    directAttrExprIds.foreach { exprId =>
      relationAttrsByExprId.get(exprId).foreach { attr =>
        recordPath(Seq(attr.name), attr.dataType, UsageReason.DirectValue)
      }
    }

    generatorInfos.foreach { info =>
      val nestedUsage = info.nestedFieldsAccessed.filter(_.nonEmpty)
      val generatorHasDirect =
        info.outputAttributes.exists {
          case outAttr: AttributeReference => directGeneratorExprIds.contains(outAttr.exprId)
          case _ => false
        }
      val generatorNeedsWholeStruct =
        info.outputAttributes.exists {
          case outAttr: AttributeReference => wholeStructGeneratorExprIds.contains(outAttr.exprId)
          case _ => false
        }
      val requiresFullStruct =
        directAttrExprIds.contains(info.inputAttr.exprId) ||
          generatorNeedsWholeStruct ||
          (generatorHasDirect && info.pathPrefix.isEmpty && nestedUsage.isEmpty)
      val baseAttrOpt =
        relationAttrsByExprId.get(info.inputAttr.exprId)
          .orElse(info.outputAttributes.collectFirst {
            case outAttr: AttributeReference =>
              generatorMetadata.get(outAttr.exprId).flatMap { ctx =>
                relationAttrsByExprId.get(ctx.input)
              }
          }.flatten)

      baseAttrOpt.foreach { baseAttr =>
        val rootSegments = Seq(baseAttr.name) ++ info.pathPrefix
        val canonicalRoot =
          canonicalizeSegments(relationSchema, rootSegments)
        val generatorInputTypeOpt =
          resolveDataType(relationSchema, canonicalRoot)
        val recordingDataType =
          if (requiresFullStruct) {
            generatorInputTypeOpt.getOrElse(info.dataType)
          } else {
            info.dataType
          }

        if (requiresFullStruct) {
          recordPath(rootSegments, recordingDataType, UsageReason.DirectValue)
        } else if (info.pathPrefix.nonEmpty) {
          recordPath(rootSegments, info.dataType, UsageReason.GeneratorInput)
        }

        nestedUsage.foreach { pathSegments =>
          val fullSegments = rootSegments ++ pathSegments
          recordPath(fullSegments, info.dataType, UsageReason.GeneratorInput)
        }

        info.outputAttributes.foreach {
          case outAttr: AttributeReference
              if outAttr.dataType.isInstanceOf[StructType] &&
                wholeStructGeneratorExprIds.contains(outAttr.exprId) =>
            val originalStructOpt = generatorInputTypeOpt.flatMap {
              case ArrayType(st: StructType, _) => Some(st)
              case MapType(_, st: StructType, _) => Some(st)
              case st: StructType => Some(st)
              case _ => None
            }
            originalStructOpt.foreach { st =>
              generatorOriginalStructs.getOrElseUpdate(outAttr.exprId, st)
            }
          case _ =>
        }
      }
    }

    generatorMetadata.valuesIterator.foreach { ctx =>
      recordPath(Seq(ctx.output.name), ctx.output.dataType, UsageReason.GeneratorInput)
    }

    val collected = detailMap.values.toVector
    V2Requirements(relationId, collected, generatorOriginalStructs.toMap)
  }

  def toRootFields(
      requirementsOpt: Option[V2Requirements],
      relationSchema: StructType): Seq[RootField] = {
    requirementsOpt.map { requirements =>
      val rootNodes = mutable.Map[String, RequirementNode]()

      requirements.requirements.foreach { detail =>
        val segments = detail.path.segments
        if (segments.nonEmpty && relationSchema.fieldNames.contains(segments.head)) {
          val root = segments.head
          val tail = segments.tail
          val current = rootNodes.getOrElse(root, RequirementNode())
          val updated = detail.reason match {
            case UsageReason.DirectValue => current.add(Seq.empty, markDirect = true)
            case _ => current.add(tail, markDirect = false)
          }
          rootNodes(root) = updated
        }
      }

      rootNodes.iterator.flatMap { case (rootName, node) =>
        relationSchema.find(_.name == rootName).map { originalField =>
          val prunedField =
            if (node.direct) {
              originalField
            } else {
              val prunedType = pruneDataType(originalField.dataType, node)
              originalField.copy(dataType = prunedType)
            }
          RootField(prunedField, derivedFromAtt = node.direct)
        }
      }.toVector
    }.getOrElse(Seq.empty[RootField])
  }

  private def pruneDataType(dataType: DataType, node: RequirementNode): DataType = {
    if (node.direct || node.children.isEmpty) {
      dataType
    } else dataType match {
      case struct: StructType =>
        val prunedFields = struct.fields.flatMap { field =>
          node.children.get(field.name).map { childNode =>
            val childType = pruneDataType(field.dataType, childNode)
            field.copy(dataType = childType)
          }
        }
        StructType(prunedFields)
      case ArrayType(elementType, containsNull) =>
        val prunedElement = pruneDataType(elementType, node)
        ArrayType(prunedElement, containsNull)
      case MapType(keyType, valueType, valueContainsNull) =>
        val prunedValue = pruneDataType(valueType, node)
        MapType(keyType, prunedValue, valueContainsNull)
      case _ =>
        dataType
    }
  }

  private def resolveDataType(schema: StructType, segments: Seq[String]): Option[DataType] = {
    if (segments.isEmpty) {
      None
    } else {
      schema.find(_.name == segments.head).flatMap { field =>
        val tail = segments.tail
        if (tail.isEmpty) {
          Some(field.dataType)
        } else {
          field.dataType match {
            case struct: StructType =>
              resolveDataType(struct, tail)
            case ArrayType(elementType, _) =>
              elementType match {
                case nestedStruct: StructType => resolveDataType(nestedStruct, tail)
                case other => Some(other)
              }
            case MapType(_, valueType, _) =>
              valueType match {
                case nestedStruct: StructType => resolveDataType(nestedStruct, tail)
                case other => Some(other)
              }
            case other => Some(other)
          }
        }
      }
    }
  }
}
