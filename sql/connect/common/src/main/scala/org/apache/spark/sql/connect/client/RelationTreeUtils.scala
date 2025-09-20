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
package org.apache.spark.sql.connect.client

import java.util

import scala.jdk.CollectionConverters._

import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Message

import org.apache.spark.connect.proto
import org.apache.spark.util.SparkEnvUtils

/**
 * Utility functions for visiting and transforming relation trees (a.k.a. query trees).
 *
 * This implementation is efficient for know Relation/Message types. For unknown message types we
 * use proto reflection.
 */
private[connect] object RelationTreeUtils {

  private val NO_INPUT_REL_TYPE_CASES = {
    val typeCases = util.EnumSet.noneOf(classOf[proto.Relation.RelTypeCase])
    typeCases.add(proto.Relation.RelTypeCase.RELTYPE_NOT_SET)
    typeCases.add(proto.Relation.RelTypeCase.READ)
    typeCases.add(proto.Relation.RelTypeCase.LOCAL_RELATION)
    typeCases.add(proto.Relation.RelTypeCase.CACHED_LOCAL_RELATION)
    typeCases.add(proto.Relation.RelTypeCase.CACHED_REMOTE_RELATION)
    typeCases.add(proto.Relation.RelTypeCase.SQL)
    typeCases.add(proto.Relation.RelTypeCase.RANGE)
    typeCases.add(proto.Relation.RelTypeCase.COMMON_INLINE_USER_DEFINED_TABLE_FUNCTION)
    typeCases.add(proto.Relation.RelTypeCase.COMMON_INLINE_USER_DEFINED_DATA_SOURCE)
    typeCases.add(proto.Relation.RelTypeCase.UNRESOLVED_TABLE_VALUED_FUNCTION)
    typeCases.add(proto.Relation.RelTypeCase.REFERENCED_PLAN_ID)
    typeCases.add(proto.Relation.RelTypeCase.UNKNOWN)
    typeCases.add(proto.Relation.RelTypeCase.CATALOG)
    typeCases.add(proto.Relation.RelTypeCase.EXTENSION)
    typeCases
  }

  def visit(relation: proto.Relation)(f: proto.Relation => Boolean): Unit = {
    visit[Null](relation, null) { (current, _) =>
      (f(current), null)
    }
  }

  /**
   * Visit all [[proto.Relation relations]] in a tree.
   *
   * @param relation
   *   root of the tree.
   * @param f
   *   visit callback. The children of a relation will be visited when this function returns true.
   */
  def visit[T](relation: proto.Relation, context: T)(
      f: (proto.Relation, T) => (Boolean, T)): Unit = {
    val messages = new util.ArrayDeque[(Message, T)]
    messages.push(relation -> context)
    while (!messages.isEmpty) {
      val (message, context) = messages.pop()
      visitSingleMessage(message, context, (m, c: T) => messages.push(m -> c), f)
    }
  }

  private def visitSingleMessage[T](
      message: Message,
      context: T,
      addMessage: (Message, T) => Unit,
      f: (proto.Relation, T) => (Boolean, T)): Unit = {
    message match {
      case relation: proto.Relation =>
        val (continue, newContext) = f(relation, context)
        def addRelation(next: proto.Relation): Unit = {
          if (next ne proto.Relation.getDefaultInstance) {
            addMessage(next, newContext)
          }
        }
        if (continue) {
          relation.getRelTypeCase match {
            // Relations without inputs.
            case relTypeCase if NO_INPUT_REL_TYPE_CASES.contains(relTypeCase) =>

            // Single input relations.
            case proto.Relation.RelTypeCase.PROJECT =>
              addRelation(relation.getProject.getInput)
            case proto.Relation.RelTypeCase.FILTER =>
              addRelation(relation.getFilter.getInput)
            case proto.Relation.RelTypeCase.SORT =>
              addRelation(relation.getSort.getInput)
            case proto.Relation.RelTypeCase.LIMIT =>
              addRelation(relation.getLimit.getInput)
            case proto.Relation.RelTypeCase.AGGREGATE =>
              addRelation(relation.getAggregate.getInput)
            case proto.Relation.RelTypeCase.SAMPLE =>
              addRelation(relation.getSample.getInput)
            case proto.Relation.RelTypeCase.OFFSET =>
              addRelation(relation.getOffset.getInput)
            case proto.Relation.RelTypeCase.DEDUPLICATE =>
              addRelation(relation.getDeduplicate.getInput)
            case proto.Relation.RelTypeCase.SUBQUERY_ALIAS =>
              addRelation(relation.getSubqueryAlias.getInput)
            case proto.Relation.RelTypeCase.REPARTITION =>
              addRelation(relation.getRepartition.getInput)
            case proto.Relation.RelTypeCase.TO_DF =>
              addRelation(relation.getToDf.getInput)
            case proto.Relation.RelTypeCase.WITH_COLUMNS_RENAMED =>
              addRelation(relation.getWithColumnsRenamed.getInput)
            case proto.Relation.RelTypeCase.SHOW_STRING =>
              addRelation(relation.getShowString.getInput)
            case proto.Relation.RelTypeCase.DROP =>
              addRelation(relation.getDrop.getInput)
            case proto.Relation.RelTypeCase.TAIL =>
              addRelation(relation.getTail.getInput)
            case proto.Relation.RelTypeCase.WITH_COLUMNS =>
              addRelation(relation.getWithColumns.getInput)
            case proto.Relation.RelTypeCase.HINT =>
              addRelation(relation.getHint.getInput)
            case proto.Relation.RelTypeCase.UNPIVOT =>
              addRelation(relation.getUnpivot.getInput)
            case proto.Relation.RelTypeCase.TO_SCHEMA =>
              addRelation(relation.getToSchema.getInput)
            case proto.Relation.RelTypeCase.REPARTITION_BY_EXPRESSION =>
              addRelation(relation.getRepartitionByExpression.getInput)
            case proto.Relation.RelTypeCase.MAP_PARTITIONS =>
              addRelation(relation.getMapPartitions.getInput)
            case proto.Relation.RelTypeCase.COLLECT_METRICS =>
              addRelation(relation.getCollectMetrics.getInput)
            case proto.Relation.RelTypeCase.PARSE =>
              addRelation(relation.getParse.getInput)
            case proto.Relation.RelTypeCase.WITH_WATERMARK =>
              addRelation(relation.getWithWatermark.getInput)
            case proto.Relation.RelTypeCase.APPLY_IN_PANDAS_WITH_STATE =>
              addRelation(relation.getApplyInPandasWithState.getInput)
            case proto.Relation.RelTypeCase.HTML_STRING =>
              addRelation(relation.getHtmlString.getInput)
            case proto.Relation.RelTypeCase.TRANSPOSE =>
              addRelation(relation.getTranspose.getInput)
            case proto.Relation.RelTypeCase.FILL_NA =>
              addRelation(relation.getFillNa.getInput)
            case proto.Relation.RelTypeCase.DROP_NA =>
              addRelation(relation.getDropNa.getInput)
            case proto.Relation.RelTypeCase.REPLACE =>
              addRelation(relation.getReplace.getInput)
            case proto.Relation.RelTypeCase.SUMMARY =>
              addRelation(relation.getSummary.getInput)
            case proto.Relation.RelTypeCase.CROSSTAB =>
              addRelation(relation.getCrosstab.getInput)
            case proto.Relation.RelTypeCase.DESCRIBE =>
              addRelation(relation.getDescribe.getInput)
            case proto.Relation.RelTypeCase.COV =>
              addRelation(relation.getCov.getInput)
            case proto.Relation.RelTypeCase.CORR =>
              addRelation(relation.getCorr.getInput)
            case proto.Relation.RelTypeCase.APPROX_QUANTILE =>
              addRelation(relation.getApproxQuantile.getInput)
            case proto.Relation.RelTypeCase.FREQ_ITEMS =>
              addRelation(relation.getFreqItems.getInput)
            case proto.Relation.RelTypeCase.SAMPLE_BY =>
              addRelation(relation.getSampleBy.getInput)

            // Multi input relations
            case proto.Relation.RelTypeCase.JOIN =>
              val join = relation.getJoin
              addRelation(join.getLeft)
              addRelation(join.getRight)
            case proto.Relation.RelTypeCase.SET_OP =>
              val setOp = relation.getSetOp
              addRelation(setOp.getLeftInput)
              addRelation(setOp.getRightInput)
            case proto.Relation.RelTypeCase.GROUP_MAP =>
              val groupMap = relation.getGroupMap
              addRelation(groupMap.getInput)
              addRelation(groupMap.getInitialInput)
            case proto.Relation.RelTypeCase.CO_GROUP_MAP =>
              val coGroupMap = relation.getCoGroupMap
              addRelation(coGroupMap.getInput)
              addRelation(coGroupMap.getOther)
            case proto.Relation.RelTypeCase.AS_OF_JOIN =>
              val asOfJoin = relation.getAsOfJoin
              addRelation(asOfJoin.getLeft)
              addRelation(asOfJoin.getRight)
            case proto.Relation.RelTypeCase.WITH_RELATIONS =>
              val withRelations = relation.getWithRelations
              withRelations.getReferencesList.forEach(addRelation(_))
              addRelation(withRelations.getRoot)
            case proto.Relation.RelTypeCase.LATERAL_JOIN =>
              val lateralJoin = relation.getLateralJoin
              addRelation(lateralJoin.getLeft)
              addRelation(lateralJoin.getRight)
            case proto.Relation.RelTypeCase.ML_RELATION =>
              val mlRelation = relation.getMlRelation
              if (mlRelation.hasTransform) {
                addRelation(mlRelation.getTransform.getInput)
              } else if (mlRelation.hasFetch) {
                mlRelation.getFetch.getMethodsList.forEach { method =>
                  method.getArgsList.forEach { args =>
                    if (args.hasInput) {
                      addRelation(args.getInput)
                    }
                  }
                }
              }
              addRelation(mlRelation.getModelSummaryDataset)

            // Unhandled relation type. Fall back to proto reflection.
            case relTypeCase =>
              assert(!SparkEnvUtils.isTesting, "Unhandled relTypeCase: " + relTypeCase)
              val descriptor = relation.getDescriptorForType
                .findFieldByNumber(relTypeCase.getNumber)
              if (descriptor != null && descriptor.getType == FieldDescriptor.Type.MESSAGE) {
                addMessage(relation.getField(descriptor).asInstanceOf[Message], newContext)
              }
          }
        }

      case message =>
        // Unknown message. Fall back to proto reflection.
        assert(
          !SparkEnvUtils.isTesting,
          "Unhandled Message type: " + message.getDescriptorForType.getName)
        message.getAllFields.forEach { (desc, value) =>
          if (desc.getType == FieldDescriptor.Type.MESSAGE) {
            value match {
              case list: util.List[Message @unchecked] =>
                list.forEach(addMessage(_, context))
              case message: Message =>
                addMessage(message, context)
            }
          }
        }
    }
  }

  /**
   * Recursively transform a [[proto.Relation relation]].
   *
   * @param message
   *   entry point.
   * @param pf
   *   transformation to apply to all relations.
   * @tparam M
   *   type of the current message.
   * @return
   *   the transformed relation.
   */
  private[connect] def transform[M <: Message](message: M)(
      pf: PartialFunction[proto.Relation, proto.Relation]): M = {
    def transformRelation(relation: proto.Relation, set: proto.Relation => Any): Unit = {
      if (relation ne proto.Relation.getDefaultInstance) {
        set(transform(relation)(pf))
      }
    }
    def transformMessage(value: Message, fd: FieldDescriptor, builder: Message.Builder): Unit = {
      builder.setField(fd, transform(value)(pf))
    }
    def result(builder: Message.Builder): M = {
      val result = builder.build()
      if (result == message) {
        message
      } else {
        result.asInstanceOf[M]
      }
    }

    message match {
      case relation: proto.Relation =>
        val transformed = pf.applyOrElse(relation, identity[proto.Relation])
        val builder = transformed.toBuilder
        // Transform input relations.
        builder.getRelTypeCase match {
          // Relations without inputs.
          case relTypeCase if NO_INPUT_REL_TYPE_CASES.contains(relTypeCase) =>

          // Single input relations
          case proto.Relation.RelTypeCase.PROJECT =>
            val typeCaseBuilder = builder.getProjectBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.FILTER =>
            val typeCaseBuilder = builder.getFilterBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.SORT =>
            val typeCaseBuilder = builder.getSortBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.LIMIT =>
            val typeCaseBuilder = builder.getLimitBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.AGGREGATE =>
            val typeCaseBuilder = builder.getAggregateBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.SAMPLE =>
            val typeCaseBuilder = builder.getSampleBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.OFFSET =>
            val typeCaseBuilder = builder.getOffsetBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.DEDUPLICATE =>
            val typeCaseBuilder = builder.getDeduplicateBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.SUBQUERY_ALIAS =>
            val typeCaseBuilder = builder.getSubqueryAliasBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.REPARTITION =>
            val typeCaseBuilder = builder.getRepartitionBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.TO_DF =>
            val typeCaseBuilder = builder.getToDfBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.WITH_COLUMNS_RENAMED =>
            val typeCaseBuilder = builder.getWithColumnsRenamedBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.SHOW_STRING =>
            val typeCaseBuilder = builder.getShowStringBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.DROP =>
            val typeCaseBuilder = builder.getDropBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.TAIL =>
            val typeCaseBuilder = builder.getTailBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.WITH_COLUMNS =>
            val typeCaseBuilder = builder.getWithColumnsBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.HINT =>
            val typeCaseBuilder = builder.getHintBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.UNPIVOT =>
            val typeCaseBuilder = builder.getUnpivotBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.TO_SCHEMA =>
            val typeCaseBuilder = builder.getToSchemaBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.REPARTITION_BY_EXPRESSION =>
            val typeCaseBuilder = builder.getRepartitionByExpressionBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.MAP_PARTITIONS =>
            val typeCaseBuilder = builder.getMapPartitionsBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.COLLECT_METRICS =>
            val typeCaseBuilder = builder.getCollectMetricsBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.PARSE =>
            val typeCaseBuilder = builder.getParseBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.WITH_WATERMARK =>
            val typeCaseBuilder = builder.getWithWatermarkBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.APPLY_IN_PANDAS_WITH_STATE =>
            val typeCaseBuilder = builder.getApplyInPandasWithStateBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.HTML_STRING =>
            val typeCaseBuilder = builder.getHtmlStringBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.TRANSPOSE =>
            val typeCaseBuilder = builder.getTransposeBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.FILL_NA =>
            val typeCaseBuilder = builder.getFillNaBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.DROP_NA =>
            val typeCaseBuilder = builder.getDropNaBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.REPLACE =>
            val typeCaseBuilder = builder.getReplaceBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.SUMMARY =>
            val typeCaseBuilder = builder.getSummaryBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.CROSSTAB =>
            val typeCaseBuilder = builder.getCrosstabBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.DESCRIBE =>
            val typeCaseBuilder = builder.getDescribeBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.COV =>
            val typeCaseBuilder = builder.getCovBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.CORR =>
            val typeCaseBuilder = builder.getCorrBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.APPROX_QUANTILE =>
            val typeCaseBuilder = builder.getApproxQuantileBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.FREQ_ITEMS =>
            val typeCaseBuilder = builder.getFreqItemsBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
          case proto.Relation.RelTypeCase.SAMPLE_BY =>
            val typeCaseBuilder = builder.getSampleByBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)

          // Multi-input relations.
          case proto.Relation.RelTypeCase.JOIN =>
            val typeCaseBuilder = builder.getJoinBuilder
            transformRelation(typeCaseBuilder.getLeft, typeCaseBuilder.setLeft)
            transformRelation(typeCaseBuilder.getRight, typeCaseBuilder.setRight)
          case proto.Relation.RelTypeCase.SET_OP =>
            val typeCaseBuilder = builder.getSetOpBuilder
            transformRelation(typeCaseBuilder.getLeftInput, typeCaseBuilder.setLeftInput)
            transformRelation(typeCaseBuilder.getRightInput, typeCaseBuilder.setRightInput)
          case proto.Relation.RelTypeCase.GROUP_MAP =>
            val typeCaseBuilder = builder.getGroupMapBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
            transformRelation(typeCaseBuilder.getInitialInput, typeCaseBuilder.setInitialInput)
          case proto.Relation.RelTypeCase.CO_GROUP_MAP =>
            val typeCaseBuilder = builder.getCoGroupMapBuilder
            transformRelation(typeCaseBuilder.getInput, typeCaseBuilder.setInput)
            transformRelation(typeCaseBuilder.getOther, typeCaseBuilder.setOther)
          case proto.Relation.RelTypeCase.AS_OF_JOIN =>
            val typeCaseBuilder = builder.getAsOfJoinBuilder
            transformRelation(typeCaseBuilder.getLeft, typeCaseBuilder.setLeft)
            transformRelation(typeCaseBuilder.getRight, typeCaseBuilder.setRight)
          case proto.Relation.RelTypeCase.WITH_RELATIONS =>
            val typeCaseBuilder = builder.getWithRelationsBuilder
            (0 until typeCaseBuilder.getReferencesCount).foreach { i =>
              transformRelation(
                typeCaseBuilder.getReferences(i),
                typeCaseBuilder.setReferences(i, _))
            }
            transformRelation(typeCaseBuilder.getRoot, typeCaseBuilder.setRoot)
          case proto.Relation.RelTypeCase.LATERAL_JOIN =>
            val typeCaseBuilder = builder.getLateralJoinBuilder
            transformRelation(typeCaseBuilder.getLeft, typeCaseBuilder.setLeft)
            transformRelation(typeCaseBuilder.getRight, typeCaseBuilder.setRight)
          case proto.Relation.RelTypeCase.ML_RELATION =>
            val typeCaseBuilder = builder.getMlRelationBuilder
            if (typeCaseBuilder.hasTransform) {
              val transformBuilder = typeCaseBuilder.getTransformBuilder
              transformRelation(transformBuilder.getInput, transformBuilder.setInput)
            } else if (typeCaseBuilder.hasFetch) {
              val fetchBuilder = typeCaseBuilder.getFetchBuilder
              (0 until fetchBuilder.getMethodsCount).foreach { i =>
                val methodBuilder = fetchBuilder.getMethodsBuilder(i)
                (0 until methodBuilder.getArgsCount).foreach { j =>
                  val argsBuilder = methodBuilder.getArgsBuilder(j)
                  if (argsBuilder.hasInput) {
                    transformRelation(argsBuilder.getInput, argsBuilder.setInput)
                  }
                }
              }
            }
            transformRelation(
              typeCaseBuilder.getModelSummaryDataset,
              typeCaseBuilder.setModelSummaryDataset)

          // Unhandled relation type. Fall back to proto reflection.
          case relTypeCase =>
            assert(!SparkEnvUtils.isTesting)
            val descriptor = builder.getDescriptorForType
              .findFieldByNumber(relTypeCase.getNumber)
            if (descriptor != null && descriptor.getType == FieldDescriptor.Type.MESSAGE) {
              val value = builder.getField(descriptor).asInstanceOf[Message]
              transformMessage(value, descriptor, builder)
            }
        }
        result(builder)

      case message =>
        // Unknown message type. Fall back to proto reflection.
        val builder = message.toBuilder
        message.getAllFields.forEach { (desc, value) =>
          if (desc.getType == FieldDescriptor.Type.MESSAGE) {
            value match {
              case list: util.List[Message @unchecked] =>
                list.asScala.zipWithIndex.foreach { case (element, i) =>
                  builder.setRepeatedField(desc, i, transform(element)(pf))
                }
              case item: Message =>
                transformMessage(item, desc, builder)
            }
          }
        }
        result(builder)
    }
  }
}
