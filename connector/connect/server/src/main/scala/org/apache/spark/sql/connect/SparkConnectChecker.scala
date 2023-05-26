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

package org.apache.spark.sql.connect

import scala.collection.JavaConverters._

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connect.common.InvalidPlanInput
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

object SparkConnectChecker {

  def checkRelation(rel: proto.Relation): Unit = {
    rel.getRelTypeCase match {
      case proto.Relation.RelTypeCase.SHOW_STRING => // checkShowString(rel.getShowString)
      case proto.Relation.RelTypeCase.HTML_STRING => // checkHtmlString(rel.getHtmlString)
      case proto.Relation.RelTypeCase.READ => checkReadRel(rel.getRead)
      case proto.Relation.RelTypeCase.PROJECT => // checkProject(rel.getProject)
      case proto.Relation.RelTypeCase.FILTER => // checkFilter(rel.getFilter)
      case proto.Relation.RelTypeCase.LIMIT => // checkLimit(rel.getLimit)
      case proto.Relation.RelTypeCase.OFFSET => // checkOffset(rel.getOffset)
      case proto.Relation.RelTypeCase.TAIL => // checkTail(rel.getTail)
      case proto.Relation.RelTypeCase.JOIN => // checkJoin(rel.getJoin)
      case proto.Relation.RelTypeCase.DEDUPLICATE => // checkDeduplicate(rel.getDeduplicate)
      case proto.Relation.RelTypeCase.SET_OP => // checkSetOperation(rel.getSetOp)
      case proto.Relation.RelTypeCase.SORT => // checkSort(rel.getSort)
      case proto.Relation.RelTypeCase.DROP => // checkDrop(rel.getDrop)
      case proto.Relation.RelTypeCase.AGGREGATE => // checkAggregate(rel.getAggregate)
      case proto.Relation.RelTypeCase.SQL => // checkSql(rel.getSql)
      case proto.Relation.RelTypeCase.LOCAL_RELATION =>
      // checkLocalRelation(rel.getLocalRelation)
      case proto.Relation.RelTypeCase.SAMPLE => // checkSample(rel.getSample)
      case proto.Relation.RelTypeCase.RANGE => // checkRange(rel.getRange)
      case proto.Relation.RelTypeCase.SUBQUERY_ALIAS =>
      // checkSubqueryAlias(rel.getSubqueryAlias)
      case proto.Relation.RelTypeCase.REPARTITION => // checkRepartition(rel.getRepartition)
      case proto.Relation.RelTypeCase.FILL_NA => // checkNAFill(rel.getFillNa)
      case proto.Relation.RelTypeCase.DROP_NA => // checkNADrop(rel.getDropNa)
      case proto.Relation.RelTypeCase.REPLACE => // checkReplace(rel.getReplace)
      case proto.Relation.RelTypeCase.SUMMARY => // checkStatSummary(rel.getSummary)
      case proto.Relation.RelTypeCase.DESCRIBE => // checkStatDescribe(rel.getDescribe)
      case proto.Relation.RelTypeCase.COV => // checkStatCov(rel.getCov)
      case proto.Relation.RelTypeCase.CORR => // checkStatCorr(rel.getCorr)
      case proto.Relation.RelTypeCase.APPROX_QUANTILE =>
      // checkStatApproxQuantile(rel.getApproxQuantile)
      case proto.Relation.RelTypeCase.CROSSTAB =>
      // checkStatCrosstab(rel.getCrosstab)
      case proto.Relation.RelTypeCase.FREQ_ITEMS => // checkStatFreqItems(rel.getFreqItems)
      case proto.Relation.RelTypeCase.SAMPLE_BY =>
      // checkStatSampleBy(rel.getSampleBy)
      case proto.Relation.RelTypeCase.TO_SCHEMA => // checkToSchema(rel.getToSchema)
      case proto.Relation.RelTypeCase.TO_DF =>
      // checkToDF(rel.getToDf)
      case proto.Relation.RelTypeCase.WITH_COLUMNS_RENAMED =>
      // checkWithColumnsRenamed(rel.getWithColumnsRenamed)
      case proto.Relation.RelTypeCase.WITH_COLUMNS => // checkWithColumns(rel.getWithColumns)
      case proto.Relation.RelTypeCase.WITH_WATERMARK =>
      // checkWithWatermark(rel.getWithWatermark)
      case proto.Relation.RelTypeCase.CACHED_LOCAL_RELATION =>
      // checkCachedLocalRelation(rel.getCachedLocalRelation)
      case proto.Relation.RelTypeCase.HINT => // checkHint(rel.getHint)
      case proto.Relation.RelTypeCase.UNPIVOT => // checkUnpivot(rel.getUnpivot)
      case proto.Relation.RelTypeCase.REPARTITION_BY_EXPRESSION =>
      // checkRepartitionByExpression(rel.getRepartitionByExpression)
      case proto.Relation.RelTypeCase.MAP_PARTITIONS =>
      // checkMapPartitions(rel.getMapPartitions)
      case proto.Relation.RelTypeCase.GROUP_MAP =>
      // checkGroupMap(rel.getGroupMap)
      case proto.Relation.RelTypeCase.CO_GROUP_MAP =>
      // checkCoGroupMap(rel.getCoGroupMap)
      case proto.Relation.RelTypeCase.APPLY_IN_PANDAS_WITH_STATE =>
      // checkApplyInPandasWithState(rel.getApplyInPandasWithState)
      case proto.Relation.RelTypeCase.COLLECT_METRICS =>
      // checkCollectMetrics(rel.getCollectMetrics)
      case proto.Relation.RelTypeCase.PARSE => // checkParse(rel.getParse)
      case proto.Relation.RelTypeCase.RELTYPE_NOT_SET =>
        throw new IndexOutOfBoundsException("Expected Relation to be set, but is empty.")

      // Catalog API (internal-only)
      case proto.Relation.RelTypeCase.CATALOG => // checkCatalog(rel.getCatalog)
      // Handle plugins for Spark Connect Relation types.
      case proto.Relation.RelTypeCase.EXTENSION =>
      // checkRelationPlugin(rel.getExtension)
      case _ => throw InvalidPlanInput(s"${rel.getUnknown} not supported.")
    }
  }

  private def checkReadRel(rel: proto.Read): Unit = {
    rel.getReadTypeCase match {
      case proto.Read.ReadTypeCase.NAMED_TABLE =>
      case proto.Read.ReadTypeCase.DATA_SOURCE if !rel.getIsStreaming =>
        val localMap = CaseInsensitiveMap[String](rel.getDataSource.getOptionsMap.asScala.toMap)
        if (rel.getDataSource.getFormat == "jdbc" && rel.getDataSource.getPredicatesCount > 0) {
          if (!localMap.contains(JDBCOptions.JDBC_URL) ||
            !localMap.contains(JDBCOptions.JDBC_TABLE_NAME)) {
            throw InvalidPlanInput(s"Invalid jdbc params, please specify jdbc url and table.")
          }
        } else if (rel.getDataSource.getPredicatesCount != 0) {
          throw InvalidPlanInput(
            s"Predicates are not supported for ${rel.getDataSource.getFormat} data sources.")
        }
      case proto.Read.ReadTypeCase.DATA_SOURCE if rel.getIsStreaming =>
        val streamSource = rel.getDataSource
        if (streamSource.getPathsCount < 0 || streamSource.getPathsCount > 1) {
          throw InvalidPlanInput(s"Multiple paths are not supported for streaming source")
        }
      case _ => throw InvalidPlanInput(s"Does not support ${rel.getReadTypeCase.name()}")
    }
  }
}
