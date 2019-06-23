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
package org.apache.spark.sql.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, HiveTableRelation, UnresolvedCatalogRelation}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, EqualTo, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{FindDataSourceTable, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

case class SubstituteMaterializedOSView(mvCatalog: HiveMvCatalog)
  extends Rule[LogicalPlan] {

  val supportedHiveVersion = Seq("3.1.1")
  val spark: SparkSession = SparkSession.getActiveSession.get
  val conf: SQLConf = spark.sqlContext.conf

  def apply(plan: LogicalPlan): LogicalPlan = if (isHiveVersionSupported && isMvOsEnabled) {
    plan transformDown {
      case op@PhysicalOperation(projects, filters, leafPlan) if filters.nonEmpty =>
        val rel = getRelation(leafPlan)
        rel match {
          case Some(relation@HiveTableRelation(table, _, _)) if !isMVTable(Option(table)) =>
            transformToMV(projects, filters, relation, table).getOrElse(op)
          case Some(relation@LogicalRelation(_, _, tableOpt, _)) if !isMVTable(tableOpt) =>
            val a = transformToMV(projects, filters, relation, tableOpt.get).getOrElse(op)
            a
          case _ => op
        }
    }
  } else {
    plan
  }

  private def getRelation(plan: LogicalPlan): Option[LogicalPlan] = {
    plan.find(x => x.isInstanceOf[HiveTableRelation] || x.isInstanceOf[LogicalRelation])
  }

  private def transformToMV(projects: Seq[NamedExpression], filters: Seq[Expression],
      relation: LogicalPlan, catalogTable: CatalogTable): Option[LogicalPlan] = {

    // 1. Not checking for project list right now, only filters
    //    project of mv should be super-set of project of table. Also need to map each
    //    mv project [AttributeReference] to original [AttributeReference]
    // 2. returning the 1st MV which matches found
    // 3. Reducing the seq or filters by And ?
    // 4. The original relation is substituted with mv's relation
    // 5. The original filter is transformed to have mv's attribute using name,

    val ident = catalogTable.identifier
    val mv = mvCatalog.
      getMaterializedViewForTable(ident.database.get, ident.table)

    val attrs = filters.flatMap {
      filter =>
        filter match {
          case EqualTo(expr: AttributeReference, _) =>
            Seq(expr.name)
          case EqualTo(_, expr: AttributeReference) =>
            Seq(expr.name)
          case _ =>
            Seq.empty
        }
    }

    val mvs = mvCatalog.getMaterializedViewsOfTable(mv.mvDetails)

    val table = mvs.map(table => {
      val mvPlan = mvCatalog.getMaterializedViewPlan(table).get
      val sort = mvPlan.collect {
        case s: Sort => s
      }
      val sortAttrs = sort.head.references.map(x => x.name).toSeq
      val commonAttrs = attrs.intersect(sortAttrs)
      CatalogTableInfo(table, commonAttrs.size)
    })
    .filter(_.commonAttrsCount != 0)
    .reduceLeftOption((item1, item2) => {
      if (item1.commonAttrsCount > item2.commonAttrsCount) item1 else item2
    })
    table match {
      case Some(tableInfo) =>
        val plan = getTableRelationFromIdentifier(tableInfo.table)
        constructLogicalPlan(filters, projects, plan, relation)
      case _ =>
        None
    }
  }


  private def constructLogicalPlan(filters: Seq[Expression], projects: Seq[NamedExpression],
     relationOption: Option[LogicalPlan], originalLogicalPlan: LogicalPlan) = {
    val filterExpr = filters.reduceLeft(And)
    def getReplacedPlan(plan: LogicalPlan): LogicalPlan = {
      plan match {
        case relation: LogicalRelation =>
          val nameToAttr = relation.output.map(_.name).zip(relation.output).toMap
          val replaced = originalLogicalPlan.output.map (
            x =>
              nameToAttr(x.name).withExprId(x.exprId).withQualifier(x.qualifier)
          )
          relation.copy(output = replaced)
        case relation: HiveTableRelation =>
          val hiveRelation = originalLogicalPlan.asInstanceOf[HiveTableRelation]
          val nameToAttrData = relation.dataCols.map(_.name).zip(relation.output).toMap
          val nameToAttrPart = relation.partitionCols.map(_.name).zip(relation.output).toMap
          val newDataCols = hiveRelation.dataCols.map {
            col =>
              nameToAttrData(col.name).withExprId(col.exprId)
                .withQualifier(Seq("db", "mv"))
          }

          val newPartCols = hiveRelation.partitionCols.map {
            col =>
              nameToAttrPart(col.name).withExprId(col.exprId)
                .withQualifier(Seq("db", "mv"))
          }
          relation.copy(dataCols = newDataCols, partitionCols = newPartCols)
        case _ =>
          null
      }
    }
    relationOption match {
      case Some(relation: LogicalPlan) =>
        val newRelation = getReplacedPlan(relation)
        val projectSmap = newRelation.output.map(x => (x.name -> x)).toMap
        val newProjects = projects.map {
          x => projectSmap(x.name)
        }

        val newFilterExpr = filterExpr.transform {
          case a: Attribute =>
            projectSmap(a.name)
        }
        Some(Project(newProjects, Filter(newFilterExpr, getReplacedPlan(relation))))
      case _ =>
        None
    }
  }

  private def getTableRelationFromIdentifier(table: CatalogTable):
  Option[LogicalPlan] = {
    try {
      val catalog = spark.sessionState.catalog.asInstanceOf[HiveSessionCatalog]
      table match {
        case catalogTable =>
          val resolvedRelation =
            new FindDataSourceTable(spark)(UnresolvedCatalogRelation(catalogTable))

          /*
            * When an MV(HiveTableRelation) is used for a table involved in a join
            * the SQL execution crashes for the following reason
            * When the physical planner tries to decide on type of join,
            * it queries HiveTableRelation to get stats.
            * When stats are empty, exception is thrown
            * To populate the stats, DetermineTableStats rule is invoked
            * in MV Optimizer rule.
            */
          val resolvedRelationWithStats = new DetermineTableStats(spark)(resolvedRelation)
          Some(RelationConversions(conf, catalog)(resolvedRelationWithStats))
        case _ => None
      }
    } catch {
      case e: Exception =>
        logError("Exception occured when trying to get MV", e)
        None
    }
  }


  private def isMVTable(table: Option[CatalogTable]): Boolean = {
    table.isDefined &&
      (table.get.tableType == CatalogTableType.MV ||
        (Utils.isTesting && table.get.viewOriginalText.isDefined))
  }

  private def isMvOsEnabled: Boolean = {
    spark.sqlContext.conf.mvOSEnabled
  }

  /**
    * Returns true if the Hive version is from
    * org.apache.spark.sql.hive.SubstituteMaterializedOSView#supportedHiveVersion()
    */
  private def isHiveVersionSupported: Boolean = {
    val catalog = spark.sharedState.externalCatalog
    Utils.isTesting || {
      if (catalog.isInstanceOf[HiveExternalCatalog] ) {
        val version = catalog.asInstanceOf[HiveExternalCatalog].client.version.fullVersion
        supportedHiveVersion.contains(version)
      } else {
        false
      }
    }
  }


  case class CatalogTableInfo(table: CatalogTable, commonAttrsCount: Int)

}