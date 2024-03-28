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

package org.apache.spark.sql.execution

import scala.collection.mutable

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, AttributeSet, Expression, NamedExpression, SubqueryExpression}
import org.apache.spark.sql.catalyst.optimizer.EliminateResolvedHint
import org.apache.spark.sql.catalyst.plans.logical.{Filter, IgnoreCachedData, LeafNode, LogicalPlan, Project, ResolvedHint, SubqueryAlias, View}
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.analysis.EarlyCollapseProject
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, FileTable}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK


/** Holds a cached logical plan and its data */
case class CachedData(plan: LogicalPlan,
                      cachedRepresentation: Either[LogicalPlan, InMemoryRelation])

/**
 * Provides support in a SQLContext for caching query results and automatically using these cached
 * results when subsequent queries are executed.  Data is cached using byte buffers stored in an
 * InMemoryRelation.  This relation is automatically substituted query plans that return the
 * `sameResult` as the originally cached query.
 *
 * Internal to Spark SQL.
 */
class CacheManager extends Logging with AdaptiveSparkPlanHelper {

  /**
   * Maintains the list of cached plans as an immutable sequence.  Any updates to the list
   * should be protected in a "this.synchronized" block which includes the reading of the
   * existing value and the update of the cachedData var.
   */
  @transient @volatile
  private var cachedData = IndexedSeq[CachedData]()

  /** Clears all cached tables. */
  def clearCache(): Unit = this.synchronized {
    cachedData.foreach(_.cachedRepresentation.fold(CacheManager.inMemoryRelationExtractor, identity)
      .cacheBuilder.clearCache())
    cachedData = IndexedSeq[CachedData]()
  }

  /** Checks if the cache is empty. */
  def isEmpty: Boolean = {
    cachedData.isEmpty
  }

  /**
   * Caches the data produced by the logical representation of the given [[Dataset]].
   * Unlike `RDD.cache()`, the default storage level is set to be `MEMORY_AND_DISK` because
   * recomputing the in-memory columnar representation of the underlying table is expensive.
   */
  def cacheQuery(
      query: Dataset[_],
      tableName: Option[String] = None,
      storageLevel: StorageLevel = MEMORY_AND_DISK): Unit = {
    cacheQuery(query.sparkSession, query.queryExecution.normalized, tableName, storageLevel)
  }

  /**
   * Caches the data produced by the given [[LogicalPlan]].
   * Unlike `RDD.cache()`, the default storage level is set to be `MEMORY_AND_DISK` because
   * recomputing the in-memory columnar representation of the underlying table is expensive.
   */
  def cacheQuery(
      spark: SparkSession,
      planToCache: LogicalPlan,
      tableName: Option[String]): Unit = {
    cacheQuery(spark, planToCache, tableName, MEMORY_AND_DISK)
  }

  /**
   * Caches the data produced by the given [[LogicalPlan]].
   */
  def cacheQuery(
      spark: SparkSession,
      planToCache: LogicalPlan,
      tableName: Option[String],
      storageLevel: StorageLevel): Unit = {
    if (storageLevel == StorageLevel.NONE) {
      // Do nothing for StorageLevel.NONE since it will not actually cache any data.
    } else if (lookupCachedData(planToCache).exists(_.cachedRepresentation.isRight)) {
      logWarning("Asked to cache already cached data.")
    } else {
      val sessionWithConfigsOff = getOrCloneSessionWithConfigsOff(spark)
      val inMemoryRelation = sessionWithConfigsOff.withActive {
        val qe = sessionWithConfigsOff.sessionState.executePlan(planToCache)
        InMemoryRelation(
          storageLevel,
          qe,
          tableName)
      }

      this.synchronized {
        if (lookupCachedData(planToCache).exists(_.cachedRepresentation.isRight)) {
          logWarning("Data has already been cached.")
        } else {
          cachedData = CachedData(planToCache, Right(inMemoryRelation)) +: cachedData
        }
      }
    }
  }

  /**
   * Un-cache the given plan or all the cache entries that refer to the given plan.
   * @param query     The [[Dataset]] to be un-cached.
   * @param cascade   If true, un-cache all the cache entries that refer to the given
   *                  [[Dataset]]; otherwise un-cache the given [[Dataset]] only.
   */
  def uncacheQuery(
      query: Dataset[_],
      cascade: Boolean): Unit = {
    uncacheQuery(query.sparkSession, query.queryExecution.normalized, cascade)
  }

  /**
   * Un-cache the given plan or all the cache entries that refer to the given plan.
   * @param spark     The Spark session.
   * @param plan      The plan to be un-cached.
   * @param cascade   If true, un-cache all the cache entries that refer to the given
   *                  plan; otherwise un-cache the given plan only.
   * @param blocking  Whether to block until all blocks are deleted.
   */
  def uncacheQuery(
      spark: SparkSession,
      plan: LogicalPlan,
      cascade: Boolean,
      blocking: Boolean = false): Unit = {
    val dummyCd = CachedData(plan, Left(plan))
    uncacheQuery(spark,
      (planToCheck: LogicalPlan, partialMatchOk: Boolean) => {
      dummyCd.plan.sameResult(planToCheck) || (partialMatchOk &&
        (planToCheck match {
        case p: Project => lookUpPartiallyMatchedCachedPlan(p, IndexedSeq(dummyCd)).isDefined
        case _ => false
      }))
    }, cascade, blocking)
  }

  def uncacheTableOrView(spark: SparkSession, name: Seq[String], cascade: Boolean): Unit = {
    uncacheQuery(
      spark,
      isMatchedTableOrView(_, _, name, spark.sessionState.conf),
      cascade,
      blocking = false)
  }

  private def isMatchedTableOrView(
      plan: LogicalPlan,
      partialMatch: Boolean,
      name: Seq[String],
      conf: SQLConf): Boolean = {
    def isSameName(nameInCache: Seq[String]): Boolean = {
      nameInCache.length == name.length && nameInCache.zip(name).forall(conf.resolver.tupled)
    }

    plan match {
      case SubqueryAlias(ident, LogicalRelation(_, _, Some(catalogTable), _)) =>
        val v1Ident = catalogTable.identifier
        isSameName(ident.qualifier :+ ident.name) && isSameName(v1Ident.nameParts)

      case SubqueryAlias(ident, DataSourceV2Relation(_, _, Some(catalog), Some(v2Ident), _)) =>
        import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
        isSameName(ident.qualifier :+ ident.name) &&
          isSameName(v2Ident.toQualifiedNameParts(catalog))

      case SubqueryAlias(ident, View(catalogTable, _, _)) =>
        val v1Ident = catalogTable.identifier
        isSameName(ident.qualifier :+ ident.name) && isSameName(v1Ident.nameParts)

      case SubqueryAlias(ident, HiveTableRelation(catalogTable, _, _, _, _)) =>
        val v1Ident = catalogTable.identifier
        isSameName(ident.qualifier :+ ident.name) && isSameName(v1Ident.nameParts)

      case _ => false
    }
  }

  def uncacheQuery(
      spark: SparkSession,
      isMatchedPlan: (LogicalPlan, Boolean) => Boolean,
      cascade: Boolean,
      blocking: Boolean): Unit = {

    val shouldRemove: LogicalPlan => Boolean = if (cascade) {
        _.exists(isMatchedPlan(_, false))
      } else {
        isMatchedPlan(_, false)
      }
    val plansToUncache = cachedData.filter(cd => shouldRemove(cd.plan))
    this.synchronized {
      cachedData = cachedData.filterNot(cd => plansToUncache.exists(_ eq cd))
    }
    plansToUncache.foreach { _.cachedRepresentation.
      fold(CacheManager.inMemoryRelationExtractor, identity).cacheBuilder.clearCache(blocking) }

    // Re-compile dependent cached queries after removing the cached query.
    if (!cascade) {
      recacheByCondition(spark, cd => {
        // If the cache buffer has already been loaded, we don't need to recompile the cached plan,
        // as it does not rely on the plan that has been uncached anymore, it will just produce
        // data from the cache buffer.
        // Note that the `CachedRDDBuilder.isCachedColumnBuffersLoaded` call is a non-locking
        // status test and may not return the most accurate cache buffer state. So the worse case
        // scenario can be:
        // 1) The buffer has been loaded, but `isCachedColumnBuffersLoaded` returns false, then we
        //    will clear the buffer and re-compiled the plan. It is inefficient but doesn't affect
        //    correctness.
        // 2) The buffer has been cleared, but `isCachedColumnBuffersLoaded` returns true, then we
        //    will keep it as it is. It means the physical plan has been re-compiled already in the
        //    other thread.
        val cacheAlreadyLoaded = cd.cachedRepresentation.
          fold(CacheManager.inMemoryRelationExtractor, identity).cacheBuilder.
          isCachedColumnBuffersLoaded
        !cacheAlreadyLoaded && cd.plan.exists(isMatchedPlan(_, true))
      })
    }
  }

  // Analyzes column statistics in the given cache data
  private[sql] def analyzeColumnCacheQuery(
      sparkSession: SparkSession,
      cachedData: CachedData,
      column: Seq[Attribute]): Unit = {
    val relation = cachedData.cachedRepresentation
    val (rowCount, newColStats) =
      CommandUtils.computeColumnStats(sparkSession, relation.merge, column)
    relation.fold(CacheManager.inMemoryRelationExtractor, identity).
      updateStats(rowCount, newColStats)
  }

  /**
   * Tries to re-cache all the cache entries that refer to the given plan.
   */
  def recacheByPlan(spark: SparkSession, plan: LogicalPlan): Unit = {
    recacheByCondition(spark, _.plan.exists(_.sameResult(plan)))
  }

  /**
   *  Re-caches all the cache entries that satisfies the given `condition`.
   */
  private def recacheByCondition(
      spark: SparkSession,
      condition: CachedData => Boolean): Unit = {
    val needToRecache = cachedData.filter(condition)
    this.synchronized {
      // Remove the cache entry before creating a new ones.
      cachedData = cachedData.filterNot(cd => needToRecache.exists(_ eq cd))
    }
    needToRecache.foreach { cd =>
      cd.cachedRepresentation.fold(CacheManager.inMemoryRelationExtractor, identity).
        cacheBuilder.clearCache()
      val sessionWithConfigsOff = getOrCloneSessionWithConfigsOff(spark)
      val newCache = sessionWithConfigsOff.withActive {
        val qe = sessionWithConfigsOff.sessionState.executePlan(cd.plan)
        InMemoryRelation(cd.cachedRepresentation.
          fold(CacheManager.inMemoryRelationExtractor, identity).cacheBuilder, qe)
      }
      val recomputedPlan = cd.copy(cachedRepresentation = Right(newCache))
      this.synchronized {
        if (lookupCachedData(recomputedPlan.plan).exists(_.cachedRepresentation.isRight)) {
          logWarning("While recaching, data was already added to cache.")
        } else {
          cachedData = recomputedPlan +: cachedData
        }
      }
    }
  }

  /** Optionally returns cached data for the given [[Dataset]] */
  def lookupCachedData(query: Dataset[_]): Option[CachedData] = {
    lookupCachedData(query.queryExecution.normalized)
  }


  /*
      Partial match cases:
      InComingPlan (case of add cols)         cached plan      InComing Plan ( case of rename)
     Project P2                              Project P1         Project P2
       attr1                                  attr1               attr1
       attr2                                  attr2               Alias2'(x, attr2)
       Alias3                                 Alias3              Alias3'(y, Alias3-childExpr)
       Alias4                                 Alias4              Alias4'(z, Alias4-childExpr)
       Alias5 (k, f(attr1, attr2, al3, al4)
       Alias6 (p, f(attr1, attr2, al3, al4)
   */
  /** Optionally returns cached data for the given [[LogicalPlan]]. */
  def lookupCachedData(plan: LogicalPlan): Option[CachedData] = {
    val fullMatch = cachedData.find(cd => plan.sameResult(cd.plan))
    fullMatch.map(Option(_)).getOrElse(
      plan match {
        case p: Project => lookUpPartiallyMatchedCachedPlan(p, cachedData)
        case _ => None
      })
  }

  private def lookUpPartiallyMatchedCachedPlan(
        incomingProject: Project,
        cachedPlansToUse: IndexedSeq[CachedData]): Option[CachedData] = {
    var foundMatch = false
    var partialMatch: Option[CachedData] = None
    val (incmngchild, incomingFilterChain) =
      CompatibilityChecker.extractChildIgnoringFiltersFromIncomingProject(incomingProject)
    for (cd <- cachedPlansToUse if !foundMatch) {
      (incmngchild, incomingFilterChain, cd.plan) match {
        case CompatibilityChecker(residualIncomingFilterChain, cdPlanProject) =>
          // since the child of both incoming and cached plan are same
          // that is why we are here. for mapping and comparison purposes lets
          // canonicalize the cachedPlan's project list in terms of the incoming plan's child
          // so that we can map correctly.
          val cdPlanToIncomngPlanChildOutputMapping =
            cdPlanProject.child.output.zip(incmngchild.output).toMap

          val canonicalizedCdProjList = cdPlanProject.projectList.map(_.transformUp {
            case attr: Attribute => cdPlanToIncomngPlanChildOutputMapping(attr)
          }.asInstanceOf[NamedExpression])

          // matchIndexInCdPlanProj remains  -1 in the end, it indicates it is
          // new cols created out of existing output attribs
          val (directlyMappedincomingToCachedPlanIndx, inComingProjNoDirectMapping) =
          getDirectAndIndirectMappingOfIncomingToCachedProjectAttribs(
            incomingProject, canonicalizedCdProjList)

          // Now there is a possible case where a literal is present in IMR as attribute
          // and the incoming project also has that literal somewhere in the alias. Though
          // we do not need to read it but looks like the deserializer fails if we skip that
          // literal in the projection enforced on IMR. so in effect even if we do not
          // require an attribute it still needs to be present in the projection forced
          // also its possible that some attribute from IMR can be used in subexpression
          // of the incoming projection. so we have to handle that
          val unusedAttribsOfCDPlanToGenIncomingAttr =
          cdPlanProject.projectList.indices.filterNot(i =>
            directlyMappedincomingToCachedPlanIndx.exists(_._2 == i)).map(i => {
            val cdAttrib = cdPlanProject.projectList(i)
            i -> AttributeReference(cdAttrib.name, cdAttrib.dataType,
              cdAttrib.nullable, cdAttrib.metadata)(qualifier = cdAttrib.qualifier)
          })

          // Because in case of rename multiple incmong named exprs ( attribute or aliases)
          // will point to a common cdplan attrib, we need to ensure they do not create
          // separate attribute in the the modifiedProject for incoming plan..
          // that is a single attribute ref is present in all mixes of rename and  pass thru
          // attributes.
          // so we will use the first attribute ref in the incoming directly mapped project
          // or if no attrib exists ( only case of rename) we will pick the child expr which
          // is bound to be an attribute as the common ref.
          val cdAttribToCommonAttribForIncmngNe = directlyMappedincomingToCachedPlanIndx.map {
            case (inAttribIndex, cdAttribIndex) =>
              cdPlanProject.projectList(cdAttribIndex).toAttribute ->
                incomingProject.projectList(inAttribIndex)
          }.groupBy(_._1).map {
            case (cdAttr, incomngSeq) =>
              val incmngCommonAttrib = incomngSeq.map(_._2).flatMap {
                case attr: Attribute => Seq(attr)
                case Alias(attr: Attribute, _) => Seq(attr)
                case _ => Seq.empty
              }.headOption.getOrElse(
                AttributeReference(cdAttr.name, cdAttr.dataType, cdAttr.nullable)())
              cdAttr -> incmngCommonAttrib
          }

          // If expressions of inComingProjNoDirectMapping can be expressed in terms of the
          // incoming attribute refs or incoming alias exprs,  which can be mapped directly
          // to the CachedPlan's output, we are good. so lets transform such indirectly
          // mappable named expressions in terms of mappable attributes of the incoming plan
          val transformedIndirectlyMappableExpr =
          transformIndirectlyMappedExpressionsToUseCachedPlanAttributes(
            inComingProjNoDirectMapping, incomingProject, cdPlanProject,
            directlyMappedincomingToCachedPlanIndx, cdAttribToCommonAttribForIncmngNe,
            unusedAttribsOfCDPlanToGenIncomingAttr, canonicalizedCdProjList)

          val projectionToForceOnCdPlan = cdPlanProject.output.zipWithIndex.map {
            case (cdAttr, i) =>
              cdAttribToCommonAttribForIncmngNe.getOrElse(cdAttr,
                unusedAttribsOfCDPlanToGenIncomingAttr.find(_._1 == i).map(_._2).get)
          }
          val forcedAttribset = AttributeSet(projectionToForceOnCdPlan)
          if (transformedIndirectlyMappableExpr.forall(
            _._2.references.subsetOf(forcedAttribset))) {
            val transformedIntermediateFilters = transformFilters(residualIncomingFilterChain,
              projectionToForceOnCdPlan, canonicalizedCdProjList)
            if (transformedIntermediateFilters.forall(_.references.subsetOf(forcedAttribset))) {
              val modifiedInProj = replacementProjectListForIncomingProject(incomingProject,
                directlyMappedincomingToCachedPlanIndx, cdPlanProject,
                cdAttribToCommonAttribForIncmngNe, transformedIndirectlyMappableExpr)
              // If InMemoryRelation (right is defined) it is the case of lookup or cache query
              // Else it is a case of dummy CachedData partial lookup for finding out if the
              // plan being checked uses the uncached plan
              val newPartialPlan = if (cd.cachedRepresentation.isRight) {
                val root = cd.cachedRepresentation.toOption.get.withOutput(
                  projectionToForceOnCdPlan)
                if (transformedIntermediateFilters.isEmpty) {
                  Project(modifiedInProj, root)
                } else {
                  val chainedFilter = CompatibilityChecker.combineFilterChainUsingRoot(
                    transformedIntermediateFilters, root)
                  Project(modifiedInProj, chainedFilter)
                }
              } else {
                cd.cachedRepresentation.left.toOption.get
              }
              partialMatch = Option(cd.copy(cachedRepresentation = Left(newPartialPlan)))
              foundMatch = true
            }
          }
        case _ =>
      }
    }
    partialMatch
  }

  private def transformFilters(skippedFilters: Seq[Filter],
      projectionToForceOnCdPlan: Seq[Attribute],
      canonicalizedCdProjList: Seq[NamedExpression]): Seq[Filter] = {
    val canonicalizedCdProjAsExpr = canonicalizedCdProjList.map {
      case Alias(child, _) => child
      case x => x
    }
    skippedFilters.map(f => {
      val transformedCondn = f.condition.transformDown {
        case expr => val matchedIndex = canonicalizedCdProjAsExpr.indexWhere(_ == expr)
          if (matchedIndex != -1) {
            projectionToForceOnCdPlan(matchedIndex)
          } else {
            expr
          }
      }
      f.copy(condition = transformedCondn)
    })
  }

  private def replacementProjectListForIncomingProject(
      incomingProject: Project,
      directlyMappedincomingToCachedPlanIndx: Seq[(Int, Int)],
      cdPlanProject: Project,
      cdAttribToCommonAttribForIncmngNe: Map[Attribute, Attribute],
      transformedIndirectlyMappableExpr: Map[Int, NamedExpression]): Seq[NamedExpression] =
  {
    incomingProject.projectList.zipWithIndex.map {
      case (ne, indx) =>
        directlyMappedincomingToCachedPlanIndx.find(_._1 == indx).map {
          case (_, cdIndex) =>
            ne match {
              case attr: Attribute => attr
              case al: Alias =>
                val cdAttr = cdPlanProject.projectList(cdIndex).toAttribute
                al.copy(child = cdAttribToCommonAttribForIncmngNe(cdAttr))(
                  exprId = al.exprId, qualifier = al.qualifier,
                  explicitMetadata = al.explicitMetadata,
                  nonInheritableMetadataKeys = al.nonInheritableMetadataKeys
                )
            }
        }.getOrElse({
          transformedIndirectlyMappableExpr(indx)
        })
    }
  }

  private def transformIndirectlyMappedExpressionsToUseCachedPlanAttributes(
      inComingProjNoDirectMapping: Seq[(Int, Int)],
      incomingProject: Project,
      cdPlanProject: Project,
      directlyMappedincomingToCachedPlanIndx: Seq[(Int, Int)],
      cdAttribToCommonAttribForIncmngNe: Map[Attribute, Attribute],
      unusedAttribsOfCDPlanToGenIncomingAttr: Seq[(Int, AttributeReference)],
      canonicalizedCdProjList: Seq[NamedExpression]): Map[Int, NamedExpression] =
  {
    inComingProjNoDirectMapping.map {
      case (incomngIndex, _) =>
        val indirectIncmnNe = incomingProject.projectList(incomngIndex)
        val modifiedNe = indirectIncmnNe.transformDown {
          case expr => directlyMappedincomingToCachedPlanIndx.find {
            case (incomingIndex, _) =>
              val directMappedNe = incomingProject.projectList(incomingIndex)
              directMappedNe.toAttribute == expr ||
                directMappedNe.children.headOption.contains(expr)
          }.map {
            case (_, cdIndex) =>
              val cdAttrib = cdPlanProject.projectList(cdIndex).toAttribute
              cdAttribToCommonAttribForIncmngNe(cdAttrib)
          }.orElse(
            unusedAttribsOfCDPlanToGenIncomingAttr.find {
              case (i, _) => val cdNe = canonicalizedCdProjList(i)
                cdNe.children.headOption.contains(expr)
            }.map(_._2)).
            map(ne => ne.toAttribute).getOrElse(expr)
        }.asInstanceOf[NamedExpression]

        incomngIndex -> modifiedNe
    }.toMap
  }

  private def getDirectAndIndirectMappingOfIncomingToCachedProjectAttribs(
      incomingProject: Project,
      canonicalizedCdProjList: Seq[NamedExpression]): (Seq[(Int, Int)], Seq[(Int, Int)]) =
  {
    incomingProject.projectList.zipWithIndex.map {
      case (inComingNE, index) =>
        // first check for equivalent named expressions..if index is != -1, that means
        // it is pass thru Alias or pass thru - Attribute
        var matchIndexInCdPlanProj = canonicalizedCdProjList.indexWhere(_ == inComingNE)
        if (matchIndexInCdPlanProj == -1) {
          // if match index is -1, that means it could be two possibilities:
          // 1) it is a case of rename which means the incoming expr is an alias and
          // its child is an attrib ref, which may have a direct attribref in the
          // cdPlanProj, or it may actually have an alias whose ref matches the ref
          // of incoming attribRef
          // 2) the positions in the incoming project alias and the cdPlanProject are
          // different. as a result the canonicalized alias of each would have
          // relatively different exprIDs ( as their relative positions differ), but
          // even in such cases as their child logical plans are same, so the child
          // expression of each alias will have same canonicalized data
          val incomingExprToCheck = inComingNE match {
            case x: AttributeReference => x
            case Alias(expr, _) => expr
          }
          matchIndexInCdPlanProj = canonicalizedCdProjList.indexWhere {
            case Alias(expr, _) => expr == incomingExprToCheck
            case x => x == incomingExprToCheck
          }
        }
        index -> matchIndexInCdPlanProj
    }.partition(_._2 != -1)
  }

  /** Replaces segments of the given logical plan with cached versions where possible. */
  def useCachedData(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan transformDown {
      case command: IgnoreCachedData => command

      case currentFragment if !currentFragment.isInstanceOf[InMemoryRelation] =>
        lookupCachedData(currentFragment).map { cached =>
          // After cache lookup, we should still keep the hints from the input plan.
          val hints = EliminateResolvedHint.extractHintsFromPlan(currentFragment)._2
          val cachedPlan = cached.cachedRepresentation.map(_.withOutput(currentFragment.output)).
            merge

          // The returned hint list is in top-down order, we should create the hint nodes from
          // right to left.
          hints.foldRight[LogicalPlan](cachedPlan) { case (hint, p) =>
            ResolvedHint(p, hint)
          }
        }.getOrElse(currentFragment)
    }

    newPlan.transformAllExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
      case s: SubqueryExpression => s.withNewPlan(useCachedData(s.plan))
    }
  }

  /**
   * Tries to re-cache all the cache entries that contain `resourcePath` in one or more
   * `HadoopFsRelation` node(s) as part of its logical plan.
   */
  def recacheByPath(spark: SparkSession, resourcePath: String): Unit = {
    val path = new Path(resourcePath)
    val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
    recacheByPath(spark, path, fs)
  }

  /**
   * Tries to re-cache all the cache entries that contain `resourcePath` in one or more
   * `HadoopFsRelation` node(s) as part of its logical plan.
   */
  def recacheByPath(spark: SparkSession, resourcePath: Path, fs: FileSystem): Unit = {
    val qualifiedPath = fs.makeQualified(resourcePath)
    recacheByCondition(spark, _.plan.exists(lookupAndRefresh(_, fs, qualifiedPath)))
  }

  /**
   * Traverses a given `plan` and searches for the occurrences of `qualifiedPath` in the
   * [[org.apache.spark.sql.execution.datasources.FileIndex]] of any [[HadoopFsRelation]] nodes
   * in the plan. If found, we refresh the metadata and return true. Otherwise, this method returns
   * false.
   */
  private def lookupAndRefresh(plan: LogicalPlan, fs: FileSystem, qualifiedPath: Path): Boolean = {
    plan match {
      case lr: LogicalRelation => lr.relation match {
        case hr: HadoopFsRelation =>
          refreshFileIndexIfNecessary(hr.location, fs, qualifiedPath)
        case _ => false
      }

      case DataSourceV2Relation(fileTable: FileTable, _, _, _, _) =>
        refreshFileIndexIfNecessary(fileTable.fileIndex, fs, qualifiedPath)

      case _ => false
    }
  }

  /**
   * Refresh the given [[FileIndex]] if any of its root paths is a subdirectory
   * of the `qualifiedPath`.
   * @return whether the [[FileIndex]] is refreshed.
   */
  private def refreshFileIndexIfNecessary(
      fileIndex: FileIndex,
      fs: FileSystem,
      qualifiedPath: Path): Boolean = {
    val needToRefresh = fileIndex.rootPaths
      .map(_.makeQualified(fs.getUri, fs.getWorkingDirectory))
      .exists(isSubDir(qualifiedPath, _))
    if (needToRefresh) fileIndex.refresh()
    needToRefresh
  }

  /**
   * Checks if the given child path is a sub-directory of the given parent path.
   * @param qualifiedPathChild:
   *   Fully qualified child path
   * @param qualifiedPathParent:
   *   Fully qualified parent path.
   * @return
   *   True if the child path is a sub-directory of the given parent path. Otherwise, false.
   */
  def isSubDir(qualifiedPathParent: Path, qualifiedPathChild: Path): Boolean = {
    Iterator
      .iterate(qualifiedPathChild)(_.getParent)
      .takeWhile(_ != null)
      .exists(_.equals(qualifiedPathParent))
  }

  /**
   * If `CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING` is enabled, return the session with disabled
   * `ADAPTIVE_EXECUTION_APPLY_FINAL_STAGE_SHUFFLE_OPTIMIZATIONS`.
   * `AUTO_BUCKETED_SCAN_ENABLED` is always disabled.
   */
  private def getOrCloneSessionWithConfigsOff(session: SparkSession): SparkSession = {
    // Bucketed scan only has one time overhead but can have multi-times benefits in cache,
    // so we always do bucketed scan in a cached plan.
    var disableConfigs = Seq(SQLConf.AUTO_BUCKETED_SCAN_ENABLED)
    if (!session.conf.get(SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING)) {
      // Allowing changing cached plan output partitioning might lead to regression as it introduces
      // extra shuffle
      disableConfigs =
        disableConfigs :+ SQLConf.ADAPTIVE_EXECUTION_APPLY_FINAL_STAGE_SHUFFLE_OPTIMIZATIONS
    }
    SparkSession.getOrCloneSessionWithConfigsOff(session, disableConfigs)
  }
}

object CacheManager {
  val inMemoryRelationExtractor: LogicalPlan => InMemoryRelation =
    plan => plan.collectLeaves().head.asInstanceOf[InMemoryRelation]
}

object CompatibilityChecker {
  def unapply(data: (LogicalPlan, Seq[Filter], LogicalPlan)): Option[(Seq[Filter], Project)] = {
    val(incomingChild, incomingFilterChain, cachedPlan) = data
    cachedPlan match {
      case p: Project if incomingChild.sameResult(p.child) => Option(incomingFilterChain -> p)

      case f: Filter =>
        val collectedFilters = mutable.ListBuffer[Filter](f)
        var projectFound: Option[Project] = None
        var child: LogicalPlan = f.child
        var keepChecking = true
        while (keepChecking) {
          child match {
            case x: Filter => child = x.child
              collectedFilters += x
            case p: Project => projectFound = Option(p)
              keepChecking = false
            case _ => keepChecking = false
          }
        }
        if (collectedFilters.size <= incomingFilterChain.size &&
          projectFound.exists(_.child.sameResult(incomingChild))) {
          val (residualIncomingFilterChain, otherFilterChain) = incomingFilterChain.splitAt(
            incomingFilterChain.size - collectedFilters.size)
          val isCompatible = if (otherFilterChain.isEmpty) {
            true
          } else {
            // the other filter chain must be equal to the collected filter chain
            // But we need to transform the collected Filter chain such that it is below
            // the project of the cached plan, we have found, as the incoming filters are also below
            // the incoming project.
            val mappingFilterExpr = AttributeMap(projectFound.get.projectList.flatMap {
              case _: Attribute => Seq.empty[(Attribute, (NamedExpression, Expression))]
              case al: Alias => Seq(al.toAttribute -> (al, al.child))
            })

            val modifiedCdFilters = collectedFilters.map(f =>
              f.copy(condition = EarlyCollapseProject.expressionRemapper(
                f.condition, mappingFilterExpr))).toSeq
            val chainedFilter1 = combineFilterChainUsingRoot(otherFilterChain,
              EmptyRelation(incomingChild.output))
            val chainedFilter2 = combineFilterChainUsingRoot(modifiedCdFilters,
              EmptyRelation(projectFound.map(_.child).get.output))
            chainedFilter1.sameResult(chainedFilter2)
          }
          if (isCompatible) {
            Option(residualIncomingFilterChain -> projectFound.get)
          } else {
            None
          }
        } else {
          None
        }

      case _ => None
    }
  }

  def combineFilterChainUsingRoot(filters: Seq[Filter], root: LogicalPlan): Filter = {
    val lastFilterNode = filters.last
    val lastFilterMod = lastFilterNode.copy(child = root)
    filters.dropRight(1).foldRight(lastFilterMod)((f, c) => f.copy(child = c))
  }

  def extractChildIgnoringFiltersFromIncomingProject(incomingProject: Project):
      (LogicalPlan, Seq[Filter]) = {
    val collectedFilters = mutable.ListBuffer[Filter]()
    var child: LogicalPlan = incomingProject.child
    var keepChecking = true
    while (keepChecking) {
      child match {
        case f: Filter => child = f.child
          collectedFilters += f
        case _ => keepChecking = false
      }
    }
    (child, collectedFilters.toSeq)
  }

  case class EmptyRelation(output: Seq[Attribute]) extends LeafNode {
    override def maxRows: Option[Long] = Some(0)
  }
}
