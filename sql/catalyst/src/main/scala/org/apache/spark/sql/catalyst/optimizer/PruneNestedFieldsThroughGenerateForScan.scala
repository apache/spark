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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.types._

/**
 * Prunes nested struct fields within arrays used by explode/posexplode generators,
 * materializing pruned arrays as a Project below the Generate node so that downstream
 * [[org.apache.spark.sql.execution.datasources.SchemaPruning]] and
 * V2ScanRelationPushDown can reduce scan IO.
 *
 * This rule handles the multi-field case that [[GeneratorNestedColumnAliasing]] does
 * not support (see SPARK-34956), and additionally provides:
 *  - Posexplode pos-only optimisation (minimal-weight field selection)
 *  - Ordinal-safe field resolution using field names instead of ordinal reuse
 *  - Chained Generate support (multiple consecutive lateral views)
 *
 * The rule runs in the earlyScanPushDownRules batch, before SchemaPruning and
 * V2ScanRelationPushDown, so that the materialised fields are visible through
 * [[org.apache.spark.sql.catalyst.planning.ScanOperation]].
 *
 * === Example (single Generate) ===
 * Before (multi-field on generator output, no scan pruning):
 * {{{
 * Project [item.f1, item.f2]
 *   Generate [explode(col)]            // col: array<struct<f1,f2,f3>>
 *     Scan [col]
 * }}}
 *
 * After this rule (pruned array materialised for SchemaPruning):
 * {{{
 * Project [item.f1, item.f2]           // ordinals fixed
 *   Generate [explode(_pruned)]        // _pruned: array<struct<f1,f2>>
 *     Project [_pruned = arrays_zip(col.f1, col.f2)]
 *       Scan [col]
 * }}}
 *
 * === Example (chained Generates) ===
 * Before:
 * {{{
 * Project [complex.col1, val]
 *   Generate [explode(complex.col2)]   // inner: explodes col2 array
 *     Generate [explode(arr)]          // outer: arr is array<struct<col1,col2,col3>>
 *       Scan [arr]
 * }}}
 *
 * After (col3 pruned from outer struct):
 * {{{
 * Project [complex.col1, val]          // ordinals fixed
 *   Generate [explode(complex.col2)]   // ordinals fixed in generator child
 *     Generate [explode(_pruned)]      // _pruned: array<struct<col1,col2>>
 *       Project [_pruned = arrays_zip(arr.col1, arr.col2)]
 *         Scan [arr]
 * }}}
 */
object PruneNestedFieldsThroughGenerateForScan
    extends Rule[LogicalPlan] with SQLConfHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.nestedSchemaPruningEnabled) return plan
    rewriteGenerateChains(plan)
  }

  /**
   * Represents a Generate node in a chain with its context.
   *
   * @param generate The Generate node
   * @param generator The ExplodeBase generator
   * @param colAttr The exploded element attribute
   * @param posAttrOpt The position attribute (for posexplode)
   * @param elementStruct The struct type of array elements
   * @param containsNull Whether the array can contain nulls
   * @param intermediateNodes Nodes between this Generate and the next one in the chain
   *                          (or the leaf). These are typically Projects from
   *                          GeneratorNestedColumnAliasing that need to be preserved
   *                          and have their expressions fixed when rebuilding.
   */
  private case class GenerateInfo(
      generate: Generate,
      generator: ExplodeBase,
      colAttr: Attribute,
      posAttrOpt: Option[Attribute],
      elementStruct: StructType,
      containsNull: Boolean,
      intermediateNodes: Seq[LogicalPlan] = Nil)

  /**
   * Main traversal that detects and rewrites Generate chains.
   *
   * We look for patterns:
   * - Project -> Generate -> ... (chain of Generates) -> leaf
   * - Project -> Filter -> Generate -> ... -> leaf
   *
   * For each chain, we collect required fields for each Generate and rewrite
   * bottom-up, inserting a single pruned Project at the leaf.
   *
   * Uses nested schema approach for requirement propagation, enabling inner
   * generate pruning by embedding inner requirements into outer schemas.
   */
  private def rewriteGenerateChains(plan: LogicalPlan): LogicalPlan = {
    plan.transformDown {
      // Pattern 1: Project directly above a Generate chain
      case p @ Project(projectList, child) if startsGenerateChain(child) =>
        tryRewriteChainWithNestedSchema(p, projectList, Nil, child, None)

      // Pattern 2: Project -> Filter -> Generate chain
      case p @ Project(projectList, f @ Filter(condition, child))
          if startsGenerateChain(child) =>
        tryRewriteChainWithNestedSchema(
          p, projectList, Seq(condition), child, Some(f))

      // Pattern 3: Project above a Generate that has an intermediate Project below
      case p @ Project(projectList, g: Generate)
          if !startsGenerateChain(g) && startsGenerateChainThroughProjects(g.child) =>
        tryRewriteChainWithNestedSchema(p, projectList, Nil, g, None)
    }
  }

  /**
   * Looks through Project and Filter nodes to check if there's a Generate chain.
   */
  private def startsGenerateChainThroughProjects(plan: LogicalPlan): Boolean = plan match {
    case g: Generate if startsGenerateChain(g) => true
    case Project(_, child) => startsGenerateChainThroughProjects(child)
    case Filter(_, child) => startsGenerateChainThroughProjects(child)
    case _ => false
  }

  /**
   * Checks if any generate in the chain explodes a field from another generate's output.
   * This is the case where nested schema propagation can enable additional pruning.
   *
   * Note: The chain is ordered top-to-bottom (closer to Project first, closer to Scan last).
   * So chain[0] may use output from chain[1], not vice versa.
   *
   * For a plan like:
   *   Project -> Generate(explode(req.items)) -> Generate(explode(pv.requests)) -> Scan
   * The chain is [inner, outer] where inner.source = req.items and outer.colAttr = req.
   *
   * We need to check if any generate's source comes from a LATER generate's output.
   */
  private def hasInnerGenerateFromOuter(chain: Seq[GenerateInfo]): Boolean = {
    if (chain.length <= 1) return false

    // For each generate (except the last), check if its source comes from a later generate's output
    chain.indices.dropRight(1).exists { i =>
      val currentInfo = chain(i)
      // Check if current's source is from a later generate's output
      extractRootAttribute(currentInfo.generator.child) match {
        case Some(rootAttr) =>
          // Look for a later generate whose colAttr matches this root
          chain.drop(i + 1).exists(_.colAttr.exprId == rootAttr.exprId)
        case None =>
          false
      }
    }
  }

  /**
   * Checks if a plan node starts a Generate chain we can potentially prune.
   */
  private def startsGenerateChain(plan: LogicalPlan): Boolean = plan match {
    case Generate(gen: ExplodeBase, _, _, _, _, _) =>
      val result = isArrayOfStruct(gen.child.dataType)
      result
    case _ => false
  }

  /**
   * Extracts a chain of Generate nodes from a plan.
   * Returns the chain (top-to-bottom: closest to Project first) and the leaf node below the chain.
   *
   * Looks through intermediate Projects and Filters to find consecutive Generate nodes.
   * For each Generate, captures the intermediate nodes (Projects, Filters) between it
   * and the next Generate (or leaf). This is needed to preserve and fix those nodes
   * during chain rewriting.
   */
  private def extractGenerateChain(plan: LogicalPlan): (Seq[GenerateInfo], LogicalPlan) = {

    // Helper to collect intermediate nodes (Projects, Filters) until we hit
    // a Generate or a leaf node. Returns (intermediates, next plan to process).
    def collectIntermediates(p: LogicalPlan): (Seq[LogicalPlan], LogicalPlan) = {
      p match {
        case g @ Generate(gen: ExplodeBase, _, _, _, _, _)
            if isArrayOfStruct(gen.child.dataType) =>
          // Found next Generate
          (Nil, g)
        case proj @ Project(_, child) =>
          val (rest, next) = collectIntermediates(child)
          (proj +: rest, next)
        case flt @ Filter(_, child) =>
          val (rest, next) = collectIntermediates(child)
          (flt +: rest, next)
        case other =>
          // Leaf node
          (Nil, other)
      }
    }

    plan match {
      case g @ Generate(gen: ExplodeBase, _, _, _, genOutput, child)
          if isDirectArrayOfStruct(gen.child.dataType) =>
        // Only include generates where the element type (after explode) is a struct.
        // For array<array<struct>>, the element is array<struct>, not struct - skip those.
        val (elementStruct, containsNull) = extractDirectStruct(gen.child.dataType)
        val colAttr = if (gen.position) genOutput(1) else genOutput.head
        val posAttrOpt = if (gen.position) Some(genOutput.head) else None

        // Collect intermediate nodes between this Generate and the next (or leaf)
        val (intermediates, nextPlan) = collectIntermediates(child)


        val info = GenerateInfo(g, gen, colAttr, posAttrOpt, elementStruct, containsNull,
          intermediateNodes = intermediates)

        // Continue extracting from the next plan (next Generate or leaf)
        val (childChain, leaf) = extractGenerateChain(nextPlan)
        (info +: childChain, leaf)

      // Handle nested array case (array<array<struct>>) for inner generate pruning.
      // After GNA transforms the outer generate to explode nested arrays, we get
      // a generate with array<array<struct>> source producing array<struct> elements.
      // We MUST include this in the chain to preserve it during rewriting, even though
      // we can't prune it directly (its elements are arrays, not structs).
      case g @ Generate(gen: ExplodeBase, _, _, _, genOutput, child)
          if isArrayOfStruct(gen.child.dataType) && !isDirectArrayOfStruct(gen.child.dataType) =>
        // Extract the innermost struct for tracking, but this generate can't be pruned directly
        val (innermostStruct, containsNull) = extractInnermostStruct(gen.child.dataType)
        val colAttr = if (gen.position) genOutput(1) else genOutput.head
        val posAttrOpt = if (gen.position) Some(genOutput.head) else None

        // Collect intermediate nodes between this Generate and the next (or leaf)
        val (intermediates, nextPlan) = collectIntermediates(child)


        // Mark this as a "nested array" generate by using a special empty StructType
        // This signals to the pruning logic that we can't prune this generate directly
        // but must preserve it in the chain.
        // We use the innermost struct so ordinal fixing can still work if needed.
        val info = GenerateInfo(g, gen, colAttr, posAttrOpt, innermostStruct, containsNull,
          intermediateNodes = intermediates)

        // Continue extracting from the next plan
        val (childChain, leaf) = extractGenerateChain(nextPlan)
        (info +: childChain, leaf)

      // If we start with a Generate that isn't array-of-struct, look through its child
      // This handles Pattern 3 where the top Generate is exploding a scalar array
      // (e.g., explode(l2.l3_f1) producing array<long>)
      case g: Generate =>
        extractGenerateChain(g.child)

      // If we start with non-Generate, look through to find the first Generate
      case Project(_, child) =>
        extractGenerateChain(child)

      case Filter(_, child) =>
        extractGenerateChain(child)

      case other =>
        (Nil, other)
    }
  }

  /**
   * Collects the nodes between chainStart and the first chain Generate (chain[0]).
   * These nodes need to be preserved and rebuilt after the chain rewrite.
   *
   * For example, if chainStart is a non-struct Generate with intermediate Projects/Filters
   * before chain[0], we need to collect [Generate, Project, Filter] and rebuild them later.
   *
   * @return Sequence of nodes from chainStart down to (but not including) targetGenerate
   */
  private def collectAboveChainNodes(
      chainStart: LogicalPlan,
      targetGenerate: Generate): Seq[LogicalPlan] = {
    val nodes = mutable.ArrayBuffer[LogicalPlan]()

    @scala.annotation.tailrec
    def collect(plan: LogicalPlan): Unit = {
      if (plan eq targetGenerate) return

      plan match {
        case g: Generate =>
          nodes += g
          collect(g.child)
        case p: Project =>
          nodes += p
          collect(p.child)
        case f: Filter =>
          nodes += f
          collect(f.child)
        case _ =>
          // Shouldn't reach here if targetGenerate is in the tree
      }
    }

    collect(chainStart)
    nodes.toSeq
  }

  /**
   * Collects expressions from intermediate nodes between the start plan and the target generate.
   * This captures expressions like `outer_elem.inner_array.inner_f1` that may exist in
   * intermediate Projects created by GeneratorNestedColumnAliasing.
   *
   * @param start The starting plan node
   * @param targetGenerate The Generate node to stop at
   * @return Expressions from intermediate Projects that may reference Generate outputs
   */
  private def collectIntermediateExpressions(
      start: LogicalPlan,
      targetGenerate: Generate): Seq[Expression] = {
    val exprs = mutable.ArrayBuffer[Expression]()

    def collect(plan: LogicalPlan): Unit = {
      if (plan eq targetGenerate) return

      plan match {
        case g: Generate =>
          // Look at expressions in the generator child (for ExplodeBase generators)
          g.generator match {
            case e: ExplodeBase => exprs += e.child
            case _ => // Other generators may not have a simple child expression
          }
          collect(g.child)

        case Project(projectList, child) =>
          // Collect expressions from this Project
          exprs ++= projectList
          collect(child)

        case Filter(condition, child) =>
          exprs += condition
          collect(child)

        case _ =>
          plan.children.foreach(collect)
      }
    }

    collect(start)
    exprs.toSeq
  }

  /**
   * Attempts to rewrite a Generate chain using nested schema propagation.
   * This enables inner generate pruning by embedding inner requirements into outer schemas.
   */
  private def tryRewriteChainWithNestedSchema(
      originalProject: Project,
      projectList: Seq[NamedExpression],
      filterConditions: Seq[Expression],
      chainStart: LogicalPlan,
      filterOpt: Option[Filter]): LogicalPlan = {

    val (chain, leaf) = extractGenerateChain(chainStart)
    if (chain.isEmpty) return originalProject

    // Collect nodes between chainStart and chain[0] that need to be preserved.
    // These are rebuilt after the chain rewrite using rebuildAboveChainNodes.
    val aboveChainNodes = collectAboveChainNodes(chainStart, chain.head.generate)

    // Collect expressions from intermediate nodes between the top Project and the chain
    // This captures nested array field accesses that may exist in intermediate Projects
    // (e.g., from GeneratorNestedColumnAliasing)
    val intermediateExprs = collectIntermediateExpressions(chainStart, chain.head.generate)

    // Compute nested schema requirements with backward propagation
    val allTopExprs = projectList ++ filterConditions ++ intermediateExprs
    val requirements = computeNestedChainRequirements(chain, allTopExprs, Nil, leaf, chainStart)

    // Check if any pruning is possible (top-level count reduction OR nested type changes)
    val anyPruning = requirements.zip(chain).exists { case (req, info) =>
      req.isDefined && hasNestedTypePruning(req.get, info.elementStruct)
    }

    if (!anyPruning) return originalProject

    // Rewrite using nested schema builder
    rewriteChainWithNestedSchema(
      originalProject, projectList, filterConditions, chain, requirements, leaf, filterOpt,
      aboveChainNodes, chainStart)
  }

  /**
   * Rewrites the Generate chain using nested schema requirements.
   * This is the schema-driven replacement for rewriteChainBottomUp.
   *
   * @param aboveChainNodes Nodes between originalProject and chain[0] that need to be
   *                        preserved and rebuilt after the chain rewrite
   */
  /**
   * Traces an expression through generate outputs and aliases to find a scan-rooted source.
   * Returns the resolved expression if it can be traced to a scan attribute.
   */
  private def traceToScanRootedExpr(
      expr: Expression,
      generateSourceMap: Map[ExprId, Expression],
      aliasMap: Map[ExprId, Expression],
      scanAttrIds: Set[ExprId],
      visited: Set[ExprId] = Set.empty): Option[Expression] = {


    // Check if this expression is scan-rooted
    extractRootAttribute(expr) match {
      case Some(rootAttr) if scanAttrIds.contains(rootAttr.exprId) =>
        Some(expr)
      case Some(rootAttr) if visited.contains(rootAttr.exprId) =>
        None // Cycle detected
      case Some(rootAttr) =>
        // Try tracing through generate sources
        generateSourceMap.get(rootAttr.exprId) match {
          case Some(sourceExpr) =>
            traceToScanRootedExpr(sourceExpr, generateSourceMap, aliasMap, scanAttrIds,
              visited + rootAttr.exprId).map { resolvedSource =>
              // Rebuild the path with the resolved source
              rebuildExprWithNewBase(expr, rootAttr, resolvedSource)
            }
          case None =>
            // Try tracing through aliases
            aliasMap.get(rootAttr.exprId) match {
              case Some(aliasedExpr) =>
                traceToScanRootedExpr(aliasedExpr, generateSourceMap, aliasMap, scanAttrIds,
                  visited + rootAttr.exprId).map { resolvedAlias =>
                  rebuildExprWithNewBase(expr, rootAttr, resolvedAlias)
                }
              case None =>
                None
            }
        }
      case None =>
        None
    }
  }

  /**
   * Rebuilds an expression by replacing its base attribute with a new expression.
   * For example, rebuildExprWithNewBase(outer_elem.inner_f1, outer_elem, outer_array.inner_array)
   * returns outer_array.inner_array.inner_f1
   */
  private def rebuildExprWithNewBase(
      expr: Expression,
      oldBase: Attribute,
      newBase: Expression): Expression = {
    expr match {
      case a: Attribute if a.exprId == oldBase.exprId =>
        newBase
      case gsf @ GetStructField(child, ordinal, name) =>
        GetStructField(rebuildExprWithNewBase(child, oldBase, newBase), ordinal, name)
      case gasf @ GetArrayStructFields(child, field, ordinal, numFields, containsNull) =>
        GetArrayStructFields(rebuildExprWithNewBase(child, oldBase, newBase),
          field, ordinal, numFields, containsNull)
      case _ =>
        expr
    }
  }

  private def rewriteChainWithNestedSchema(
      originalProject: Project,
      projectList: Seq[NamedExpression],
      filterConditions: Seq[Expression],
      chain: Seq[GenerateInfo],
      requirements: Seq[Option[StructType]],
      leaf: LogicalPlan,
      filterOpt: Option[Filter],
      aboveChainNodes: Seq[LogicalPlan] = Seq.empty,
      chainStart: LogicalPlan = null): LogicalPlan = {

    // Decompose the leaf to find where to insert our pruning Project
    val (leafFilters, actualLeaf) = decomposeChild(leaf)

    // Find the TRUE scan relation to determine scan attribute IDs.
    // We can't use actualLeaf.output because actualLeaf might be an intermediate
    // Project (e.g., from GeneratorNestedColumnAliasing) that defines alias attributes.
    // Those alias attributes would be incorrectly treated as "scan-rooted".
    val scanLeaf = findScanLeaf(leaf)
    val scanAttrIds = scanLeaf.output.map(_.exprId).toSet

    // Collect generate sources and aliases from the FULL plan (not just chain).
    // This allows tracing through generates that aren't in the chain (e.g., outer
    // generates with array<array<struct>> sources that were skipped).
    val chainStartPlan = if (chainStart != null) chainStart else originalProject
    val generateSourceMap = collectGenerateSources(chainStartPlan)
    val fullAliasMap = collectAliasDefinitions(chainStartPlan)


    // Find the outermost Generate that can be pruned AND has a scan-rooted source.
    // We trace through BOTH aliases AND generate outputs to find the ultimate scan source.
    // For inner generates (i < chain.length - 1), we only support GNA-transformed cases
    // where the outer generate produces array elements.
    val outermostPrunableIdx = chain.indices.find { i =>
      val info = chain(i)
      val rootAttrOpt = extractRootAttribute(info.generator.child)
      val hasScanRootedSource = rootAttrOpt match {
        case Some(rootAttr) =>
          // First check direct match
          scanAttrIds.contains(rootAttr.exprId) ||
            // Then try tracing through chain aliases
            traceToScanAttribute(rootAttr, chain, scanAttrIds).isDefined ||
            // Finally try tracing through all generates and aliases
            traceToScanRootedExpr(info.generator.child, generateSourceMap, fullAliasMap,
              scanAttrIds).isDefined
        case None =>
          // Even if root is not a simple attribute, try tracing the full expression
          traceToScanRootedExpr(info.generator.child, generateSourceMap, fullAliasMap,
            scanAttrIds).isDefined
      }
      val canPrune = requirements(i).isDefined &&
        hasNestedTypePruning(requirements(i).get, info.elementStruct)

      // For inner generates, check if we can support this case
      // (GNA-transformed = outer gen produces array elements)
      val hasGeneratesBelow = i < chain.length - 1
      val canPruneInnerGen = if (hasGeneratesBelow) {
        val outerGenInfo = chain(i + 1)
        outerGenInfo.colAttr.dataType match {
          case _: ArrayType => true   // GNA-transformed case
          case _: StructType => false // Non-GNA - skip, try outer generate
          case _ => false
        }
      } else {
        true // Bottommost generate - always supported
      }

      hasScanRootedSource && canPrune && canPruneInnerGen
    }

    outermostPrunableIdx match {
      case None => originalProject
      case Some(prunableIdx) =>
        val prunableInfo = chain(prunableIdx)
        val requiredSchema = requirements(prunableIdx).get

        if (!hasNestedTypePruning(requiredSchema, prunableInfo.elementStruct)) {
          return originalProject
        }

        // Note: GNA vs non-GNA check is now in outermostPrunableIdx.find predicate
        val hasGeneratesBelow = prunableIdx < chain.length - 1

        // Resolve the source expression to a scan-rooted path for SchemaPruning
        val rawSourceExpr: Expression = prunableInfo.generator.child
        val resolvedSourceExpr: Expression = traceToScanRootedExpr(
          rawSourceExpr, generateSourceMap, fullAliasMap, scanAttrIds
        ).getOrElse(resolveExpressionThroughChain(rawSourceExpr, chain))
        val sourceRootAttr: Option[Attribute] = extractRootAttribute(resolvedSourceExpr)


        val prunedArrayExpr = buildPrunedArrayFromSchema(
          resolvedSourceExpr,
          prunableInfo.elementStruct,
          requiredSchema,
          prunableInfo.containsNull)


        val prunedAlias = Alias(prunedArrayExpr, "_pruned_explode")()
        val prunedAttr = prunedAlias.toAttribute

        // When the source expression is resolved (e.g., outer_array.inner_array),
        // we need to insert the pruned Project at the scan level where those attributes
        // are available. If the resolved expression references scan attributes, use scanLeaf;
        // otherwise fall back to actualLeaf for compatibility.
        val insertionPoint = sourceRootAttr match {
          case Some(rootAttr) if scanAttrIds.contains(rootAttr.exprId) => scanLeaf
          case _ => actualLeaf
        }

        // Compute pass-through attributes from the insertion point
        val aboveReferences: Set[Attribute] =
          (projectList.flatMap(_.references) ++
            filterConditions.flatMap(_.references)).toSet
        val allGenOutputs = AttributeSet(chain.flatMap(_.generate.generatorOutput))
        val childAttrsNeededAbove: Seq[Attribute] = insertionPoint.output
          .filter(a => aboveReferences.exists(r =>
            r.exprId == a.exprId && !allGenOutputs.contains(r)))

        // Inner Project directly above the insertion point
        val innerProject = Project(childAttrsNeededAbove :+ prunedAlias, insertionPoint)


        // Rewrite leaf filters
        val rewrittenLeaf = sourceRootAttr match {
          case Some(_) =>
            leafFilters.foldLeft(innerProject: LogicalPlan) { (plan, cond) =>
              val rewritten = fixArrayStructOrdinalsInExprChain(
                cond, rawSourceExpr, prunedAttr, requiredSchema, prunableInfo.containsNull)
              Filter(rewritten, plan)
            }
          case None =>
            leafFilters.foldLeft(innerProject: LogicalPlan) { (plan, cond) =>
              Filter(cond, plan)
            }
        }

        // Build new generator for the prunable Generate.
        // GNA-transformed: outer gen explodes array<array<struct>>, produces array<struct>
        // Non-GNA: outer gen explodes array<struct>, produces struct (early return above)
        val prunableGenSource = if (hasGeneratesBelow) {
          val outerGenInfo = chain(prunableIdx + 1)
          outerGenInfo.colAttr.dataType match {
            case _: ArrayType =>
              // GNA-transformed case: outer gen produces array elements, use colAttr directly
              outerGenInfo.colAttr
            case _: StructType =>
              // Non-GNA case: outer gen produces struct elements
              // We use prunedAttr - the pruned version of inner_array at scan level
              prunedAttr
          }
        } else {
          // Bottommost prunable generate - use prunedAttr directly
          prunedAttr
        }
        val newGenerator: ExplodeBase = prunableInfo.generator match {
          case _: Explode => Explode(prunableGenSource)
          case _: PosExplode => PosExplode(prunableGenSource)
        }

        // Updated generator output with new pruned schema
        val newGenOutput = prunableInfo.generate.generatorOutput
          .zip(toAttributes(newGenerator.elementSchema)).map {
            case (oldAttr, newAttr) =>
              newAttr.withExprId(oldAttr.exprId).withName(oldAttr.name)
          }

        // Build the rewritten chain from the prunable Generate upward.
        // Intermediate nodes (Projects, Filters) are stored with each Generate and
        // represent nodes BELOW that Generate (between it and the next one or the leaf).
        // We rebuild bottom-up: for each Generate, first add its intermediate nodes,
        // then add the Generate itself.
        var currentChild: LogicalPlan = rewrittenLeaf

        // Track colAttr -> new schema mappings for all generates that got updated.
        // Initialize early so intermediate node rebuilds can use it.
        val colAttrSchemaMap = mutable.Map[ExprId, StructType]()
        colAttrSchemaMap(prunableInfo.colAttr.exprId) = requiredSchema

        // Build a map of attribute types to update (colAttr exprId -> new type)
        // Initialize with the prunable generate's output type
        val attrTypeMap = mutable.Map[ExprId, DataType]()
        val prunableNewColType = prunableInfo.generator.child.dataType match {
          case ArrayType(ArrayType(_, innerContainsNull), _) =>
            ArrayType(requiredSchema, innerContainsNull)
          case ArrayType(_, _) =>
            requiredSchema
          case _ =>
            requiredSchema
        }
        attrTypeMap(prunableInfo.colAttr.exprId) = prunableNewColType

        // Rebuild Generates below the prunable one (if any).
        // These are typically GNA-transformed generates that we traced THROUGH to reach the scan.
        // They need to use the pruned output (_pruned_explode) instead of their original source.
        for (i <- (chain.length - 1) until prunableIdx by -1) {
          val info = chain(i)

          // For the BOTTOMMOST generate (directly above the scan-level Project), we:
          // 1. SKIP its intermediate nodes (they define the unpruned extraction)
          // 2. Use the pruned attribute as its source instead
          // This is analogous to how we skip prunableInfo's intermediate nodes
          val isBottommostGenerate = (i == chain.length - 1)

          if (!isBottommostGenerate) {
            // Not the bottommost - rebuild its intermediate nodes normally
            currentChild = rebuildIntermediateNodes(
              info.intermediateNodes, currentChild, colAttrSchemaMap.toMap, chain, attrTypeMap)
          }
          // If isBottommostGenerate, SKIP intermediate nodes - our pruned Project replaces them

          // For the bottommost generate, use the pruned attribute as source.
          // For others, fix ordinals in their generator child.
          val newGenChild = if (isBottommostGenerate) {
            // Use the pruned attribute directly - it has the correct pruned schema
            prunedAttr
          } else {
            // Fix ordinals based on any updated schemas
            fixOrdinalsInExprWithSchema(
              info.generator.child, prunableInfo.colAttr, requiredSchema)
          }

          val fixedGenerator: ExplodeBase = info.generator match {
            case _: Explode => Explode(newGenChild)
            case _: PosExplode => PosExplode(newGenChild)
          }

          // Update the generate's output schema.
          // For the bottommost generate with nested array source (array<array<struct>>),
          // the output is one level unwrapped: array<struct> with the pruned inner schema.
          val newGenOutput = {
            val newElementSchema = fixedGenerator.elementSchema
            info.generate.generatorOutput.zip(toAttributes(newElementSchema)).map {
              case (oldAttr, newAttr) =>
                newAttr.withExprId(oldAttr.exprId).withName(oldAttr.name)
            }
          }

          // Track the new colAttr schema for downstream rebuilding
          val newColAttr = if (info.generator.position) newGenOutput(1) else newGenOutput.head
          newColAttr.dataType match {
            case st: StructType =>
              colAttrSchemaMap(info.colAttr.exprId) = st
            case ArrayType(st: StructType, _) =>
              // For nested array generates, the colAttr is array<struct>
              colAttrSchemaMap(info.colAttr.exprId) = st
            case _ => // Non-struct output, skip
          }


          currentChild = info.generate.copy(
            generator = fixedGenerator,
            generatorOutput = newGenOutput,
            child = currentChild)
        }

        // SKIP the prunable Generate's intermediate nodes - they were inserted by GNA to
        // extract the source array (e.g., outer_array.inner_array), but our pruned Project
        // already provides the pruned array directly as _pruned_explode. The intermediate
        // extraction is now redundant.
        //
        // If we were to include them, they would:
        // 1. Reference the old source (outer_array.inner_array) which still has all 4 fields
        // 2. Block ScanOperation from matching our pruned Project
        // 3. Leave orphaned nodes with unresolved references
        //
        // By skipping them, the Generate uses _pruned_explode directly above our pruned Project.

        // Now add the prunable Generate with updated output
        val prunedIdx = childAttrsNeededAbove.length
        val prunableGenerate = prunableInfo.generate.copy(
          generator = newGenerator,
          unrequiredChildIndex = Seq(prunedIdx),
          generatorOutput = newGenOutput,
          child = currentChild)
        currentChild = prunableGenerate

        // Rebuild Generates above the prunable one (closer to Project).
        // These may reference the prunable generate's output, so we need to:
        // 1. Fix ordinals in their generator child
        // 2. Update attribute types in their generator child
        // 3. Update their generatorOutput to match the new element schema
        // 4. Rebuild intermediate nodes between this Generate and the one below it
        //
        // Note: attrTypeMap was already initialized earlier with the prunable generate's type

        for (i <- (prunableIdx - 1) to 0 by -1) {
          val info = chain(i)

          // IMPORTANT: Rebuild intermediate nodes FIRST so that alias types are tracked
          // in attrTypeMap BEFORE we try to update the generate's expression.
          // This is critical because the inner generate's source (e.g., _extract_inner_array#19)
          // is defined by an alias in the intermediate Project. We need that alias's new type
          // in attrTypeMap before we can update the inner generate's generator child.
          currentChild = rebuildIntermediateNodes(
            info.intermediateNodes, currentChild, colAttrSchemaMap.toMap, chain, attrTypeMap)

          // Now fix ordinals in generator child (with updated attrTypeMap from intermediates)
          var fixedGenChild = fixOrdinalsInExprWithSchema(
            info.generator.child, prunableInfo.colAttr, requiredSchema)
          // Also update attribute types in generator child
          fixedGenChild = updateAttributeTypes(fixedGenChild, attrTypeMap.toMap)

          val fixedGenerator: ExplodeBase = info.generator match {
            case _: Explode => Explode(fixedGenChild)
            case _: PosExplode => PosExplode(fixedGenChild)
          }

          // The inner generate's output schema is derived from the fixed generator's
          // element schema. If the source array was pruned, the element schema changes.
          val newGenOutput = info.generate.generatorOutput
            .zip(toAttributes(fixedGenerator.elementSchema)).map {
              case (oldAttr, newAttr) =>
                newAttr.withExprId(oldAttr.exprId).withName(oldAttr.name)
            }

          // Track the new schema for this generate's colAttr
          val newColAttr = if (info.generator.position) newGenOutput(1) else newGenOutput.head
          newColAttr.dataType match {
            case st: StructType =>
              colAttrSchemaMap(info.colAttr.exprId) = st
              // Also update the type map for intermediate nodes above this generate
              val newColType = fixedGenerator.child.dataType match {
                case ArrayType(ArrayType(_, innerContainsNull), _) =>
                  ArrayType(st, innerContainsNull)
                case ArrayType(_, _) =>
                  st
                case _ =>
                  st
              }
              attrTypeMap(info.colAttr.exprId) = newColType
            case _ => // Non-struct output, skip
          }

          // Add this Generate (intermediate nodes already added above)
          currentChild = info.generate.copy(
            generator = fixedGenerator,
            generatorOutput = newGenOutput,
            child = currentChild)
        }

        // Rebuild above-chain nodes (nodes between originalProject and chain[0])
        // These need their expressions fixed for the new schema

        currentChild = rebuildAboveChainNodes(
          aboveChainNodes, currentChild, colAttrSchemaMap.toMap, chain, attrTypeMap)


        // Fix ordinals in the top-level expressions for ALL updated generates
        val fixedProjectList = projectList.map { expr =>
          colAttrSchemaMap.foldLeft(expr: Expression) { case (e, (exprId, schema)) =>
            val colAttr = chain.find(_.colAttr.exprId == exprId).map(_.colAttr).get
            fixOrdinalsInExprWithSchema(e, colAttr, schema)
          }.asInstanceOf[NamedExpression]
        }

        filterOpt match {
          case Some(filter) =>
            val fixedCond = colAttrSchemaMap.foldLeft(filter.condition: Expression) {
              case (e, (exprId, schema)) =>
                val colAttr = chain.find(_.colAttr.exprId == exprId).map(_.colAttr).get
                fixOrdinalsInExprWithSchema(e, colAttr, schema)
            }
            Project(fixedProjectList, Filter(fixedCond, currentChild))
          case None =>
            Project(fixedProjectList, currentChild)
        }
    }
  }

  /**
   * Rebuilds the nodes between the original top Project and chain[0].
   * These nodes include non-struct Generates, Projects, and Filters that reference
   * chain outputs and need their expressions fixed.
   *
   * Rebuilds in reverse order (bottom-to-top: closest to chain first).
   */
  private def rebuildAboveChainNodes(
      aboveChainNodes: Seq[LogicalPlan],
      currentChild: LogicalPlan,
      colAttrSchemaMap: Map[ExprId, StructType],
      chain: Seq[GenerateInfo],
      attrTypeMap: mutable.Map[ExprId, DataType]): LogicalPlan = {

    if (aboveChainNodes.isEmpty) return currentChild

    // Rebuild in reverse order (bottom-to-top)
    aboveChainNodes.reverse.foldLeft(currentChild) { (child, node) =>
      node match {
        case g: Generate =>
          // Fix ordinals and types in generator child
          var fixedGenChild: Expression = g.generator match {
            case e: ExplodeBase =>
              var expr: Expression = e.child
              // Fix ordinals for struct field access
              expr = colAttrSchemaMap.foldLeft(expr) { case (ex, (exprId, schema)) =>
                chain.find(_.colAttr.exprId == exprId).map(_.colAttr) match {
                  case Some(colAttr) => fixOrdinalsInExprWithSchema(ex, colAttr, schema)
                  case None => ex
                }
              }
              // Update attribute types
              expr = updateAttributeTypes(expr, attrTypeMap.toMap)
              expr
            case other => other.children.headOption.getOrElse(Literal(null))
          }
          val fixedGenerator = g.generator match {
            case _: Explode => Explode(fixedGenChild)
            case _: PosExplode => PosExplode(fixedGenChild)
            case other => other // Keep as-is for other generator types
          }

          // Recalculate unrequiredChildIndex based on which child attributes are consumed
          // by the generator. The generator's source is the array being exploded.
          val genSourceRefs = fixedGenChild.references
          val newUnrequiredIdx = child.output.zipWithIndex.collect {
            case (attr, idx) if genSourceRefs.exists(_.exprId == attr.exprId) => idx
          }

          g.copy(
            generator = fixedGenerator,
            unrequiredChildIndex = newUnrequiredIdx,
            child = child)

        case p: Project =>
          // Fix ordinals and types in project expressions
          val fixedProjectList = p.projectList.map { expr =>
            var e: Expression = expr
            e = colAttrSchemaMap.foldLeft(e) { case (ex, (exprId, schema)) =>
              chain.find(_.colAttr.exprId == exprId).map(_.colAttr) match {
                case Some(colAttr) => fixOrdinalsInExprWithSchema(ex, colAttr, schema)
                case None => ex
              }
            }
            e = updateAttributeTypes(e, attrTypeMap.toMap)
            // Track alias types for downstream
            e match {
              case alias: Alias => attrTypeMap(alias.exprId) = alias.dataType
              case _ =>
            }
            e.asInstanceOf[NamedExpression]
          }
          Project(fixedProjectList, child)

        case f: Filter =>
          // Fix ordinals and types in filter condition
          var fixedCond: Expression = f.condition
          fixedCond = colAttrSchemaMap.foldLeft(fixedCond) { case (ex, (exprId, schema)) =>
            chain.find(_.colAttr.exprId == exprId).map(_.colAttr) match {
              case Some(colAttr) =>
                fixOrdinalsInExprWithSchema(ex, colAttr, schema)
              case None => ex
            }
          }
          fixedCond = updateAttributeTypes(fixedCond, attrTypeMap.toMap)
          Filter(fixedCond, child)

        case other =>
          // For other node types, just update the child
          other.withNewChildren(Seq(child))
      }
    }
  }

  /**
   * Fixes ordinals in expressions using a nested StructType schema.
   * This handles both direct field access and nested struct/array access.
   */
  private def fixOrdinalsInExprWithSchema(
      expr: Expression,
      colAttr: Attribute,
      newSchema: StructType): Expression = {

    // For GetStructField: colAttr should have StructType
    val structColAttr = colAttr.withDataType(newSchema)

    // For GetArrayStructFields: colAttr should have ArrayType(StructType)
    // Preserve the containsNull from the original array type if available
    val arrayContainsNull = colAttr.dataType match {
      case ArrayType(_, cn) => cn
      case _ => true
    }
    val arrayColAttr = colAttr.withDataType(ArrayType(newSchema, arrayContainsNull))

    // Transform bottom-up to properly propagate type changes
    expr.transformUp {
      // GetStructField directly on colAttr
      case gsf @ GetStructField(child, _, _) if isAttrRef(child, colAttr) =>
        val fieldName = gsf.extractFieldName
        if (newSchema.fieldNames.contains(fieldName)) {
          val newOrdinal = newSchema.fieldIndex(fieldName)
          gsf.copy(child = structColAttr, ordinal = newOrdinal)
        } else {
          gsf
        }

      // GetArrayStructFields directly on colAttr - use newSchema parameter
      // because child.dataType is still the OLD type (not yet transformed)
      case gasf @ GetArrayStructFields(child, field, oldOrdinal, _, containsNull)
          if isAttrRef(child, colAttr) =>
        if (newSchema.fieldNames.contains(field.name)) {
          val newOrdinal = newSchema.fieldIndex(field.name)
          val newField = newSchema(newOrdinal)
          gasf.copy(
            child = arrayColAttr,
            field = newField,
            ordinal = newOrdinal,
            numFields = newSchema.length)
        } else {
          gasf
        }

      // GetArrayStructFields on a chain rooted at colAttr (not directly on colAttr)
      // For example: l1.level2.level3 where l1.level2 was already fixed above
      // Here we can use child.dataType since the child has already been transformed
      case gasf @ GetArrayStructFields(child, field, oldOrdinal, _, containsNull)
          if !isAttrRef(child, colAttr) && isColAttrChainChild(child, colAttr) =>
        child.dataType match {
          case ArrayType(innerStruct: StructType, _) =>
            if (innerStruct.fieldNames.contains(field.name)) {
              val newOrdinal = innerStruct.fieldIndex(field.name)
              val newField = innerStruct(newOrdinal)
              gasf.copy(
                field = newField,
                ordinal = newOrdinal,
                numFields = innerStruct.length)
            } else {
              gasf
            }
          case _ => gasf
        }

      // GetStructField on a result that comes from our colAttr chain
      // For example: l1.level2.some_struct_field where level2 contains a struct
      case gsf @ GetStructField(child, oldOrdinal, _)
          if !isAttrRef(child, colAttr) && isColAttrChainChild(child, colAttr) =>
        child.dataType match {
          case innerStruct: StructType =>
            val fieldName = gsf.extractFieldName
            if (innerStruct.fieldNames.contains(fieldName)) {
              val newOrdinal = innerStruct.fieldIndex(fieldName)
              gsf.copy(ordinal = newOrdinal)
            } else {
              gsf
            }
          case _ => gsf
        }

      // GetNestedArrayStructFields directly on colAttr
      // (for nested arrays like array<array<struct>>)
      case gnasf @ GetNestedArrayStructFields(child, field, oldOrdinal, _, containsNull)
          if isAttrRef(child, colAttr) =>
        // For nested arrays, we need to wrap newSchema in the appropriate array nesting
        val nestedArrayColAttr = colAttr.dataType match {
          case at: ArrayType =>
            // Preserve original array nesting, just update innermost struct
            def updateInnermostStruct(dt: DataType): DataType = dt match {
              case ArrayType(inner: ArrayType, cn) =>
                ArrayType(updateInnermostStruct(inner), cn)
              case ArrayType(_: StructType, cn) =>
                ArrayType(newSchema, cn)
              case other => other
            }
            colAttr.withDataType(updateInnermostStruct(at))
          case _ => colAttr
        }
        if (newSchema.fieldNames.contains(field.name)) {
          val newOrdinal = newSchema.fieldIndex(field.name)
          val newField = newSchema(newOrdinal)
          gnasf.copy(
            child = nestedArrayColAttr,
            field = newField,
            ordinal = newOrdinal,
            numFields = newSchema.length)
        } else {
          gnasf
        }

      // GetNestedArrayStructFields on a chain rooted at colAttr
      case gnasf @ GetNestedArrayStructFields(child, field, oldOrdinal, _, containsNull)
          if !isAttrRef(child, colAttr) && isColAttrChainChild(child, colAttr) =>
        // Extract innermost struct from child's nested array type
        def getInnermostStruct(dt: DataType): Option[StructType] = dt match {
          case ArrayType(inner: ArrayType, _) => getInnermostStruct(inner)
          case ArrayType(st: StructType, _) => Some(st)
          case _ => None
        }
        getInnermostStruct(child.dataType) match {
          case Some(innerStruct) if innerStruct.fieldNames.contains(field.name) =>
            val newOrdinal = innerStruct.fieldIndex(field.name)
            val newField = innerStruct(newOrdinal)
            gnasf.copy(
              field = newField,
              ordinal = newOrdinal,
              numFields = innerStruct.length)
          case _ => gnasf
        }
    }
  }

  /**
   * Checks if an expression is a chain of field accesses rooted at the given attribute.
   * For example, GetStructField(GetStructField(colAttr, ...), ...) would return true.
   */
  private def isColAttrChainChild(expr: Expression, colAttr: Attribute): Boolean = {
    expr match {
      case a: Attribute => a.exprId == colAttr.exprId
      case gsf: GetStructField => isColAttrChainChild(gsf.child, colAttr)
      case gasf: GetArrayStructFields => isColAttrChainChild(gasf.child, colAttr)
      case gnasf: GetNestedArrayStructFields => isColAttrChainChild(gnasf.child, colAttr)
      case _ => false
    }
  }

  /**
   * Rebuilds intermediate nodes (Projects, Filters) between Generates.
   * These nodes need their expressions fixed when the schema changes.
   *
   * The nodes are stored top-to-bottom (closer to the Generate first),
   * so we rebuild them in reverse order (bottom-to-top).
   *
   * Important: We must update BOTH:
   * 1. GetStructField ordinals (handled by fixOrdinalsInExprWithSchema)
   * 2. AttributeReference data types (for aliases like `Alias(outer_elem, "_extract")`)
   *
   * When we update a Generate's output schema, any aliases that reference that
   * output need their AttributeReference types updated so downstream consumers
   * (like inner generates) see the correct pruned schema.
   *
   * This function also updates the attrTypeMap with alias output types, so that
   * attributes defined by these aliases can be updated in subsequent generates.
   *
   * @param intermediateNodes The intermediate nodes to rebuild
   * @param currentChild The plan to use as the child of the bottommost intermediate node
   * @param colAttrSchemaMap Map of colAttr exprId -> new schema for ordinal fixes
   * @param chain The full chain (for looking up colAttr by exprId)
   * @param attrTypeMap Mutable map of attribute types to update (also receives new alias types)
   * @return The rebuilt plan with intermediate nodes above currentChild
   */
  private def rebuildIntermediateNodes(
      intermediateNodes: Seq[LogicalPlan],
      currentChild: LogicalPlan,
      colAttrSchemaMap: Map[ExprId, StructType],
      chain: Seq[GenerateInfo],
      attrTypeMap: mutable.Map[ExprId, DataType]): LogicalPlan = {

    // Rebuild in reverse order (bottom-to-top)
    intermediateNodes.reverse.foldLeft(currentChild) { (child, node) =>
      node match {
        case Project(projectList, _) =>
          // Fix ordinals and attribute types in all expressions
          val fixedProjectList = projectList.map { expr =>
            var e: Expression = expr
            // First, fix ordinals for GetStructField
            e = colAttrSchemaMap.foldLeft(e) { case (ex, (exprId, schema)) =>
              chain.find(_.colAttr.exprId == exprId).map(_.colAttr) match {
                case Some(colAttr) => fixOrdinalsInExprWithSchema(ex, colAttr, schema)
                case None => ex
              }
            }
            // Then, update AttributeReference types
            e = updateAttributeTypes(e, attrTypeMap.toMap)

            // Track alias output types for downstream use
            e match {
              case alias: Alias =>
                // The alias's output type is derived from its child's type
                // Add it to the type map so attributes referencing this alias can be updated
                attrTypeMap(alias.exprId) = alias.dataType
              case _ =>
            }

            e.asInstanceOf[NamedExpression]
          }
          Project(fixedProjectList, child)

        case Filter(condition, _) =>
          // Fix ordinals and attribute types in the condition
          var fixedCondition = colAttrSchemaMap.foldLeft(condition) {
            case (e, (exprId, schema)) =>
              chain.find(_.colAttr.exprId == exprId).map(_.colAttr) match {
                case Some(colAttr) => fixOrdinalsInExprWithSchema(e, colAttr, schema)
                case None => e
              }
          }
          fixedCondition = updateAttributeTypes(fixedCondition, attrTypeMap.toMap)
          Filter(fixedCondition, child)

        case other =>
          // Unexpected node type - preserve as-is
          other.withNewChildren(Seq(child))
      }
    }
  }

  /**
   * Updates AttributeReference data types in an expression tree.
   * This is needed when a Generate's output schema changes - any aliases
   * or expressions that reference the generator's output attributes need
   * their types updated.
   *
   * @param expr The expression to update
   * @param attrTypeMap Map of exprId -> new dataType
   * @return Expression with updated attribute types
   */
  private def updateAttributeTypes(
      expr: Expression,
      attrTypeMap: Map[ExprId, DataType]): Expression = {
    expr.transformDown {
      case attr: Attribute if attrTypeMap.contains(attr.exprId) =>
        attr.withDataType(attrTypeMap(attr.exprId))
    }
  }

  // ---------------------------------------------------------------------------
  //  Helpers
  // ---------------------------------------------------------------------------

  /**
   * Checks if a data type is an array containing structs at any nesting level.
   * Returns true for `array<struct>`, `array<array<struct>>`, etc.
   */
  private def isArrayOfStruct(dt: DataType): Boolean = dt match {
    case ArrayType(_: StructType, _) => true
    case ArrayType(elementType: ArrayType, _) => isArrayOfStruct(elementType)
    case _ => false
  }

  /**
   * Checks if a data type is a DIRECT array of struct (not nested arrays).
   * Returns true only for `array<struct>`, false for `array<array<struct>>`.
   *
   * This is important for chain extraction: when exploding array<array<struct>>,
   * the output element is array<struct>, not struct, so it's not a prunable
   * struct access.
   */
  private def isDirectArrayOfStruct(dt: DataType): Boolean = dt match {
    case ArrayType(_: StructType, _) => true
    case _ => false
  }

  /**
   * Extracts the direct struct type from an array<struct> type.
   * Unlike extractInnermostStruct, this does NOT dig into nested arrays.
   */
  private def extractDirectStruct(dt: DataType): (StructType, Boolean) = dt match {
    case ArrayType(st: StructType, containsNull) => (st, containsNull)
    case _ => throw new IllegalArgumentException(s"Expected array<struct>, got: $dt")
  }

  /**
   * Extracts the innermost struct type and containsNull from a nested array type.
   * For `array<struct>`, returns the struct directly.
   * For `array<array<struct>>`, returns the innermost struct.
   */
  private def extractInnermostStruct(dt: DataType): (StructType, Boolean) = dt match {
    case ArrayType(st: StructType, containsNull) => (st, containsNull)
    case ArrayType(elementType: ArrayType, _) => extractInnermostStruct(elementType)
    case _ => throw new IllegalArgumentException(s"Expected nested array of struct, got: $dt")
  }

  private def isAttrRef(e: Expression, attr: Attribute): Boolean = e match {
    case a: Attribute => a.exprId == attr.exprId
    case _ => false
  }

  /**
   * Checks if the required schema has any pruning compared to the original schema.
   * Currently only checks top-level field count reduction.
   *
   * Note: We intentionally only check top-level field count, not nested types,
   * because the rewrite logic for nested type changes is complex and not fully
   * implemented for all cases (e.g., inner generate pruning through chained generates).
   */
  private def hasNestedTypePruning(req: StructType, orig: StructType): Boolean = {
    // Check top-level field count reduction
    if (req.length < orig.length) return true

    // Check if any field has a pruned nested type
    req.fields.exists { reqField =>
      orig.fields.find(_.name == reqField.name) match {
        case Some(origField) =>
          hasNestedDataTypePruning(reqField.dataType, origField.dataType)
        case None =>
          false // Field not in original (shouldn't happen)
      }
    }
  }

  /**
   * Recursively checks if a data type has been pruned compared to the original.
   * Returns true if the required type is "smaller" than the original.
   */
  private def hasNestedDataTypePruning(req: DataType, orig: DataType): Boolean = {
    (req, orig) match {
      case (reqSt: StructType, origSt: StructType) =>
        hasNestedTypePruning(reqSt, origSt)
      case (ArrayType(reqElem, _), ArrayType(origElem, _)) =>
        hasNestedDataTypePruning(reqElem, origElem)
      case _ =>
        false
    }
  }

  /**
   * Extracts the root attribute from an expression that is either:
   * 1. A direct Attribute
   * 2. A chain of GetStructField expressions rooted at an Attribute
   *
   * This allows pruning for patterns like `explode(col.structArr)` where
   * the array is nested inside a struct column from the scan.
   *
   * Returns None for complex expressions (e.g., UDFs, computations).
   */
  private def extractRootAttribute(expr: Expression): Option[Attribute] = {
    expr match {
      case a: Attribute => Some(a)
      case GetStructField(child, _, _) => extractRootAttribute(child)
      case GetArrayStructFields(child, _, _, _, _) => extractRootAttribute(child)
      case _ => None
    }
  }

  /**
   * If the expression is an Attribute that's an alias defined in the leaf Project,
   * resolve it to the aliased expression. This allows SchemaPruning to trace
   * through the alias chain to the original scan attributes.
   */
  private def resolveAliasInLeaf(expr: Expression, leaf: LogicalPlan): Expression = {
    expr match {
      case attr: Attribute =>
        leaf match {
          case Project(projectList, _) =>
            // Find the alias definition in the project list
            projectList.collectFirst {
              case a @ Alias(child, _) if a.exprId == attr.exprId => child
            }.getOrElse(attr)
          case _ => attr
        }
      case other => other
    }
  }

  /**
   * Traces an attribute through intermediate Projects to find its ultimate scan source.
   * This is needed because GeneratorNestedColumnAliasing may have extracted array fields
   * into aliases. For example:
   *   - Generate source: _extract_inner_array#21
   *   - Intermediate Project defines: _extract_inner_array = outer_array.inner_array
   *   - This function returns: outer_array (the scan attribute)
   *
   * @param attr The attribute to trace
   * @param chain The generate chain (contains intermediate nodes)
   * @param scanAttrIds Set of scan attribute IDs
   * @return The ultimate scan-rooted attribute, or None if can't be traced
   */
  private def traceToScanAttribute(
      attr: Attribute,
      chain: Seq[GenerateInfo],
      scanAttrIds: Set[ExprId]): Option[Attribute] = {

    // If already a scan attribute, return it
    if (scanAttrIds.contains(attr.exprId)) {
      return Some(attr)
    }


    // Look through all intermediate Projects in the chain for alias definitions
    val allIntermediates = chain.flatMap(_.intermediateNodes)
    allIntermediates.foreach {
      case proj @ Project(projectList, _) =>
        projectList.foreach {
          case a @ Alias(child, _) if a.exprId == attr.exprId =>
            // Found the alias definition. Trace the child expression.
            extractRootAttribute(child) match {
              case Some(childAttr) =>
                // Recursively trace
                val traced = traceToScanAttribute(childAttr, chain, scanAttrIds)
                if (traced.isDefined) return traced
              case None =>
                // Child is a complex expression. Try GetArrayStructFields.
                child match {
                  case gasf: GetArrayStructFields =>
                    extractRootAttribute(gasf.child) match {
                      case Some(gasfRootAttr) =>
                        val traced = traceToScanAttribute(gasfRootAttr, chain, scanAttrIds)
                        if (traced.isDefined) return traced
                      case None =>
                    }
                  case _ =>
                }
            }
          case _ =>
        }
      case other =>
    }

    // Couldn't trace to scan
    None
  }

  /**
   * Resolves an expression through intermediate Projects to find its scan-rooted form.
   * Similar to traceToScanAttribute but returns the full expression path.
   *
   * For example:
   *   - Input: _extract_inner_array#21
   *   - Intermediate: _extract_inner_array = outer_array.inner_array
   *   - Returns: outer_array.inner_array
   *
   * This resolved expression can be used to build GetArrayStructFields that
   * SchemaPruning can trace to scan attributes.
   */
  private def resolveExpressionThroughChain(
      expr: Expression,
      chain: Seq[GenerateInfo]): Expression = {

    expr match {
      case attr: Attribute =>
        // Look through all intermediate Projects for alias definitions
        val allIntermediates = chain.flatMap(_.intermediateNodes)
        allIntermediates.foreach {
          case Project(projectList, _) =>
            projectList.foreach {
              case a @ Alias(child, _) if a.exprId == attr.exprId =>
                // Found the alias definition. Recursively resolve the child.
                return resolveExpressionThroughChain(child, chain)
              case _ =>
            }
          case _ =>
        }
        // No alias found, return as-is
        attr

      case GetStructField(child, ordinal, name) =>
        // Resolve the child and rebuild
        val resolvedChild = resolveExpressionThroughChain(child, chain)
        GetStructField(resolvedChild, ordinal, name)

      case other =>
        // For other expressions, return as-is
        other
    }
  }

  // ---------------------------------------------------------------------------
  //  Nested schema requirement analysis (Step 11)
  // ---------------------------------------------------------------------------

  /**
   * Analyzes expressions to build a nested StructType representing required fields
   * from the given attribute. Unlike the flat Set[String] approach, this captures
   * the full nested structure needed for inner generate pruning.
   *
   * For example, if expressions access:
   *   - item.requestId
   *   - item.items (used by inner explode selecting item.itemId)
   *
   * This returns: struct<requestId: OriginalType, items: array<struct<itemId: OriginalType>>>
   *
   * @param exprs Expressions to analyze
   * @param colAttr The exploded element attribute
   * @param elementStruct The original struct type of array elements
   * @param chain Optional chain for resolving aliases (if provided)
   * @return None if the attribute is referenced directly (all fields needed),
   *         Some(StructType) with required fields otherwise
   */
  /**
   * Recursive nested requirements type. Maps field name to its nested requirements.
   * An empty map means "need this field with original type".
   * A non-empty map means "need this field but prune its contents to only these nested fields".
   */
  private type NestedReqs = mutable.Map[String, mutable.Map[String, Any]]

  /**
   * Extracts the full field path from a chained GetStructField/GetArrayStructFields expression.
   * For example, for `l1.level2.level3` returns Some(Seq("level2", "level3")).
   *
   * Also traces through generate outputs and aliases to find the ultimate path.
   * For example, if `l2` is the output of exploding `l1.level2.level3`, then
   * `l2.l3_f1` returns Some(Seq("level2", "level3", "l3_f1")).
   */
  private def extractFieldPath(
      expr: Expression,
      colAttr: Attribute,
      generateSourceMap: Map[ExprId, Expression] = Map.empty,
      aliasMap: Map[ExprId, Expression] = Map.empty): Option[Seq[String]] = {
    expr match {
      case gsf: GetStructField if isAttrRef(gsf.child, colAttr) =>
        Some(Seq(gsf.extractFieldName))
      case gsf: GetStructField =>
        extractFieldPath(gsf.child, colAttr, generateSourceMap, aliasMap)
          .map(_ :+ gsf.extractFieldName)
      case gasf: GetArrayStructFields if isAttrRef(gasf.child, colAttr) =>
        Some(Seq(gasf.field.name))
      case gasf: GetArrayStructFields =>
        extractFieldPath(gasf.child, colAttr, generateSourceMap, aliasMap)
          .map(_ :+ gasf.field.name)

      // Handle generate output attributes: l2.l3_f1 where l2 is from exploding an expression
      case attr: Attribute if generateSourceMap.contains(attr.exprId) =>
        val sourceExpr = generateSourceMap(attr.exprId)
        // Resolve through alias if needed
        val resolvedSource = resolveAliasExpr(sourceExpr, aliasMap)
        extractFieldPath(resolvedSource, colAttr, generateSourceMap, aliasMap)

      // Handle alias attributes: resolve to their definition
      case attr: Attribute if aliasMap.contains(attr.exprId) =>
        val aliasedExpr = aliasMap(attr.exprId)
        extractFieldPath(aliasedExpr, colAttr, generateSourceMap, aliasMap)

      case _ =>
        None
    }
  }

  /**
   * Resolves an expression through alias definitions.
   */
  private def resolveAliasExpr(
      expr: Expression,
      aliasMap: Map[ExprId, Expression]): Expression = {
    expr match {
      case attr: Attribute if aliasMap.contains(attr.exprId) =>
        resolveAliasExpr(aliasMap(attr.exprId), aliasMap)
      case gsf @ GetStructField(child, _, _) =>
        gsf.copy(child = resolveAliasExpr(child, aliasMap))
      case gasf @ GetArrayStructFields(child, field, ordinal, numFields, containsNull) =>
        GetArrayStructFields(resolveAliasExpr(child, aliasMap), field, ordinal,
          numFields, containsNull)
      case _ => expr
    }
  }

  /**
   * Collects all generate outputs and their source expressions from the plan.
   * This allows tracing through generate chains to understand field paths.
   */
  private def collectGenerateSources(
      plan: LogicalPlan): Map[ExprId, Expression] = {
    val sources = mutable.Map.empty[ExprId, Expression]

    def collect(p: LogicalPlan): Unit = {
      p match {
        case g: Generate =>
          g.generator match {
            case e: ExplodeBase =>
              // For explode/posexplode, the element output attribute comes from the source array
              val colAttr = if (e.position) g.generatorOutput(1) else g.generatorOutput.head
              sources(colAttr.exprId) = e.child
            case _ =>
          }
          collect(g.child)
        case _ =>
          p.children.foreach(collect)
      }
    }

    collect(plan)
    sources.toMap
  }

  /**
   * Adds a field path to nested requirements. Creates intermediate nodes as needed.
   */
  private def addPathToNestedReqs(
      reqs: mutable.Map[String, Any],
      path: Seq[String]): Unit = {
    if (path.isEmpty) return
    val field = path.head
    if (path.length == 1) {
      // Leaf of path - mark as needed (empty map means use original type)
      if (!reqs.contains(field)) {
        reqs(field) = mutable.Map.empty[String, Any]
      }
    } else {
      // Intermediate field - recurse into nested requirements
      val innerReqs = reqs.getOrElseUpdate(field, mutable.Map.empty[String, Any])
        .asInstanceOf[mutable.Map[String, Any]]
      addPathToNestedReqs(innerReqs, path.tail)
    }
  }

  /**
   * Unwraps nested ArrayTypes and returns the innermost struct along with
   * the containsNull flags at each array level (outermost first).
   *
   * @return (innerStruct, arrayNullFlags) or None if no struct found
   */
  private def unwrapArraysToStruct(dt: DataType): Option[(StructType, Seq[Boolean])] = {
    dt match {
      case st: StructType => Some((st, Seq.empty))
      case ArrayType(inner, containsNull) =>
        unwrapArraysToStruct(inner).map { case (st, flags) =>
          (st, containsNull +: flags)
        }
      case _ => None
    }
  }

  /**
   * Rewraps a struct type with the given array nesting levels.
   *
   * @param st The struct type to wrap
   * @param arrayNullFlags containsNull flags for each array level (outermost first)
   * @return The wrapped type (array<...array<struct>>)
   */
  private def rewrapWithArrays(st: StructType, arrayNullFlags: Seq[Boolean]): DataType = {
    arrayNullFlags.foldRight[DataType](st) { (containsNull, inner) =>
      ArrayType(inner, containsNull)
    }
  }

  /**
   * Builds a pruned StructField based on nested requirements.
   * Recursively prunes nested struct/array types, supporting arbitrary array nesting depth.
   */
  private def buildPrunedField(
      origField: StructField,
      nestedReqs: mutable.Map[String, Any]): StructField = {
    if (nestedReqs.isEmpty) {
      // Empty nested reqs means use original field as-is
      return origField
    }

    // Helper to prune a struct type based on requirements
    def pruneStructType(
        st: StructType,
        reqs: mutable.Map[String, Any]): (Array[StructField], Boolean) = {
      val prunedFields = st.fieldNames.flatMap { fname =>
        reqs.get(fname).map { innerReqsAny =>
          val innerReqs = innerReqsAny.asInstanceOf[mutable.Map[String, Any]]
          val innerOrigField = st(st.fieldIndex(fname))
          buildPrunedField(innerOrigField, innerReqs)
        }
      }
      val anyNestedPruning = prunedFields.zip(st.fields).exists {
        case (pruned, orig) => pruned.dataType != orig.dataType
      }
      val hasPruning = prunedFields.length < st.length || anyNestedPruning
      (prunedFields, hasPruning)
    }

    origField.dataType match {
      case st: StructType =>
        val (prunedFields, hasPruning) = pruneStructType(st, nestedReqs)
        if (hasPruning) {
          origField.copy(dataType = StructType(prunedFields))
        } else {
          origField
        }

      case _ =>
        // Try to unwrap arrays to find inner struct
        unwrapArraysToStruct(origField.dataType) match {
          case Some((innerStruct, arrayNullFlags)) if arrayNullFlags.nonEmpty =>
            val (prunedFields, hasPruning) = pruneStructType(innerStruct, nestedReqs)
            if (hasPruning) {
              val prunedType = rewrapWithArrays(StructType(prunedFields), arrayNullFlags)
              origField.copy(dataType = prunedType)
            } else {
              origField
            }

          case _ =>
            // Not a struct or array-of-struct - return as-is
            origField
        }
    }
  }

  private def analyzeRequiredSchema(
      exprs: Seq[Expression],
      colAttr: Attribute,
      elementStruct: StructType,
      chain: Seq[GenerateInfo] = Seq.empty,
      generateSourceMap: Map[ExprId, Expression] = Map.empty,
      fullAliasMap: Map[ExprId, Expression] = Map.empty): Option[StructType] = {

    // Track fields with their nested requirements as a tree structure.
    // For deeply nested access like l1.level2.level3.l3_f1, we track:
    //   level2 -> { level3 -> { l3_f1 -> {} } }
    // This allows pruning at all levels, not just one.
    val nestedReqs = mutable.Map.empty[String, Any]
    var canPrune = true

    // Build a map of alias definitions from intermediate nodes
    val aliasDefinitions: Map[ExprId, Expression] = {
      val allIntermediates = chain.flatMap(_.intermediateNodes)
      val intermediateAliases = allIntermediates.flatMap {
        case Project(projectList, _) =>
          projectList.collect {
            case a @ Alias(child, _) => a.exprId -> child
          }
        case _ => Seq.empty
      }.toMap
      // Merge with full alias map (from entire plan)
      fullAliasMap ++ intermediateAliases
    }

    def analyze(e: Expression): Unit = {
      if (!canPrune) return
      e match {
        // GetStructField on our element attribute (single level access)
        case gsf: GetStructField if isAttrRef(gsf.child, colAttr) =>
          val fieldName = gsf.extractFieldName
          if (elementStruct.fieldNames.contains(fieldName)) {
            // Single-level access: need the full field
            if (!nestedReqs.contains(fieldName)) {
              nestedReqs(fieldName) = mutable.Map.empty[String, Any]
            }
          }

        // GetArrayStructFields on our element attribute (single level access)
        case gasf: GetArrayStructFields if isAttrRef(gasf.child, colAttr) =>
          val fieldName = gasf.field.name
          if (elementStruct.fieldNames.contains(fieldName)) {
            if (!nestedReqs.contains(fieldName)) {
              nestedReqs(fieldName) = mutable.Map.empty[String, Any]
            }
          }

        // Chained GetStructField or GetArrayStructFields - extract full path
        // Now also traces through generate outputs and aliases
        case gsf: GetStructField =>
          extractFieldPath(gsf, colAttr, generateSourceMap, aliasDefinitions) match {
            case Some(path) if path.nonEmpty =>
              if (elementStruct.fieldNames.contains(path.head)) {
                addPathToNestedReqs(nestedReqs, path)
              }
            case _ =>
              // Not rooted at our colAttr, recurse
              gsf.children.foreach(analyze)
          }

        case gasf: GetArrayStructFields =>
          extractFieldPath(gasf, colAttr, generateSourceMap, aliasDefinitions) match {
            case Some(path) if path.nonEmpty =>
              if (elementStruct.fieldNames.contains(path.head)) {
                addPathToNestedReqs(nestedReqs, path)
              }
            case _ =>
              // Not rooted at our colAttr, recurse
              gasf.children.foreach(analyze)
          }

        // Direct reference to the element attribute - all fields needed
        case a: Attribute if a.exprId == colAttr.exprId =>
          canPrune = false

        // Alias reference - resolve and analyze the aliased expression
        case a: Attribute if aliasDefinitions.contains(a.exprId) =>
          val aliasedExpr = aliasDefinitions(a.exprId)
          analyze(aliasedExpr)

        case other =>
          other.children.foreach(analyze)
      }
    }

    exprs.foreach(analyze)

    if (!canPrune) {
      None
    } else if (nestedReqs.isEmpty) {
      // No fields needed - shouldn't happen but return empty struct
      Some(StructType(Seq.empty))
    } else {
      // Build the result schema with pruned nested arrays
      // CRITICAL: Order fields according to the original elementStruct order, not discovery order!
      // This ensures that ordinals in expressions (e.g., filter on b_int at ordinal 1) match
      // the actual ArraysZip output order (which also uses original struct order).
      val resultFields = elementStruct.fieldNames.flatMap { fieldName =>
        nestedReqs.get(fieldName).map { innerReqsAny =>
          val origField = elementStruct(elementStruct.fieldIndex(fieldName))
          val innerReqs = innerReqsAny.asInstanceOf[mutable.Map[String, Any]]
          buildPrunedField(origField, innerReqs)
        }
      }.toSeq
      Some(StructType(resultFields))
    }
  }

  /**
   * Merges an inner generate's required element schema into the outer generate's
   * required schema for the array field that the inner generate explodes.
   *
   * For example:
   *   - Outer generates elements with struct<requestId, ts, items: array<struct<...>>>
   *   - Inner explodes items, requiring struct<itemId, clicked>
   *   - Merged outer requirement: struct<requestId, ts, items: array<struct<itemId, clicked>>>
   *
   * Note: This REPLACES the existing field with the pruned version. The inner
   * requirements are MORE specific (fewer fields needed) than the original.
   *
   * @param outerSchema Current outer generate's required schema
   * @param innerSourceField The field name in outer's element that inner explodes
   * @param innerElementSchema The inner generate's required element schema
   * @param outerElementStruct The original outer element struct (for field metadata)
   * @return Schema with inner requirements embedded (field replaced, not merged)
   */
  private def mergeInnerRequirementsIntoOuter(
      outerSchema: StructType,
      innerSourceField: String,
      innerElementSchema: StructType,
      outerElementStruct: StructType): StructType = {
    // Backward-compatible wrapper
    mergeInnerRequirementsWithPath(
      outerSchema, Seq(innerSourceField), innerElementSchema, outerElementStruct)
  }

  /**
   * Merges inner generate's requirements into outer schema following a field path.
   *
   * For example, with:
   *   - outerElementStruct = struct<l1_field, level2: array<struct<l2_field, level3: array<...>>>>
   *   - fieldPath = Seq("level2", "level3")
   *   - innerElementSchema = struct<l3_f1>
   *
   * Returns a StructType representing:
   *   struct<level2: array<struct<level3: array<struct<l3_f1>>>>>
   *
   * This properly handles nested paths where inner generates access deeply nested arrays.
   */
  private def mergeInnerRequirementsWithPath(
      outerSchema: StructType,
      fieldPath: Seq[String],
      innerElementSchema: StructType,
      outerElementStruct: StructType): StructType = {


    if (fieldPath.isEmpty) {
      return outerSchema
    }

    val firstField = fieldPath.head
    val remainingPath = fieldPath.tail

    // Get the original field definition from the struct
    if (!outerElementStruct.fieldNames.contains(firstField)) {
      return outerSchema // Field not found, return unchanged
    }

    val origFieldIdx = outerElementStruct.fieldIndex(firstField)
    val origField = outerElementStruct(origFieldIdx)

    // Build the pruned data type
    val prunedDataType = if (remainingPath.isEmpty) {
      // At the leaf of the path - this is the array that the inner generate explodes
      // Handle arbitrary array nesting: array<...array<struct>> -> array<...array<prunedStruct>>
      unwrapArraysToStruct(origField.dataType) match {
        case Some((_, arrayNullFlags)) if arrayNullFlags.nonEmpty =>
          // Replace innermost struct with the inner element schema
          rewrapWithArrays(innerElementSchema, arrayNullFlags)
        case _ =>
          origField.dataType
      }
    } else {
      // Not at leaf - recurse into the nested array/struct
      // Handle arbitrary array nesting depth
      unwrapArraysToStruct(origField.dataType) match {
        case Some((innerStruct, arrayNullFlags)) if arrayNullFlags.nonEmpty =>
          // Recurse to build the nested pruned structure
          val nestedPruned = mergeInnerRequirementsWithPath(
            StructType(Seq.empty), remainingPath, innerElementSchema, innerStruct)
          rewrapWithArrays(nestedPruned, arrayNullFlags)
        case Some((innerStruct, _)) =>
          // Direct struct, no array wrapping
          mergeInnerRequirementsWithPath(
            StructType(Seq.empty), remainingPath, innerElementSchema, innerStruct)
        case None =>
          // Not an array of struct or struct - can't navigate deeper
          origField.dataType
      }
    }

    // Create the field with pruned type
    val prunedField = origField.copy(dataType = prunedDataType)

    // Check if the field already exists in outer schema
    val existingFieldOpt = outerSchema.fields.find(_.name == firstField)

    val result = existingFieldOpt match {
      case Some(_) =>
        // Replace the existing field with the pruned version
        StructType(outerSchema.fields.map { f =>
          if (f.name == firstField) prunedField else f
        })
      case None =>
        // Add the new field to outer schema
        StructType(outerSchema.fields :+ prunedField)
    }
    result
  }

  /**
   * Computes nested schema requirements for each Generate in the chain.
   * This is the key method for inner generate pruning (Step 12).
   *
   * Unlike the flat `computeChainRequirements` which returns `Set[String]`,
   * this returns a nested `StructType` per Generate that includes:
   * 1. Direct field accesses (e.g., item.requestId)
   * 2. Nested array fields with their own pruned element schema (for inner generates)
   *
   * The propagation works backward through the chain:
   * - Start with the innermost generate's requirements
   * - For each outer generate, merge inner requirements into its schema
   *
   * @param chain The sequence of GenerateInfo (top-to-bottom: closest to Project first)
   * @param inputExprs Expressions to analyze (from top-level project, filters, intermediate nodes)
   * @param additionalFilters Additional filter conditions
   * @param leaf The leaf node below the chain
   * @return Sequence of optional StructTypes (None = can't prune, Some = required schema)
   */
  /**
   * Collects all alias definitions from a plan tree.
   */
  private def collectAliasDefinitions(plan: LogicalPlan): Map[ExprId, Expression] = {
    val aliases = mutable.Map.empty[ExprId, Expression]

    def collect(p: LogicalPlan): Unit = {
      p match {
        case Project(projectList, child) =>
          projectList.foreach {
            case a @ Alias(childExpr, _) => aliases(a.exprId) = childExpr
            case _ =>
          }
          collect(child)
        case _ =>
          p.children.foreach(collect)
      }
    }

    collect(plan)
    aliases.toMap
  }

  private def computeNestedChainRequirements(
      chain: Seq[GenerateInfo],
      inputExprs: Seq[Expression],
      additionalFilters: Seq[Expression],
      leaf: LogicalPlan,
      chainStart: LogicalPlan): Seq[Option[StructType]] = {

    // Collect generate sources from the entire plan (to trace through generate outputs)
    val generateSourceMap = collectGenerateSources(chainStart)

    // Collect all alias definitions from the plan
    val fullAliasMap = collectAliasDefinitions(chainStart)


    // Extract filters from the leaf path
    val (leafFilters, _) = decomposeChild(leaf)

    // Collect expressions from ALL intermediate nodes in the chain
    // These include filters and projects between generates that may reference the source array
    //
    // IMPORTANT: Filter out direct attribute references from intermediate Projects.
    // When ColumnPruning inserts a pass-through Project like `Project(a_array_item, ...)`,
    // the `a_array_item` attribute reference is just a pass-through, NOT an indication
    // that all fields of the struct are needed. We only care about GetStructField accesses
    // from intermediate nodes, not direct attribute references.
    val intermediateNodeExprs = chain.flatMap { info =>
      info.intermediateNodes.flatMap {
        case Project(projectList, _) =>
          // Filter out direct attribute references (pass-throughs)
          projectList.filter {
            case _: Attribute => false
            case Alias(_: Attribute, _) => false
            case _ => true
          }
        case Filter(condition, _) => Seq(condition)
        case _ => Nil
      }
    }

    val topExprs: Seq[Expression] =
      inputExprs ++ additionalFilters ++ leafFilters ++ intermediateNodeExprs

    // First pass: compute direct requirements for each generate (without inner propagation)
    val directRequirements: Seq[Option[StructType]] = chain.indices.map { i =>
      val info = chain(i)

      // Collect expressions that may reference this generate's output
      val parentGenExprs = chain.take(i).flatMap { parentInfo =>
        Seq(parentInfo.generator.child)
      }
      val exprs = topExprs ++ parentGenExprs

      // Analyze direct requirements from expressions
      // Pass generate source map and alias map for tracing through generate chains
      val directOpt = analyzeRequiredSchema(
        exprs, info.colAttr, info.elementStruct, chain, generateSourceMap, fullAliasMap)

      // Also analyze source array field accesses
      val sourceRootAttr = extractRootAttribute(info.generator.child)
      val sourceFieldsOpt = analyzeSourceArrayFields(exprs, sourceRootAttr, info.generator.child)

      (directOpt, sourceFieldsOpt) match {
        case (None, _) => None  // Direct element reference - can't prune
        case (_, None) => None  // Complex source expression - can't prune
        case (Some(directSchema), Some(sourceFields)) =>
          // Merge source array field requirements into schema
          val withSourceFields = sourceFields.foldLeft(directSchema) { (schema, fieldName) =>
            if (info.elementStruct.fieldNames.contains(fieldName)) {
              val origField = info.elementStruct(info.elementStruct.fieldIndex(fieldName))
              if (schema.fieldNames.contains(fieldName)) {
                schema // Already present
              } else {
                StructType(schema.fields :+ origField)
              }
            } else {
              schema
            }
          }
          Some(withSourceFields)
      }
    }

    // Second pass: propagate requirements from generates closer to Project to those closer to Scan.
    // Chain is ordered top-to-bottom: [generate closest to Project, ..., generate closest to Scan]
    // If chain[i]'s source is from chain[j] (where j > i), merge chain[i]'s element requirements
    // into chain[j]'s schema for that array field.
    val propagatedRequirements = mutable.ArrayBuffer[Option[StructType]]()
    propagatedRequirements ++= directRequirements

    // Iterate from generates closest to Project (index 0) toward Scan
    for (i <- 0 until chain.length - 1) {
      val currentInfo = chain(i)
      val currentReqOpt = propagatedRequirements(i)


      currentReqOpt match {
        case Some(currentSchema) if currentSchema.nonEmpty =>
          // Find which source generate (at larger index) provides this generate's input
          val sourceIdx = findSourceGenerateForCurrent(chain, i, currentInfo)

          sourceIdx match {
            case Some(si) =>
              // Extract the full field path that current accesses from source's element
              val sourceFieldPath = extractSourceFieldPath(chain(si), currentInfo)
              val sourceStruct = chain(si).elementStruct

              if (sourceFieldPath.nonEmpty) {
                if (propagatedRequirements(si).isDefined) {
                  // Source already has requirements - merge inner's into it
                  val sourceSchema = propagatedRequirements(si).get
                  val merged = mergeInnerRequirementsWithPath(
                    sourceSchema, sourceFieldPath, currentSchema, sourceStruct)
                  propagatedRequirements(si) = Some(merged)
                } else {
                  // Source has no direct requirements (None).
                  // Initialize with inner's requirements for the accessed path.
                  val emptySchema = StructType(Seq.empty)
                  val merged = mergeInnerRequirementsWithPath(
                    emptySchema, sourceFieldPath, currentSchema, sourceStruct)
                  propagatedRequirements(si) = Some(merged)
                }
              } else {
                // Inner generate explodes outer's element directly (not a field of it).
                // This happens when GeneratorNestedColumnAliasing has already extracted
                // the array field. In this case, inner's element type IS outer's innermost
                // element type, so inner's requirements apply directly to outer.
                if (sourceStruct.fieldNames.toSet ==
                    currentInfo.elementStruct.fieldNames.toSet) {
                  // Inner's requirements become outer's requirements
                  propagatedRequirements(si) = Some(currentSchema)
                }
              }

            case _ => // No source generate found
          }

        case _ => // Current can't be pruned or has no requirements
      }
    }

    // Handle pos-only posexplode optimization and empty requirements
    propagatedRequirements.zipWithIndex.map { case (reqOpt, i) =>
      val info = chain(i)
      reqOpt match {
        case Some(schema) if schema.isEmpty && info.posAttrOpt.isDefined =>
          // Pos-only: select minimal-weight field
          if (info.elementStruct.fields.length > 1) {
            selectMinimalWeightField(info.elementStruct).map { f =>
              StructType(Array(f))
            }
          } else {
            // Already at minimal - no pruning possible (idempotency)
            None
          }
        case Some(schema) if schema.isEmpty =>
          // Empty requirements with non-posexplode means no explicit field accesses were found.
          // This typically happens when GeneratorNestedColumnAliasing has already extracted
          // the needed fields into an alias. The generate's source is already optimized,
          // so we should not try to "further prune" to an empty struct.
          // Return None to indicate no pruning should happen.
          None
        case other => other
      }
    }.toSeq
  }

  /**
   * Finds the index of the source generate whose output is used by the current generate.
   * Returns None if the current generate's source is from the scan (not from another generate).
   *
   * Note: Chain is ordered top-to-bottom, so source generates have LARGER indices.
   *
   * When GeneratorNestedColumnAliasing creates intermediate Projects with aliases,
   * we need to trace through those aliases to find the connection.
   */
  private def findSourceGenerateForCurrent(
      chain: Seq[GenerateInfo],
      currentIdx: Int,
      currentInfo: GenerateInfo): Option[Int] = {

    // The current generate's source is an expression like `outer.items` or an alias
    // We need to find which source generate's colAttr matches the root
    extractRootAttribute(currentInfo.generator.child) match {
      case Some(rootAttr) =>
        // First, try direct match
        val directMatch = chain.drop(currentIdx + 1).zipWithIndex.find { case (sourceInfo, _) =>
          sourceInfo.colAttr.exprId == rootAttr.exprId
        }.map { case (_, relIdx) => currentIdx + 1 + relIdx }

        directMatch.orElse {
          // If no direct match, check if the rootAttr is an alias for another generate's output.
          // This happens when GeneratorNestedColumnAliasing creates intermediate aliases.
          // Look at intermediate Projects between current and source generates.
          findSourceThroughAliases(chain, currentIdx, currentInfo, rootAttr)
        }
      case None =>
        None
    }
  }

  /**
   * Traces through intermediate alias Projects to find the source generate.
   * When GeneratorNestedColumnAliasing runs, it may create aliases like:
   *   _extract = outer_elem
   * or more complex expressions like:
   *   _extract_level3 = l1.level2.level3
   * We need to resolve these aliases to find the true source.
   */
  private def findSourceThroughAliases(
      chain: Seq[GenerateInfo],
      currentIdx: Int,
      currentInfo: GenerateInfo,
      aliasAttr: Attribute): Option[Int] = {


    // Extracts the root attribute from expressions like:
    // - Attribute: returns it directly
    // - GetStructField(child, ...) / GetArrayStructFields(child, ...) chains:
    //   recursively extracts root from child
    @scala.annotation.tailrec
    def extractRootAttrFromExpr(expr: Expression): Option[Attribute] = {
      expr match {
        case a: Attribute => Some(a)
        case GetStructField(child, _, _) => extractRootAttrFromExpr(child)
        case GetArrayStructFields(child, _, _, _, _) => extractRootAttrFromExpr(child)
        case _ => None
      }
    }

    // Look at the child plan of the current generate to find intermediate Projects
    // that might define the alias
    def findAliasDefinition(plan: LogicalPlan): Option[ExprId] = {
      plan match {
        case Project(projectList, child) =>
          // Find the alias that defines our aliasAttr
          val result = projectList.collectFirst {
            case a @ Alias(childExpr, _) if a.exprId == aliasAttr.exprId =>
              // Extract the root attribute from the child expression
              val root = extractRootAttrFromExpr(childExpr)
              root.map(_.exprId)
          }.flatten
          result.orElse(findAliasDefinition(child))
        case Filter(_, child) =>
          findAliasDefinition(child)
        case _ =>
          None
      }
    }

    // Find what the alias refers to
    val resolvedResult = findAliasDefinition(currentInfo.generate.child)
    resolvedResult.flatMap { resolvedExprId =>
      // Now look for a source generate whose colAttr matches the resolved exprId
      chain.drop(currentIdx + 1).zipWithIndex.find { case (sourceInfo, _) =>
        sourceInfo.colAttr.exprId == resolvedExprId
      }.map { case (_, relIdx) => currentIdx + 1 + relIdx }
    }
  }

  /**
   * Extracts the full field path that the current generate accesses from the source
   * generate's element. For example, if current explodes `source.level2.level3`,
   * returns Seq("level2", "level3").
   *
   * Handles aliased expressions by tracing through intermediate Projects.
   *
   * @param sourceInfo The generate whose output provides the array
   * @param currentInfo The generate that explodes a field from sourceInfo's output
   * @return The field path from source element to the exploded array
   */
  private def extractSourceFieldPath(
      sourceInfo: GenerateInfo,
      currentInfo: GenerateInfo): Seq[String] = {

    /**
     * Extracts the full path of field accesses from an expression rooted at colAttr.
     * For GetArrayStructFields(GetStructField(colAttr, level2), level3), returns
     * Seq("level2", "level3").
     */
    def extractPathFromExpr(expr: Expression, colAttr: Attribute): Seq[String] = {
      expr match {
        case gsf: GetStructField if isAttrRef(gsf.child, colAttr) =>
          Seq(gsf.extractFieldName)
        case gsf: GetStructField =>
          extractPathFromExpr(gsf.child, colAttr) :+ gsf.extractFieldName
        case gasf: GetArrayStructFields if isAttrRef(gasf.child, colAttr) =>
          Seq(gasf.field.name)
        case gasf: GetArrayStructFields =>
          extractPathFromExpr(gasf.child, colAttr) :+ gasf.field.name
        case _ =>
          Seq.empty
      }
    }

    // First try direct match
    val directPath = extractPathFromExpr(currentInfo.generator.child, sourceInfo.colAttr)
    if (directPath.nonEmpty) {
      return directPath
    }

    // If generator child is an attribute (alias), resolve it through intermediate Projects
    currentInfo.generator.child match {
      case attr: Attribute =>
        currentInfo.intermediateNodes.foreach {
          case Project(projectList, _) =>
            projectList.foreach {
              case a @ Alias(childExpr, _) if a.exprId == attr.exprId =>
                val path = extractPathFromExpr(childExpr, sourceInfo.colAttr)
                if (path.nonEmpty) {
                  return path
                }
              case _ =>
            }
          case _ =>
        }
        Seq.empty
      case _ =>
        Seq.empty
    }
  }

  /**
   * Extracts the field name (backward-compatible wrapper for extractSourceFieldPath).
   */
  private def extractSourceFieldName(
      sourceInfo: GenerateInfo,
      currentInfo: GenerateInfo): Option[String] = {
    val path = extractSourceFieldPath(sourceInfo, currentInfo)
    path.headOption
  }

  /**
   * Checks if two expressions are semantically equivalent for the purpose of
   * matching source array expressions (supports Attribute and GetStructField chains).
   */
  private def exprMatches(e1: Expression, e2: Expression): Boolean = {
    (e1, e2) match {
      case (a1: Attribute, a2: Attribute) => a1.exprId == a2.exprId
      case (gsf1: GetStructField, gsf2: GetStructField) =>
        gsf1.extractFieldName == gsf2.extractFieldName && exprMatches(gsf1.child, gsf2.child)
      case _ => false
    }
  }

  /**
   * Returns field names accessed on the source array via [[GetArrayStructFields]].
   * These are fields like `someArray.field` where `someArray` is an array of structs.
   *
   * Note: Direct references to the source array (e.g., in `isnotnull(array)` or
   * `size(array)`) are allowed - they don't access specific struct fields.
   * We only collect fields that are explicitly extracted via [[GetArrayStructFields]].
   *
   * @param exprs Expressions to analyze
   * @param sourceRootAttr The root attribute of the source array expression (for eligibility)
   * @param sourceArrayExpr The full source array expression (e.g., `col.structArr`)
   */
  private def analyzeSourceArrayFields(
      exprs: Seq[Expression],
      sourceRootAttr: Option[Attribute],
      sourceArrayExpr: Expression): Option[Set[String]] = {
    sourceRootAttr match {
      case None => Some(Set.empty) // Complex expression - allow pruning but no source fields
      case Some(_) =>
        val fieldNames = mutable.LinkedHashSet[String]()

        def analyze(e: Expression): Unit = {
          e match {
            case gasf: GetArrayStructFields if exprMatches(gasf.child, sourceArrayExpr) =>
              // Source array field access like someArray.field or col.structArr.field
              fieldNames += gasf.field.name
            case _ =>
              // Direct attribute references like isnotnull(someArray) are OK -
              // they don't access specific struct fields.
              // Continue recursing to find any nested GetArrayStructFields.
              e.children.foreach(analyze)
          }
        }
        exprs.foreach(analyze)
        Some(fieldNames.toSet)
    }
  }

  // ---------------------------------------------------------------------------
  //  Minimal-weight field for pos-only posexplode
  // ---------------------------------------------------------------------------

  /**
   * Selects the field with the smallest `defaultSize`. Tie-breaker:
   * lexicographic field name.
   */
  private def selectMinimalWeightField(
      st: StructType): Option[StructField] = {
    if (st.isEmpty) None
    else Some(st.fields.minBy(f => (f.dataType.defaultSize, f.name)))
  }

  /**
   * Checks if a Project list contains only simple attribute references
   * (possibly with aliases). These are typically inserted by ColumnPruning
   * and can be safely looked through.
   *
   * Returns false for Projects with GetStructField or other expressions,
   * as these create aliases that the generator may reference.
   */
  private def isAttributeOnlyProject(projectList: Seq[NamedExpression]): Boolean = {
    projectList.forall {
      case _: Attribute => true
      case Alias(_: Attribute, _) => true
      case _ => false
    }
  }

  /**
   * Walks through a plan extracting Filter conditions and looking through
   * attribute-only Projects to find the leaf (non-Project, non-Filter) child.
   * This allows our inner Project to be inserted directly above the leaf,
   * enabling ScanOperation to match it without interference from filters that
   * reference the full source array attribute.
   *
   * Only looks through Projects that contain simple attribute references
   * (from ColumnPruning). Projects with GetStructField or other expressions
   * create aliases that the generator may reference, so we don't look through
   * them to avoid breaking the reference chain.
   *
   * Returns (all filter conditions in bottom-first order, the leaf child).
   */
  private def decomposeChild(
      plan: LogicalPlan): (Seq[Expression], LogicalPlan) = {
    plan match {
      case Filter(condition, child) =>
        val (deeper, base) = decomposeChild(child)
        (deeper :+ condition, base)
      case Project(projectList, child) if isAttributeOnlyProject(projectList) =>
        // Look through column-pruning Projects inserted by ColumnPruning.
        // Our inner Project will handle column selection instead.
        decomposeChild(child)
      case other =>
        (Nil, other)
    }
  }

  /**
   * Finds the TRUE scan relation by looking through ALL Projects, Filters, and Generates.
   * This is needed to correctly determine scan attribute IDs - we can't use the first
   * non-attribute-only Project because that might be an intermediate alias Project
   * (e.g., from GeneratorNestedColumnAliasing) whose output includes alias attributes
   * that would be incorrectly treated as "scan attributes".
   *
   * @param plan The plan to traverse
   * @return The actual scan relation (or the deepest non-Project/Filter/Generate node)
   */
  private def findScanLeaf(plan: LogicalPlan): LogicalPlan = plan match {
    case Project(_, child) => findScanLeaf(child)
    case Filter(_, child) => findScanLeaf(child)
    case Generate(_, _, _, _, _, child) => findScanLeaf(child)
    case other => other
  }

  // ---------------------------------------------------------------------------
  //  Pruned-array builder (ArraysZip + GetArrayStructFields)
  // ---------------------------------------------------------------------------

  /**
   * Extracts each required field from the source array with
   * [[GetArrayStructFields]] (or [[GetNestedArrayStructFields]] for deeper
   * nesting) and recombines them into a single array-of-struct using [[ArraysZip]].
   *
   * Field ordering follows the original struct order to keep ordinals
   * predictable.
   *
   * For source arrays with deeper nesting (e.g., `array<array<struct>>`),
   * uses [[GetNestedArrayStructFields]] which handles arbitrary array depth.
   */
  private def buildPrunedArray(
      arrayExpr: Expression,
      elementStruct: StructType,
      requiredFieldNames: Seq[String],
      containsNull: Boolean): Expression = {

    val arrayDepth = computeArrayDepth(arrayExpr.dataType)

    val fieldArrays = requiredFieldNames.map { fieldName =>
      val ordinal = elementStruct.fieldIndex(fieldName)
      val field = elementStruct(ordinal)

      if (arrayDepth > 1) {
        // Nested array (e.g., array<array<struct>>) - use GetNestedArrayStructFields
        GetNestedArrayStructFields(
          arrayExpr, field, ordinal, elementStruct.length,
          containsNull || field.nullable)
      } else {
        // Simple array<struct> - use GetArrayStructFields
        GetArrayStructFields(
          arrayExpr, field, ordinal, elementStruct.length,
          containsNull || field.nullable)
      }
    }

    val names = requiredFieldNames.map(n => Literal(n))
    ArraysZip(fieldArrays, names)
  }

  // ---------------------------------------------------------------------------
  //  Schema-driven pruned-array builder (Step 13)
  // ---------------------------------------------------------------------------

  /**
   * Builds a pruned array expression from a nested StructType schema.
   *
   * This is the schema-driven replacement for buildPrunedArray. It handles:
   * - Primitive fields: extracted with GetArrayStructFields
   * - Nested array fields: recursively builds pruned nested arrays
   * - Nested struct fields: preserved with nested pruning applied
   *
   * For example, given:
   *   - Source: array<struct<requestId, ts, items: array<struct<itemId, clicked, visible>>>>
   *   - Required: struct<requestId, items: array<struct<itemId, clicked>>>
   *
   * Produces:
   *   arrays_zip(
   *     source.requestId,
   *     arrays_zip(source.items.itemId, source.items.clicked) AS items
   *   )
   *
   * @param arrayExpr The source array expression
   * @param originalStruct The original struct type of array elements
   * @param requiredSchema The required nested schema (subset of originalStruct)
   * @param containsNull Whether the array can contain nulls
   * @return Expression that produces the pruned array
   */
  private def buildPrunedArrayFromSchema(
      arrayExpr: Expression,
      originalStruct: StructType,
      requiredSchema: StructType,
      containsNull: Boolean): Expression = {

    val arrayDepth = computeArrayDepth(arrayExpr.dataType)

    // Order fields according to original struct order
    val orderedRequiredFields = originalStruct.fieldNames.toSeq
      .filter(name => requiredSchema.fieldNames.contains(name))
      .map(name => requiredSchema(requiredSchema.fieldIndex(name)))

    val fieldArrays = orderedRequiredFields.map { requiredField =>
      val fieldName = requiredField.name
      val origOrdinal = originalStruct.fieldIndex(fieldName)
      val origField = originalStruct(origOrdinal)

      (origField.dataType, requiredField.dataType) match {
        // Nested array of struct - recursively prune if schemas differ
        case (ArrayType(origInnerStruct: StructType, innerContainsNull),
              ArrayType(requiredInnerStruct: StructType, _))
            if hasNestedTypePruning(requiredInnerStruct, origInnerStruct) =>
          // Extract the field arrays first
          val fieldExpr = if (arrayDepth > 1) {
            GetNestedArrayStructFields(
              arrayExpr, origField, origOrdinal, originalStruct.length,
              containsNull || origField.nullable)
          } else {
            GetArrayStructFields(
              arrayExpr, origField, origOrdinal, originalStruct.length,
              containsNull || origField.nullable)
          }
          // Recursively prune the nested array
          buildPrunedArrayFromSchema(
            fieldExpr, origInnerStruct, requiredInnerStruct, innerContainsNull)

        // Non-nested field or no pruning needed for nested struct
        case _ =>
          if (arrayDepth > 1) {
            GetNestedArrayStructFields(
              arrayExpr, origField, origOrdinal, originalStruct.length,
              containsNull || origField.nullable)
          } else {
            GetArrayStructFields(
              arrayExpr, origField, origOrdinal, originalStruct.length,
              containsNull || origField.nullable)
          }
      }
    }

    val names = orderedRequiredFields.map(f => Literal(f.name))

    // For nested arrays (depth > 1), use NestedArraysZip to zip at the innermost level
    // For simple arrays (depth == 1), use the standard ArraysZip
    if (arrayDepth > 1) {
      NestedArraysZip.withDepth(fieldArrays, names, arrayDepth)
    } else {
      ArraysZip(fieldArrays, names)
    }
  }

  /**
   * Computes the depth of array nesting in a data type.
   * `array<struct>` has depth 1, `array<array<struct>>` has depth 2, etc.
   */
  private def computeArrayDepth(dt: DataType): Int = dt match {
    case ArrayType(elementType: ArrayType, _) => 1 + computeArrayDepth(elementType)
    case ArrayType(_, _) => 1
    case _ => 0
  }

  // ---------------------------------------------------------------------------
  //  Ordinal fixer (name-based)
  // ---------------------------------------------------------------------------

  /**
   * Rewrites [[GetStructField]] expressions that reference `colAttr` to use
   * positions in `newStruct` (resolved by field name) and updates the child
   * attribute's data type to match the pruned struct.
   *
   * This is critical: we must update BOTH the ordinal AND the child's data type.
   * Otherwise, `GetStructField.dataType` would use the old struct type's field
   * at the new ordinal position, returning the wrong field type.
   */
  private def fixOrdinalsInExpr(
      expr: Expression,
      colAttr: Attribute,
      newStruct: StructType): Expression = {
    // Create attribute with the pruned struct type
    val newColAttr = colAttr.withDataType(newStruct)

    expr.transformDown {
      case gsf @ GetStructField(child, _, _)
          if isAttrRef(child, colAttr) =>
        val fieldName = gsf.extractFieldName
        val newOrdinal = newStruct.fieldIndex(fieldName)
        // Update both child (with new type) and ordinal
        gsf.copy(child = newColAttr, ordinal = newOrdinal)
    }
  }

  /**
   * Rewrites [[GetArrayStructFields]] expressions that reference `srcAttr` to use
   * positions in `newStruct` (resolved by field name) and updates the child
   * attribute to use the pruned array type with `newStruct` as element type.
   *
   * This is needed for leaf filters that access source array fields like
   * `someArray.col3 IS NOT NULL`. After pruning to `{col1, col3}`, the ordinal
   * changes from 2 to 1, and the source array type changes.
   *
   * @param expr The expression to fix
   * @param srcAttr The original source array attribute
   * @param prunedAttr The _pruned attribute to replace srcAttr with
   * @param newStruct The pruned struct type (element type of the pruned array)
   * @param containsNull Whether the array can contain nulls
   */
  private def fixArrayStructOrdinalsInExpr(
      expr: Expression,
      srcAttr: Attribute,
      prunedAttr: Attribute,
      newStruct: StructType,
      containsNull: Boolean): Expression = {

    expr.transformDown {
      case gasf @ GetArrayStructFields(child, field, _, _, _)
          if isAttrRef(child, srcAttr) =>
        // Find the new ordinal in the pruned struct
        val fieldName = field.name
        val newOrdinal = newStruct.fieldIndex(fieldName)
        val newField = newStruct(newOrdinal)
        // Rebuild with updated child (pruned attr), field, ordinal, and numFields
        GetArrayStructFields(
          prunedAttr, newField, newOrdinal, newStruct.length,
          containsNull || newField.nullable)

      case a: Attribute if a.exprId == srcAttr.exprId =>
        // Direct reference to source array - replace with pruned
        prunedAttr
    }
  }

  /**
   * Rewrites [[GetArrayStructFields]] expressions that match the source array
   * expression (supports both direct Attributes and GetStructField chains like
   * `col.structArr`).
   *
   * This handles patterns like:
   * - `arr.field` where generator is `explode(arr)`
   * - `col.structArr.field` where generator is `explode(col.structArr)`
   *
   * @param expr The expression to fix
   * @param srcExpr The original source array expression (Attribute or GetStructField chain)
   * @param prunedAttr The _pruned attribute to replace srcExpr with
   * @param newStruct The pruned struct type (element type of the pruned array)
   * @param containsNull Whether the array can contain nulls
   */
  private def fixArrayStructOrdinalsInExprChain(
      expr: Expression,
      srcExpr: Expression,
      prunedAttr: Attribute,
      newStruct: StructType,
      containsNull: Boolean): Expression = {

    expr.transformDown {
      case gasf @ GetArrayStructFields(child, field, _, _, _)
          if exprMatches(child, srcExpr) =>
        // Find the new ordinal in the pruned struct
        val fieldName = field.name
        val newOrdinal = newStruct.fieldIndex(fieldName)
        val newField = newStruct(newOrdinal)
        // Rebuild with updated child (pruned attr), field, ordinal, and numFields
        GetArrayStructFields(
          prunedAttr, newField, newOrdinal, newStruct.length,
          containsNull || newField.nullable)

      case e if exprMatches(e, srcExpr) =>
        // Direct reference to source array expression - replace with pruned
        prunedAttr
    }
  }
}
