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

package org.apache.spark.sql.execution.datasources

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Generate, LeafNode, LogicalPlan,
  Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.GENERATE
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, AtomicType, DataType, MapType, StructField,
  StructType}
import org.apache.spark.sql.util.SchemaUtils._
import org.apache.spark.util.Utils

/**
 * Prunes unnecessary physical columns given a [[ScanOperation]] over a data source relation.
 * By "physical column", we mean a column as defined in the data source format like Parquet format
 * or ORC format. For example, in Spark SQL, a root-level Parquet column corresponds to a SQL
 * column, and a nested Parquet column corresponds to a [[StructField]].
 *
 * Also prunes the unnecessary metadata columns if any for all file formats.
 */
object SchemaPruning extends Rule[LogicalPlan] {
  import org.apache.spark.sql.catalyst.expressions.SchemaPruning._

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformDown {
      case op @ GeneratorsOverScan(_, _,
          l @ LogicalRelationWithTable(hadoopFsRelation: HadoopFsRelation, _)) =>
        // Fields read from the generators' elements above them narrow the scan below them.
        pruneThroughGenerators(op, l) { (scan, otherProjects, inputRootFields) =>
          val allFilters = scan.filtersPushDown.reduceOption(And).toSeq ++ scan.filtersStayUp
          prunePhysicalColumns(l, scan.projects, allFilters, hadoopFsRelation,
            (prunedDataSchema, prunedMetadataSchema) => {
              val prunedHadoopRelation = hadoopFsRelation.copy(
                dataSchema = prunedDataSchema)(hadoopFsRelation.sparkSession)
              buildPrunedRelation(l, prunedHadoopRelation, prunedMetadataSchema)
            }, Some(otherProjects), inputRootFields)
        }.getOrElse(op)

      case op @ ScanOperation(projects, filtersStayUp, filtersPushDown,
        l @ LogicalRelationWithTable(hadoopFsRelation: HadoopFsRelation, _)) =>
        val allFilters = filtersPushDown.reduceOption(And).toSeq ++ filtersStayUp
        prunePhysicalColumns(l, projects, allFilters, hadoopFsRelation,
          (prunedDataSchema, prunedMetadataSchema) => {
            val prunedHadoopRelation =
              hadoopFsRelation.copy(dataSchema = prunedDataSchema)(hadoopFsRelation.sparkSession)
            buildPrunedRelation(l, prunedHadoopRelation, prunedMetadataSchema)
          }).getOrElse(op)
    }

  /** The projects and filters [[ScanOperation]] collects between two operators. */
  private[sql] case class Segment(
      projects: Seq[NamedExpression],
      filtersStayUp: Seq[Expression],
      filtersPushDown: Seq[Expression]) {
    def filters: Seq[Expression] = filtersPushDown ++ filtersStayUp
  }

  /**
   * Matches `explode` / `posexplode` generators stacked over a leaf, each one optionally under
   * `Project`s and `Filter`s. Returns the segments from the top (the one above the top generator
   * first, the one over the leaf last), the generators from the top, and the leaf.
   */
  private[sql] object GeneratorsOverScan {
    def unapply(plan: LogicalPlan): Option[(Seq[Segment], Seq[Generate], LogicalPlan)] = {
      if (!plan.containsPattern(GENERATE) || !conf.nestedSchemaPruningEnabled ||
          !conf.nestedSchemaPruningThroughGenerateEnabled) {
        return None
      }
      @tailrec
      def collect(p: LogicalPlan, segments: Seq[Segment], generators: Seq[Generate])
          : Option[(Seq[Segment], Seq[Generate], LogicalPlan)] = p match {
        case ScanOperation(projects, filtersStayUp, filtersPushDown,
            g @ Generate(_: ExplodeBase, _, _, _, _, _)) =>
          collect(g.child, segments :+ Segment(projects, filtersStayUp, filtersPushDown),
            generators :+ g)
        case ScanOperation(projects, filtersStayUp, filtersPushDown, leaf: LeafNode)
            if generators.nonEmpty =>
          Some((segments :+ Segment(projects, filtersStayUp, filtersPushDown), generators, leaf))
        case _ => None
      }
      collect(plan, Nil, Nil)
    }
  }

  /**
   * Prunes the scan below a chain of `explode` / `posexplode` generators of arrays to the fields
   * that the plan above each generator, up to `op`, reads from its element, then re-resolves the
   * chain against the narrower types. Going down from the top generator, the fields read from a
   * generator's element become fields read from its input: a column of the scan, or the element
   * of a generator below it.
   *
   * `pruneScan` prunes the scan given the projects over the scan that do not only feed the
   * bottom generator and the root fields the generators need, and returns the new plan over the
   * scan. Returns None if nothing is pruned, or if the output of `op` would change.
   */
  private[sql] def pruneThroughGenerators(
      op: LogicalPlan,
      scanRelation: LogicalPlan)(
      pruneScan: (Segment, Seq[NamedExpression], Seq[RootField]) => Option[LogicalPlan])
      : Option[LogicalPlan] = {
    // the flags are checked by GeneratorsOverScan, which matched `op`
    try {
      // e.g. the `size(arr) > 0` filters inferred for a non-outer explode
      op.transformAllExpressions {
        case s @ Size(child, _) => s.copy(child = lengthOnly(child))
      } match {
        case sized @ GeneratorsOverScan(segments, generators, _) =>
          pruneThroughGeneratorsImpl(sized, segments, generators, scanRelation)(pruneScan)
        case _ => None
      }
    } catch {
      // In tests, surface the bug (e.g. SelectedField and ProjectionOverSchema drifting apart)
      // instead of passing on the unpruned plan.
      case NonFatal(e) if Utils.isTesting => throw e
      // Otherwise a missed optimization, not a failed query: keep the plan as it is. The rule
      // runs several times per query, so this is not logged as a warning.
      case NonFatal(e) =>
        logDebug("Failed to prune nested fields through generators; " +
          "the scan is pruned without them", e)
        None
    }
  }

  /**
   * The size of an array only needs its length. Returns an array of the same length that reads
   * as little as possible: the smallest field of an array of structs, like
   * [[org.apache.spark.sql.catalyst.optimizer.RewriteSizeOfArrayStruct]] does for scan columns,
   * and of the outer array for a nested field of an array of structs, which has the same length.
   */
  private def lengthOnly(array: Expression): Expression = {
    def smallestField(array: Expression): Option[Expression] = array.dataType match {
      case ArrayType(StructType(fields), containsNull) if fields.length > 1 =>
        val (field, ordinal) = fields.zipWithIndex.minBy(_._1.dataType.defaultSize)
        Some(GetArrayStructFields(
          array, field, ordinal, fields.length, containsNull || field.nullable))
      case _ => None
    }
    array match {
      // a field that is itself nested, e.g. `items.parts`: read a field of `items` instead
      case g @ GetArrayStructFields(child, field, _, _, _)
          if !field.dataType.isInstanceOf[AtomicType] =>
        smallestField(child).orElse(smallestField(g)).getOrElse(g)
      case _ => smallestField(array).getOrElse(array)
    }
  }

  private def pruneThroughGeneratorsImpl(
      op: LogicalPlan,
      segments: Seq[Segment],
      generators: Seq[Generate],
      scanRelation: LogicalPlan)(
      pruneScan: (Segment, Seq[NamedExpression], Seq[RootField]) => Option[LogicalPlan])
      : Option[LogicalPlan] = {
    val attrNameMap = scanRelation.output.map(att => (att.exprId, att.name)).toMap
    // Each generator's input in terms of the segment below it, and the projects of that segment
    // which only feed the generator: their reads are the generator's input root fields instead.
    val inputs = generators.zip(segments.tail).map { case (g, below) =>
      val aliases = below.projects.collect { case a: Alias => a.exprId -> a.child }.toMap
      val input = g.generator.children.head.transformUp {
        case a: Attribute if aliases.contains(a.exprId) => aliases(a.exprId)
      }
      val feeding = g.generator.references -- g.requiredChildOutput
      (normalizeAttributeRefNames(attrNameMap, Seq(input)).head,
        below.projects.filterNot(p => feeding.contains(p.toAttribute)))
    }
    val elements = generators.map(_.generatorOutput.last)
    val lowerElements = elements.map(_.exprId).toSet
    // Reads above each generator. Columns that a lower segment only passes up are read where
    // they are used, so only the top segment counts them as read whole.
    val readsAbove = segments.indices.dropRight(1).scanLeft(Seq.empty[Expression]) { (reads, i) =>
      val projects = if (i == 0) segments(i).projects else inputs(i - 1)._2.filter {
        case _: Attribute => false
        case _ => true
      }
      reads ++ projects ++ segments(i).filters
    }.tail
    val elementFields = mutable.Map.empty[ExprId, Seq[StructField]].withDefaultValue(Nil)
    val scanRootFields = mutable.ArrayBuffer.empty[RootField]
    // Inputs that are not a nested field access path, e.g. `explode(array(item.sku))`, are read
    // by the generators below like any other expression above them.
    val inputReads = mutable.ArrayBuffer.empty[Expression]
    generators.indices.foreach { i =>
      val (input, _) = inputs(i)
      val rootField = getGeneratorInputRootField(
        input, elements(i), readsAbove(i) ++ inputReads, elementFields(elements(i).exprId))
      // the input also reads what a filter predicate or sort comparator inside it reads
      val inputFields = rootField.map(_ +: getGeneratorInputLambdaRootFields(input))
      (inputFields, input.references.toSeq) match {
        case (Some(fields), Seq(root)) if lowerElements.contains(root.exprId) =>
          elementFields(root.exprId) ++= fields.map(_.field)
        case (Some(fields), Seq(root)) if scanRelation.outputSet.contains(root) =>
          scanRootFields ++= fields
        case _ =>
          inputReads += input
          scanRootFields ++= scanRootFieldsOf(input, scanRelation)
      }
    }
    pruneScan(segments.last, inputs.last._2, scanRootFields.toSeq)
      .map(rebuildAbove(op, generators.last.child, _))
      .filter(newOp => newOp.resolved && newOp.schema == op.schema &&
        !conf.getConf(SQLConf.NESTED_SCHEMA_PRUNING_THROUGH_GENERATE_REJECT_FOR_TESTING))
      .map(readSizesDirectly)
  }

  /**
   * Once the scan is narrowed, `size` no longer needs to read through a field of an array of
   * structs: the narrowed array has the same length and nullness, and reading it directly avoids
   * building a projected array per row, e.g. `size(item.parts.pid)` becomes `size(item.parts)`.
   * The field accesses [[lengthOnly]] added only served to decide what the scan reads.
   */
  private def readSizesDirectly(plan: LogicalPlan): LogicalPlan = {
    @tailrec
    def array(expr: Expression): Expression = expr match {
      case GetArrayStructFields(child, _, _, _, _) => array(child)
      case other => other
    }
    plan.transformAllExpressions {
      case s @ Size(child: GetArrayStructFields, _) => s.copy(child = array(child))
    }
  }

  /** The root fields of scan columns that `expr` reads. */
  private def scanRootFieldsOf(
      expr: Expression,
      scanRelation: LogicalPlan): Seq[RootField] = {
    // Give every other attribute, e.g. a generator's element or the position of a posexplode,
    // a name no scan column has, so that it is not mistaken for a column of its name.
    val renamed = expr.transform {
      case a: Attribute if !scanRelation.outputSet.contains(a) =>
        a.withName(s"${a.name}#${a.exprId.id}")
    }
    val scanColumns = scanRelation.output.map(_.name).toSet
    identifyRootFields(Nil, Seq(renamed)).filter(f => scanColumns.contains(f.field.name))
  }

  /**
   * Replaces `bottom` in `plan` with `newBottom`, whose output types are narrower, and
   * re-resolves every operator from there up: references take their child's new type, nested
   * field accesses are re-resolved by name and generator outputs take the new element type.
   *
   * Every access collected through [[SelectedField]] must be rewritten here by
   * [[ProjectionOverSchema]]. A missed rewrite usually fails resolution or the output schema
   * check, but a stale ordinal that lands on a sibling field of the same type would not, which
   * is why references are matched by exprId rather than by name.
   */
  private def rebuildAbove(
      plan: LogicalPlan,
      bottom: LogicalPlan,
      newBottom: LogicalPlan): LogicalPlan = {
    if (plan eq bottom) {
      newBottom
    } else {
      val withNewChildren =
        plan.withNewChildren(plan.children.map(rebuildAbove(_, bottom, newBottom)))
      val childOutput = AttributeMap(withNewChildren.children.flatMap(_.output).map(a => a -> a))
      val narrowed = withNewChildren.references.toSeq.flatMap { ref =>
        childOutput.get(ref).filter(_.dataType != ref.dataType)
      }
      // one attribute at a time, since attributes from different generators can share a name
      val resolved = narrowed.foldLeft(withNewChildren) { (p, attr) =>
        val projectionOverSchema = ProjectionOverSchema(
          StructType(Array(StructField(attr.name, attr.dataType))), AttributeSet(attr))
        // ProjectionOverSchema matches references by name, but a reference keeps the name it was
        // written with (e.g. `ITEM` for `item`), and a missed one keeps its old ordinal and reads
        // the wrong field. The scan's own pruning normalizes names first
        // (`normalizeAttributeRefNames`); the operators above the generators are only rewritten
        // here, so normalize them here too.
        p.transformExpressions {
          case a: AttributeReference if a.exprId == attr.exprId => a.withName(attr.name)
        }.transformExpressions { case projectionOverSchema(expr) => expr }
      }
      resolved match {
        case g: Generate =>
          g.copy(generatorOutput = g.generatorOutput.zip(g.generator.elementSchema).map {
            case (attr, field) => attr.withDataType(field.dataType)
          })
        case other => other
      }
    }
  }

  /**
   * This method returns optional logical plan. `None` is returned if no nested field is required or
   * all nested fields are required.
   *
   * This method will prune both the data schema and the metadata schema.
   * When working out which fields are read, `rootFieldProjects` replaces `projects`, which is
   * still the project list of the returned plan, and `extraRootFields` are read too.
   */
  private def prunePhysicalColumns(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filters: Seq[Expression],
      hadoopFsRelation: HadoopFsRelation,
      leafNodeBuilder: (StructType, StructType) => LeafNode,
      rootFieldProjects: Option[Seq[NamedExpression]] = None,
      extraRootFields: Seq[RootField] = Nil): Option[LogicalPlan] = {
    val attrNameMap = relation.output.map(att => (att.exprId, att.name)).toMap
    val normalizedProjects = normalizeAttributeRefNames(attrNameMap, projects)
      .asInstanceOf[Seq[NamedExpression]]
    val normalizedFilters = normalizeAttributeRefNames(attrNameMap, filters)
    val normalizedRootFieldProjects = rootFieldProjects.map(
      normalizeAttributeRefNames(attrNameMap, _).asInstanceOf[Seq[NamedExpression]])
    val requestedRootFields = identifyRootFields(
      normalizedRootFieldProjects.getOrElse(normalizedProjects), normalizedFilters,
      extraRootFields)

    // If requestedRootFields includes a nested field, continue. Otherwise,
    // return op
    if (requestedRootFields.exists { root: RootField => !root.derivedFromAtt }) {

      val prunedDataSchema = if (canPruneDataSchema(hadoopFsRelation)) {
        pruneSchema(hadoopFsRelation.dataSchema, requestedRootFields)
      } else {
        hadoopFsRelation.dataSchema
      }

      val metadataSchema =
        relation.output.collect { case FileSourceMetadataAttribute(attr) => attr }.toStructType
      val prunedMetadataSchema = if (metadataSchema.nonEmpty) {
        pruneMetadataSchema(metadataSchema, requestedRootFields)
      } else {
        metadataSchema
      }

      // If the data schema is different from the pruned data schema
      // OR
      // the metadata schema is different from the pruned metadata schema, continue.
      // Otherwise, return None.
      if (countLeaves(hadoopFsRelation.dataSchema) > countLeaves(prunedDataSchema) ||
        countLeaves(metadataSchema) > countLeaves(prunedMetadataSchema)) {
        val prunedRelation = leafNodeBuilder(prunedDataSchema, prunedMetadataSchema)
        val projectionOverSchema = ProjectionOverSchema(
          prunedDataSchema.merge(prunedMetadataSchema), AttributeSet(relation.output))
        Some(buildNewProjection(projects, normalizedProjects, normalizedFilters,
          prunedRelation, projectionOverSchema))
      } else {
        None
      }
    } else {
      None
    }
  }

  /**
   * Checks to see if the given relation can be pruned. Currently we support Parquet and ORC v1.
   */
  private def canPruneDataSchema(fsRelation: HadoopFsRelation): Boolean =
    conf.nestedSchemaPruningEnabled && (
      fsRelation.fileFormat.isInstanceOf[ParquetFileFormat] ||
        fsRelation.fileFormat.isInstanceOf[OrcFileFormat])

  /**
   * Prunes a file-source metadata schema (one `StructType` containing each
   * `FileSourceMetadataAttribute`). Unlike pruning a data file schema, this only prunes
   * unused sibling sub-attributes (each is its own per-field extractor); kept sub-attributes'
   * data types are preserved verbatim because the extractor produces a complete catalyst
   * value, and shaving fields out would shift positions in that value.
   */
  private def pruneMetadataSchema(
      metadataSchema: StructType,
      requestedRootFields: Seq[RootField]): StructType = {
    val resolver = conf.resolver
    StructType(metadataSchema.map { topField =>
      topField.dataType match {
        case innerStruct: StructType =>
          // Collect the requested sub-attribute names for this metadata attribute from the
          // root field tree. Anything below those sub-attributes (e.g. nested struct/array
          // element fields) is ignored, since extractor outputs aren't pruned.
          val requestedSubNames: Set[String] = requestedRootFields.collect {
            case rf if resolver(rf.field.name, topField.name) =>
              rf.field.dataType match {
                case rs: StructType => rs.fieldNames.toSet
                case _ => Set.empty[String]
              }
          }.flatten.toSet
          val keptSubFields = innerStruct.fields.filter { sub =>
            requestedSubNames.exists(name => resolver(name, sub.name))
          }
          if (keptSubFields.length == innerStruct.fields.length) {
            // Nothing to prune for this attribute; keep the original.
            topField
          } else {
            topField.copy(dataType = StructType(keptSubFields))
          }
        case _ => topField
      }
    })
  }

  /**
   * Normalizes the names of the attribute references in the given expressions to reflect
   * the names in the given logical relation. This makes it possible to compare attributes and
   * fields by name. Returns a tuple with the normalized projects and filters, respectively.
   */
  private def normalizeAttributeRefNames(
      attrNameMap: Map[ExprId, String],
      exprs: Seq[Expression]): Seq[Expression] = {
    exprs.map(_.transform {
      case att: AttributeReference if attrNameMap.contains(att.exprId) =>
        att.withName(attrNameMap(att.exprId))
    })
  }

  /**
   * Builds the new output [[Project]] Spark SQL operator that has the `leafNode`.
   */
  private def buildNewProjection(
      projects: Seq[NamedExpression],
      normalizedProjects: Seq[NamedExpression],
      filters: Seq[Expression],
      leafNode: LeafNode,
      projectionOverSchema: ProjectionOverSchema): Project = {
    // Construct a new target for our projection by rewriting and
    // including the original filters where available
    val projectionChild =
      if (filters.nonEmpty) {
        val projectedFilters = filters.map(_.transformDown {
          case projectionOverSchema(expr) => expr
        })
        // bottom-most filters are put in the left of the list.
        projectedFilters.foldLeft[LogicalPlan](leafNode)((plan, cond) => Filter(cond, plan))
      } else {
        leafNode
      }

    // Construct the new projections of our Project by
    // rewriting the original projections
    val newProjects = normalizedProjects.map(_.transformDown {
      case projectionOverSchema(expr) => expr
    }).map { case expr: NamedExpression => expr }

    if (log.isDebugEnabled) {
      logDebug(s"New projects:\n${newProjects.map(_.treeString).mkString("\n")}")
    }

    Project(restoreOriginalOutputNames(newProjects, projects.map(_.name)), projectionChild)
  }

  /**
   * Builds a pruned logical relation from the output of the output relation and the schema of the
   * pruned base relation.
   */
  private def buildPrunedRelation(
      outputRelation: LogicalRelation,
      prunedBaseRelation: HadoopFsRelation,
      prunedMetadataSchema: StructType) = {
    val finalSchema = prunedBaseRelation.schema.merge(prunedMetadataSchema)
    val prunedOutput = getPrunedOutput(outputRelation.output, finalSchema)
    val prunedRelation = outputRelation.copy(relation = prunedBaseRelation, output = prunedOutput)
    prunedRelation.copyTagsFrom(outputRelation)
    prunedRelation
  }

  // Prune the given output to make it consistent with `requiredSchema`.
  private def getPrunedOutput(
      output: Seq[AttributeReference],
      requiredSchema: StructType): Seq[AttributeReference] = {
    // We need to update the data type of the output attributes to use the pruned ones.
    // so that references to the original relation's output are not broken
    val nameAttributeMap = output.map(att => (att.name, att)).toMap
    toAttributes(requiredSchema)
      .map {
        case att if nameAttributeMap.contains(att.name) =>
          nameAttributeMap(att.name).withDataType(att.dataType)
        case att => att
      }
  }

  /**
   * Counts the "leaf" fields of the given dataType. Informally, this is the
   * number of fields of non-complex data type in the tree representation of
   * [[DataType]].
   */
  private def countLeaves(dataType: DataType): Int = {
    dataType match {
      case array: ArrayType => countLeaves(array.elementType)
      case map: MapType => countLeaves(map.keyType) + countLeaves(map.valueType)
      case struct: StructType =>
        struct.map(field => countLeaves(field.dataType)).sum
      case _ => 1
    }
  }
}
