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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.{AliasIdentifier, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis.{AnsiTypeCoercion, MultiInstanceRelation, Resolver, TypeCoercion, TypeCoercionBase}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.catalyst.catalog.CatalogTable.VIEW_STORING_ANALYZED_PLAN
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning, RoundRobinPartitioning, SinglePartition}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.Utils
import org.apache.spark.util.random.RandomSampler

/**
 * When planning take() or collect() operations, this special node is inserted at the top of
 * the logical plan before invoking the query planner.
 *
 * Rules can pattern-match on this node in order to apply transformations that only take effect
 * at the top of the logical query plan.
 */
case class ReturnAnswer(child: LogicalPlan) extends UnaryNode {
  override def maxRows: Option[Long] = child.maxRows
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: LogicalPlan): ReturnAnswer =
    copy(child = newChild)
}

/**
 * This node is inserted at the top of a subquery when it is optimized. This makes sure we can
 * recognize a subquery as such, and it allows us to write subquery aware transformations.
 *
 * @param correlated flag that indicates the subquery is correlated, and will be rewritten into a
 *                   join during analysis.
 */
case class Subquery(child: LogicalPlan, correlated: Boolean) extends OrderPreservingUnaryNode {
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: LogicalPlan): Subquery =
    copy(child = newChild)
}

object Subquery {
  def fromExpression(s: SubqueryExpression): Subquery =
    Subquery(s.plan, SubqueryExpression.hasCorrelatedSubquery(s))
}

case class Project(projectList: Seq[NamedExpression], child: LogicalPlan)
    extends OrderPreservingUnaryNode {
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)
  override def maxRows: Option[Long] = child.maxRows
  override def maxRowsPerPartition: Option[Long] = child.maxRowsPerPartition

  final override val nodePatterns: Seq[TreePattern] = Seq(PROJECT)

  override lazy val resolved: Boolean = {
    val hasSpecialExpressions = projectList.exists ( _.collect {
        case agg: AggregateExpression => agg
        case generator: Generator => generator
        case window: WindowExpression => window
      }.nonEmpty
    )

    expressions.forall(_.resolved) && childrenResolved && !hasSpecialExpressions
  }

  override lazy val validConstraints: ExpressionSet =
    getAllValidConstraints(projectList)

  override def metadataOutput: Seq[Attribute] =
    getTagValue(Project.hiddenOutputTag).getOrElse(child.metadataOutput)

  override protected def withNewChildInternal(newChild: LogicalPlan): Project =
    copy(child = newChild)
}

object Project {
  val hiddenOutputTag: TreeNodeTag[Seq[Attribute]] = TreeNodeTag[Seq[Attribute]]("hidden_output")

  def matchSchema(plan: LogicalPlan, schema: StructType, conf: SQLConf): Project = {
    assert(plan.resolved)
    val projectList = reorderFields(plan.output.map(a => (a.name, a)), schema.fields, Nil, conf)
    Project(projectList, plan)
  }

  private def reconcileColumnType(
      col: Expression,
      columnPath: Seq[String],
      dt: DataType,
      nullable: Boolean,
      conf: SQLConf): Expression = {
    if (col.nullable && !nullable) {
      throw QueryCompilationErrors.nullableColumnOrFieldError(columnPath)
    }
    (col.dataType, dt) match {
      case (StructType(fields), expected: StructType) =>
        val newFields = reorderFields(
          fields.zipWithIndex.map { case (f, index) =>
            (f.name, GetStructField(col, index))
          },
          expected.fields,
          columnPath,
          conf)
        if (col.nullable) {
          If(IsNull(col), Literal(null, dt), CreateStruct(newFields))
        } else {
          CreateStruct(newFields)
        }

      case (ArrayType(et, containsNull), expected: ArrayType) =>
        if (containsNull & !expected.containsNull) {
          throw QueryCompilationErrors.nullableArrayOrMapElementError(columnPath)
        }
        val param = NamedLambdaVariable("x", et, containsNull)
        val reconciledElement = reconcileColumnType(
          param, columnPath :+ "element", expected.elementType, expected.containsNull, conf)
        val func = LambdaFunction(reconciledElement, Seq(param))
        ArrayTransform(col, func)

      case (MapType(kt, vt, valueContainsNull), expected: MapType) =>
        if (valueContainsNull & !expected.valueContainsNull) {
          throw QueryCompilationErrors.nullableArrayOrMapElementError(columnPath)
        }
        val keyParam = NamedLambdaVariable("key", kt, nullable = false)
        val valueParam = NamedLambdaVariable("value", vt, valueContainsNull)
        val reconciledKey = reconcileColumnType(
          keyParam, columnPath :+ "key", expected.keyType, false, conf)
        val reconciledValue = reconcileColumnType(
          valueParam, columnPath :+ "value", expected.valueType, expected.valueContainsNull, conf)
        val keyFunc = LambdaFunction(reconciledKey, Seq(keyParam))
        val valueFunc = LambdaFunction(reconciledValue, Seq(valueParam))
        val newKeys = ArrayTransform(MapKeys(col), keyFunc)
        val newValues = ArrayTransform(MapValues(col), valueFunc)
        MapFromArrays(newKeys, newValues)

      case (other, target) =>
        if (other == target) {
          col
        } else if (Cast.canANSIStoreAssign(other, target)) {
          Cast(col, target, Option(conf.sessionLocalTimeZone), ansiEnabled = true)
        } else {
          throw QueryCompilationErrors.invalidColumnOrFieldDataTypeError(columnPath, other, target)
        }
    }
  }

  private def reorderFields(
      fields: Seq[(String, Expression)],
      expected: Seq[StructField],
      columnPath: Seq[String],
      conf: SQLConf): Seq[NamedExpression] = {
    expected.map { f =>
      val matched = fields.filter(field => conf.resolver(field._1, f.name))
      if (matched.isEmpty) {
        if (f.nullable) {
          val columnExpr = Literal.create(null, f.dataType)
          // Fill nullable missing new column with null value.
          createNewColumn(columnExpr, f.name, f.metadata, Metadata.empty)
        } else {
          if (columnPath.isEmpty) {
            val candidates = fields.map(_._1)
            val orderedCandidates =
              StringUtils.orderStringsBySimilarity(f.name, candidates)
            throw QueryCompilationErrors.unresolvedColumnError(f.name, orderedCandidates)
          } else {
            throw QueryCompilationErrors.unresolvedFieldError(f.name, columnPath, fields.map(_._1))
          }
        }
      } else if (matched.length > 1) {
        throw QueryCompilationErrors.ambiguousColumnOrFieldError(
          columnPath :+ f.name, matched.length)
      } else {
        val columnExpr = matched.head._2
        val originalMetadata = columnExpr match {
          case ne: NamedExpression => ne.metadata
          case g: GetStructField => g.childSchema(g.ordinal).metadata
          case _ => Metadata.empty
        }

        val newColumnPath = columnPath :+ matched.head._1
        val newColumnExpr = reconcileColumnType(
          columnExpr, newColumnPath, f.dataType, f.nullable, conf)
        createNewColumn(newColumnExpr, f.name, f.metadata, originalMetadata)
      }
    }
  }

  private def createNewColumn(
      col: Expression,
      name: String,
      newMetadata: Metadata,
      originalMetadata: Metadata): NamedExpression = {
    val metadata = new MetadataBuilder()
      .withMetadata(originalMetadata)
      .withMetadata(newMetadata)
      .build()

    col match {
      case a: Attribute => a.withName(name).withMetadata(metadata)
      case other =>
        if (metadata == Metadata.empty) {
          Alias(other, name)()
        } else {
          Alias(other, name)(explicitMetadata = Some(metadata))
        }
    }
  }
}

/**
 * Applies a [[Generator]] to a stream of input rows, combining the
 * output of each into a new stream of rows.  This operation is similar to a `flatMap` in functional
 * programming with one important additional feature, which allows the input rows to be joined with
 * their output.
 *
 * @param generator the generator expression
 * @param unrequiredChildIndex this parameter starts as Nil and gets filled by the Optimizer.
 *                             It's used as an optimization for omitting data generation that will
 *                             be discarded next by a projection.
 *                             A common use case is when we explode(array(..)) and are interested
 *                             only in the exploded data and not in the original array. before this
 *                             optimization the array got duplicated for each of its elements,
 *                             causing O(n^^2) memory consumption. (see [SPARK-21657])
 * @param outer when true, each input row will be output at least once, even if the output of the
 *              given `generator` is empty.
 * @param qualifier Qualifier for the attributes of generator(UDTF)
 * @param generatorOutput The output schema of the Generator.
 * @param child Children logical plan node
 */
case class Generate(
    generator: Generator,
    unrequiredChildIndex: Seq[Int],
    outer: Boolean,
    qualifier: Option[String],
    generatorOutput: Seq[Attribute],
    child: LogicalPlan)
  extends UnaryNode {

  final override val nodePatterns: Seq[TreePattern] = Seq(GENERATE)

  lazy val requiredChildOutput: Seq[Attribute] = {
    val unrequiredSet = unrequiredChildIndex.toSet
    child.output.zipWithIndex.filterNot(t => unrequiredSet.contains(t._2)).map(_._1)
  }

  override lazy val resolved: Boolean = {
    generator.resolved &&
      childrenResolved &&
      generator.elementSchema.length == generatorOutput.length &&
      generatorOutput.forall(_.resolved)
  }

  override def producedAttributes: AttributeSet = AttributeSet(generatorOutput)

  def qualifiedGeneratorOutput: Seq[Attribute] = {
    val qualifiedOutput = qualifier.map { q =>
      // prepend the new qualifier to the existed one
      generatorOutput.map(a => a.withQualifier(Seq(q)))
    }.getOrElse(generatorOutput)
    val nullableOutput = qualifiedOutput.map {
      // if outer, make all attributes nullable, otherwise keep existing nullability
      a => a.withNullability(outer || a.nullable)
    }
    nullableOutput
  }

  def output: Seq[Attribute] = requiredChildOutput ++ qualifiedGeneratorOutput

  override protected def withNewChildInternal(newChild: LogicalPlan): Generate =
    copy(child = newChild)
}

case class Filter(condition: Expression, child: LogicalPlan)
  extends OrderPreservingUnaryNode with PredicateHelper {
  override def output: Seq[Attribute] = child.output

  override def maxRows: Option[Long] = child.maxRows
  override def maxRowsPerPartition: Option[Long] = child.maxRowsPerPartition

  final override val nodePatterns: Seq[TreePattern] = Seq(FILTER)

  override protected lazy val validConstraints: ExpressionSet = {
    val predicates = splitConjunctivePredicates(condition)
      .filterNot(SubqueryExpression.hasCorrelatedSubquery)
    child.constraints.union(ExpressionSet(predicates))
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): Filter =
    copy(child = newChild)
}

abstract class SetOperation(left: LogicalPlan, right: LogicalPlan) extends BinaryNode {

  def duplicateResolved: Boolean = left.outputSet.intersect(right.outputSet).isEmpty

  protected def leftConstraints: ExpressionSet = left.constraints

  protected def rightConstraints: ExpressionSet = {
    require(left.output.size == right.output.size)
    val attributeRewrites = AttributeMap(right.output.zip(left.output))
    right.constraints.map(_ transform {
      case a: Attribute => attributeRewrites(a)
    })
  }

  override lazy val resolved: Boolean =
    childrenResolved &&
      left.output.length == right.output.length &&
      left.output.zip(right.output).forall { case (l, r) =>
        l.dataType.sameType(r.dataType)
      } && duplicateResolved
}

object SetOperation {
  def unapply(p: SetOperation): Option[(LogicalPlan, LogicalPlan)] = Some((p.left, p.right))
}

case class Intersect(
    left: LogicalPlan,
    right: LogicalPlan,
    isAll: Boolean) extends SetOperation(left, right) {

  override def nodeName: String = getClass.getSimpleName + ( if ( isAll ) "All" else "" )

  final override val nodePatterns: Seq[TreePattern] = Seq(INTERSECT)

  override def output: Seq[Attribute] =
    left.output.zip(right.output).map { case (leftAttr, rightAttr) =>
      leftAttr.withNullability(leftAttr.nullable && rightAttr.nullable)
    }

  override def metadataOutput: Seq[Attribute] = Nil

  override protected lazy val validConstraints: ExpressionSet =
    leftConstraints.union(rightConstraints)

  override def maxRows: Option[Long] = {
    if (children.exists(_.maxRows.isEmpty)) {
      None
    } else {
      Some(children.flatMap(_.maxRows).min)
    }
  }

  override protected def withNewChildrenInternal(
    newLeft: LogicalPlan, newRight: LogicalPlan): Intersect = copy(left = newLeft, right = newRight)
}

case class Except(
    left: LogicalPlan,
    right: LogicalPlan,
    isAll: Boolean) extends SetOperation(left, right) {
  override def nodeName: String = getClass.getSimpleName + ( if ( isAll ) "All" else "" )
  /** We don't use right.output because those rows get excluded from the set. */
  override def output: Seq[Attribute] = left.output

  override def metadataOutput: Seq[Attribute] = Nil

  final override val nodePatterns : Seq[TreePattern] = Seq(EXCEPT)

  override protected lazy val validConstraints: ExpressionSet = leftConstraints

  override def maxRows: Option[Long] = left.maxRows

  override protected def withNewChildrenInternal(
    newLeft: LogicalPlan, newRight: LogicalPlan): Except = copy(left = newLeft, right = newRight)
}

/** Factory for constructing new `Union` nodes. */
object Union {
  def apply(left: LogicalPlan, right: LogicalPlan): Union = {
    Union (left :: right :: Nil)
  }
}

/**
 * Logical plan for unioning multiple plans, without a distinct. This is UNION ALL in SQL.
 *
 * @param byName          Whether resolves columns in the children by column names.
 * @param allowMissingCol Allows missing columns in children query plans. If it is true,
 *                        this function allows different set of column names between two Datasets.
 *                        This can be set to true only if `byName` is true.
 */
case class Union(
    children: Seq[LogicalPlan],
    byName: Boolean = false,
    allowMissingCol: Boolean = false) extends LogicalPlan {
  assert(!allowMissingCol || byName, "`allowMissingCol` can be true only if `byName` is true.")

  override def maxRows: Option[Long] = {
    var sum = BigInt(0)
    children.foreach { child =>
      if (child.maxRows.isDefined) {
        sum += child.maxRows.get
        if (!sum.isValidLong) {
          return None
        }
      } else {
        return None
      }
    }
    Some(sum.toLong)
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(UNION)

  /**
   * Note the definition has assumption about how union is implemented physically.
   */
  override def maxRowsPerPartition: Option[Long] = {
    var sum = BigInt(0)
    children.foreach { child =>
      if (child.maxRowsPerPartition.isDefined) {
        sum += child.maxRowsPerPartition.get
        if (!sum.isValidLong) {
          return None
        }
      } else {
        return None
      }
    }
    Some(sum.toLong)
  }

  def duplicateResolved: Boolean = {
    children.map(_.outputSet.size).sum ==
      AttributeSet.fromAttributeSets(children.map(_.outputSet)).size
  }

  // updating nullability to make all the children consistent
  override def output: Seq[Attribute] = {
    children.map(_.output).transpose.map { attrs =>
      val firstAttr = attrs.head
      val nullable = attrs.exists(_.nullable)
      val newDt = attrs.map(_.dataType).reduce(StructType.unionLikeMerge)
      if (firstAttr.dataType == newDt) {
        firstAttr.withNullability(nullable)
      } else {
        AttributeReference(firstAttr.name, newDt, nullable, firstAttr.metadata)(
          firstAttr.exprId, firstAttr.qualifier)
      }
    }
  }

  override def metadataOutput: Seq[Attribute] = Nil

  override lazy val resolved: Boolean = {
    // allChildrenCompatible needs to be evaluated after childrenResolved
    def allChildrenCompatible: Boolean =
      children.tail.forall( child =>
        // compare the attribute number with the first child
        child.output.length == children.head.output.length &&
        // compare the data types with the first child
        child.output.zip(children.head.output).forall {
          case (l, r) => DataType.equalsStructurally(l.dataType, r.dataType, true)
        })
    children.length > 1 && !(byName || allowMissingCol) && childrenResolved && allChildrenCompatible
  }

  /**
   * Maps the constraints containing a given (original) sequence of attributes to those with a
   * given (reference) sequence of attributes. Given the nature of union, we expect that the
   * mapping between the original and reference sequences are symmetric.
   */
  private def rewriteConstraints(
      reference: Seq[Attribute],
      original: Seq[Attribute],
      constraints: ExpressionSet): ExpressionSet = {
    require(reference.size == original.size)
    val attributeRewrites = AttributeMap(original.zip(reference))
    constraints.map(_ transform {
      case a: Attribute => attributeRewrites(a)
    })
  }

  private def merge(a: ExpressionSet, b: ExpressionSet): ExpressionSet = {
    val common = a.intersect(b)
    // The constraint with only one reference could be easily inferred as predicate
    // Grouping the constraints by it's references so we can combine the constraints with same
    // reference together
    val othera = a.diff(common).filter(_.references.size == 1).groupBy(_.references.head)
    val otherb = b.diff(common).filter(_.references.size == 1).groupBy(_.references.head)
    // loose the constraints by: A1 && B1 || A2 && B2  ->  (A1 || A2) && (B1 || B2)
    val others = (othera.keySet intersect otherb.keySet).map { attr =>
      Or(othera(attr).reduceLeft(And), otherb(attr).reduceLeft(And))
    }
    common ++ others
  }

  override protected lazy val validConstraints: ExpressionSet = {
    children
      .map(child => rewriteConstraints(children.head.output, child.output, child.constraints))
      .reduce(merge(_, _))
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): Union =
    copy(children = newChildren)
}

case class Join(
    left: LogicalPlan,
    right: LogicalPlan,
    joinType: JoinType,
    condition: Option[Expression],
    hint: JoinHint)
  extends BinaryNode with PredicateHelper {

  override def maxRows: Option[Long] = {
    joinType match {
      case Inner | Cross | FullOuter | LeftOuter | RightOuter
          if left.maxRows.isDefined && right.maxRows.isDefined =>
        val leftMaxRows = BigInt(left.maxRows.get)
        val rightMaxRows = BigInt(right.maxRows.get)
        val minRows = joinType match {
          case LeftOuter => leftMaxRows
          case RightOuter => rightMaxRows
          case FullOuter => leftMaxRows + rightMaxRows
          case _ => BigInt(0)
        }
        val maxRows = (leftMaxRows * rightMaxRows).max(minRows)
        if (maxRows.isValidLong) {
          Some(maxRows.toLong)
        } else {
          None
        }

      case LeftSemi | LeftAnti =>
        left.maxRows

      case _ =>
        None
    }
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case _ =>
        left.output ++ right.output
    }
  }

  override def metadataOutput: Seq[Attribute] = {
    joinType match {
      case ExistenceJoin(_) =>
        left.metadataOutput
      case LeftExistence(_) =>
        left.metadataOutput
      case _ =>
        children.flatMap(_.metadataOutput)
    }
  }

  override protected lazy val validConstraints: ExpressionSet = {
    joinType match {
      case _: InnerLike if condition.isDefined =>
        left.constraints
          .union(right.constraints)
          .union(ExpressionSet(splitConjunctivePredicates(condition.get)))
      case LeftSemi if condition.isDefined =>
        left.constraints
          .union(ExpressionSet(splitConjunctivePredicates(condition.get)))
      case j: ExistenceJoin =>
        left.constraints
      case _: InnerLike =>
        left.constraints.union(right.constraints)
      case LeftExistence(_) =>
        left.constraints
      case LeftOuter =>
        left.constraints
      case RightOuter =>
        right.constraints
      case _ =>
        ExpressionSet()
    }
  }

  def duplicateResolved: Boolean = left.outputSet.intersect(right.outputSet).isEmpty

  // Joins are only resolved if they don't introduce ambiguous expression ids.
  // NaturalJoin should be ready for resolution only if everything else is resolved here
  lazy val resolvedExceptNatural: Boolean = {
    childrenResolved &&
      expressions.forall(_.resolved) &&
      duplicateResolved &&
      condition.forall(_.dataType == BooleanType)
  }

  // if not a natural join, use `resolvedExceptNatural`. if it is a natural join or
  // using join, we still need to eliminate natural or using before we mark it resolved.
  override lazy val resolved: Boolean = joinType match {
    case NaturalJoin(_) => false
    case UsingJoin(_, _) => false
    case _ => resolvedExceptNatural
  }

  override val nodePatterns : Seq[TreePattern] = {
    var patterns = Seq(JOIN)
    joinType match {
      case _: InnerLike => patterns = patterns :+ INNER_LIKE_JOIN
      case LeftOuter | FullOuter | RightOuter => patterns = patterns :+ OUTER_JOIN
      case LeftSemiOrAnti(_) => patterns = patterns :+ LEFT_SEMI_OR_ANTI_JOIN
      case NaturalJoin(_) | UsingJoin(_, _) => patterns = patterns :+ NATURAL_LIKE_JOIN
      case _ =>
    }
    patterns
  }

  // Ignore hint for canonicalization
  protected override def doCanonicalize(): LogicalPlan =
    super.doCanonicalize().asInstanceOf[Join].copy(hint = JoinHint.NONE)

  // Do not include an empty join hint in string description
  protected override def stringArgs: Iterator[Any] = super.stringArgs.filter { e =>
    (!e.isInstanceOf[JoinHint]
      || e.asInstanceOf[JoinHint].leftHint.isDefined
      || e.asInstanceOf[JoinHint].rightHint.isDefined)
  }

  override protected def withNewChildrenInternal(
    newLeft: LogicalPlan, newRight: LogicalPlan): Join = copy(left = newLeft, right = newRight)
}

/**
 * Insert query result into a directory.
 *
 * @param isLocal Indicates whether the specified directory is local directory
 * @param storage Info about output file, row and what serialization format
 * @param provider Specifies what data source to use; only used for data source file.
 * @param child The query to be executed
 * @param overwrite If true, the existing directory will be overwritten
 *
 * Note that this plan is unresolved and has to be replaced by the concrete implementations
 * during analysis.
 */
case class InsertIntoDir(
    isLocal: Boolean,
    storage: CatalogStorageFormat,
    provider: Option[String],
    child: LogicalPlan,
    overwrite: Boolean = true)
  extends UnaryNode {

  override def output: Seq[Attribute] = Seq.empty
  override def metadataOutput: Seq[Attribute] = Nil
  override lazy val resolved: Boolean = false

  override protected def withNewChildInternal(newChild: LogicalPlan): InsertIntoDir =
    copy(child = newChild)
}

/**
 * A container for holding the view description(CatalogTable) and info whether the view is temporary
 * or not. If it's a SQL (temp) view, the child should be a logical plan parsed from the
 * `CatalogTable.viewText`. Otherwise, the view is a temporary one created from a dataframe and the
 * view description should contain a `VIEW_CREATED_FROM_DATAFRAME` property; in this case, the child
 * must be already resolved.
 *
 * This operator will be removed at the end of analysis stage.
 *
 * @param desc A view description(CatalogTable) that provides necessary information to resolve the
 *             view.
 * @param isTempView A flag to indicate whether the view is temporary or not.
 * @param child The logical plan of a view operator. If the view description is available, it should
 *              be a logical plan parsed from the `CatalogTable.viewText`.
 */
case class View(
    desc: CatalogTable,
    isTempView: Boolean,
    child: LogicalPlan) extends UnaryNode {
  require(!isTempViewStoringAnalyzedPlan || child.resolved)

  override def output: Seq[Attribute] = child.output

  override def metadataOutput: Seq[Attribute] = Nil

  override def simpleString(maxFields: Int): String = {
    s"View (${desc.identifier}, ${output.mkString("[", ",", "]")})"
  }

  override def doCanonicalize(): LogicalPlan = child match {
    case p: Project if p.resolved && canRemoveProject(p) => p.child.canonicalized
    case _ => child.canonicalized
  }

  def isTempViewStoringAnalyzedPlan: Boolean =
    isTempView && desc.properties.contains(VIEW_STORING_ANALYZED_PLAN)

  // When resolving a SQL view, we use an extra Project to add cast and alias to make sure the view
  // output schema doesn't change even if the table referenced by the view is changed after view
  // creation. We should remove this extra Project during canonicalize if it does nothing.
  // See more details in `SessionCatalog.fromCatalogTable`.
  private def canRemoveProject(p: Project): Boolean = {
    p.output.length == p.child.output.length && p.projectList.zip(p.child.output).forall {
      case (Alias(cast: Cast, name), childAttr) =>
        cast.child match {
          case a: AttributeReference =>
            a.dataType == cast.dataType && a.name == name && childAttr.semanticEquals(a)
          case _ => false
        }
      case _ => false
    }
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): View =
    copy(child = newChild)
}

object View {
  def effectiveSQLConf(configs: Map[String, String], isTempView: Boolean): SQLConf = {
    val activeConf = SQLConf.get
    // For temporary view, we always use captured sql configs
    if (activeConf.useCurrentSQLConfigsForView && !isTempView) return activeConf

    val sqlConf = new SQLConf()
    // We retain below configs from current session because they are not captured by view
    // as optimization configs but they are still needed during the view resolution.
    // TODO: remove this `retainedConfigs` after the `RelationConversions` is moved to
    // optimization phase.
    val retainedConfigs = activeConf.getAllConfs.filterKeys(key =>
      Seq(
        "spark.sql.hive.convertMetastoreParquet",
        "spark.sql.hive.convertMetastoreOrc",
        "spark.sql.hive.convertInsertingPartitionedTable",
        "spark.sql.hive.convertMetastoreCtas"
      ).contains(key) || key.startsWith("spark.sql.catalog."))
    for ((k, v) <- configs ++ retainedConfigs) {
      sqlConf.settings.put(k, v)
    }
    sqlConf
  }
}

/**
 * A container for holding named common table expressions (CTEs) and a query plan.
 * This operator will be removed during analysis and the relations will be substituted into child.
 *
 * @param child The final query of this CTE.
 * @param cteRelations A sequence of pair (alias, the CTE definition) that this CTE defined
 *                     Each CTE can see the base tables and the previously defined CTEs only.
 */
case class UnresolvedWith(
    child: LogicalPlan,
    cteRelations: Seq[(String, SubqueryAlias)]) extends UnaryNode {
  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_WITH)

  override def output: Seq[Attribute] = child.output

  override def simpleString(maxFields: Int): String = {
    val cteAliases = truncatedString(cteRelations.map(_._1), "[", ", ", "]", maxFields)
    s"CTE $cteAliases"
  }

  override def innerChildren: Seq[LogicalPlan] = cteRelations.map(_._2)

  override protected def withNewChildInternal(newChild: LogicalPlan): UnresolvedWith =
    copy(child = newChild)
}

/**
 * A wrapper for CTE definition plan with a unique ID.
 * @param child The CTE definition query plan.
 * @param id    The unique ID for this CTE definition.
 * @param originalPlanWithPredicates The original query plan before predicate pushdown and the
 *                                   predicates that have been pushed down into `child`. This is
 *                                   a temporary field used by optimization rules for CTE predicate
 *                                   pushdown to help ensure rule idempotency.
 * @param underSubquery If true, it means we don't need to add a shuffle for this CTE relation as
 *                      subquery reuse will be applied to reuse CTE relation output.
 */
case class CTERelationDef(
    child: LogicalPlan,
    id: Long = CTERelationDef.newId,
    originalPlanWithPredicates: Option[(LogicalPlan, Seq[Expression])] = None,
    underSubquery: Boolean = false) extends UnaryNode {

  final override val nodePatterns: Seq[TreePattern] = Seq(CTE)

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(child = newChild)

  override def output: Seq[Attribute] = if (resolved) child.output else Nil
}

object CTERelationDef {
  private[sql] val curId = new java.util.concurrent.atomic.AtomicLong()
  def newId: Long = curId.getAndIncrement()
}

/**
 * Represents the relation of a CTE reference.
 * @param cteId                The ID of the corresponding CTE definition.
 * @param _resolved            Whether this reference is resolved.
 * @param output               The output attributes of this CTE reference, which can be different
 *                             from the output of its corresponding CTE definition after attribute
 *                             de-duplication.
 * @param statsOpt             The optional statistics inferred from the corresponding CTE
 *                             definition.
 */
case class CTERelationRef(
    cteId: Long,
    _resolved: Boolean,
    override val output: Seq[Attribute],
    statsOpt: Option[Statistics] = None) extends LeafNode with MultiInstanceRelation {

  final override val nodePatterns: Seq[TreePattern] = Seq(CTE)

  override lazy val resolved: Boolean = _resolved

  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))

  def withNewStats(statsOpt: Option[Statistics]): CTERelationRef = copy(statsOpt = statsOpt)

  override def computeStats(): Statistics = statsOpt.getOrElse(Statistics(conf.defaultSizeInBytes))
}

/**
 * The resolved version of [[UnresolvedWith]] with CTE referrences linked to CTE definitions
 * through unique IDs instead of relation aliases.
 *
 * @param plan    The query plan.
 * @param cteDefs The CTE definitions.
 */
case class WithCTE(plan: LogicalPlan, cteDefs: Seq[CTERelationDef]) extends LogicalPlan {

  final override val nodePatterns: Seq[TreePattern] = Seq(CTE)

  override def output: Seq[Attribute] = plan.output

  override def children: Seq[LogicalPlan] = cteDefs :+ plan

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = {
    copy(plan = newChildren.last, cteDefs = newChildren.init.asInstanceOf[Seq[CTERelationDef]])
  }

  def withNewPlan(newPlan: LogicalPlan): WithCTE = {
    withNewChildren(children.init :+ newPlan).asInstanceOf[WithCTE]
  }
}

case class WithWindowDefinition(
    windowDefinitions: Map[String, WindowSpecDefinition],
    child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  final override val nodePatterns: Seq[TreePattern] = Seq(WITH_WINDOW_DEFINITION)
  override protected def withNewChildInternal(newChild: LogicalPlan): WithWindowDefinition =
    copy(child = newChild)
}

/**
 * @param order  The ordering expressions
 * @param global True means global sorting apply for entire data set,
 *               False means sorting only apply within the partition.
 * @param child  Child logical plan
 */
case class Sort(
    order: Seq[SortOrder],
    global: Boolean,
    child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override def maxRows: Option[Long] = child.maxRows
  override def maxRowsPerPartition: Option[Long] = {
    if (global) maxRows else child.maxRowsPerPartition
  }
  override def outputOrdering: Seq[SortOrder] = order
  final override val nodePatterns: Seq[TreePattern] = Seq(SORT)
  override protected def withNewChildInternal(newChild: LogicalPlan): Sort = copy(child = newChild)
}

/** Factory for constructing new `Range` nodes. */
object Range {
  def apply(start: Long, end: Long, step: Long, numSlices: Int): Range = {
    Range(start, end, step, Some(numSlices))
  }

  def getOutputAttrs: Seq[Attribute] = {
    StructType(StructField("id", LongType, nullable = false) :: Nil).toAttributes
  }

  private def typeCoercion: TypeCoercionBase = {
    if (SQLConf.get.ansiEnabled) AnsiTypeCoercion else TypeCoercion
  }

  private def castAndEval[T](expression: Expression, dataType: DataType): T = {
    typeCoercion.implicitCast(expression, dataType)
      .map(_.eval())
      .filter(_ != null)
      .getOrElse {
        throw QueryCompilationErrors.incompatibleRangeInputDataTypeError(expression, dataType)
      }.asInstanceOf[T]
  }

  def toLong(expression: Expression): Long = castAndEval[Long](expression, LongType)

  def toInt(expression: Expression): Int = castAndEval[Int](expression, IntegerType)
}

@ExpressionDescription(
  usage = """
    _FUNC_(start: long, end: long, step: long, numSlices: integer)
    _FUNC_(start: long, end: long, step: long)
    _FUNC_(start: long, end: long)
    _FUNC_(end: long)""",
  examples = """
    Examples:
      > SELECT * FROM _FUNC_(1);
        +---+
        | id|
        +---+
        |  0|
        +---+
      > SELECT * FROM _FUNC_(0, 2);
        +---+
        |id |
        +---+
        |0  |
        |1  |
        +---+
      > SELECT * FROM _FUNC_(0, 4, 2);
        +---+
        |id |
        +---+
        |0  |
        |2  |
        +---+
  """,
  since = "2.0.0",
  group = "table_funcs")
case class Range(
    start: Long,
    end: Long,
    step: Long,
    numSlices: Option[Int],
    override val output: Seq[Attribute] = Range.getOutputAttrs,
    override val isStreaming: Boolean = false)
  extends LeafNode with MultiInstanceRelation {

  require(step != 0, s"step ($step) cannot be 0")

  def this(start: Expression, end: Expression, step: Expression, numSlices: Expression) =
    this(Range.toLong(start), Range.toLong(end), Range.toLong(step), Some(Range.toInt(numSlices)))

  def this(start: Expression, end: Expression, step: Expression) =
    this(Range.toLong(start), Range.toLong(end), Range.toLong(step), None)

  def this(start: Expression, end: Expression) = this(start, end, Literal.create(1L, LongType))

  def this(end: Expression) = this(Literal.create(0L, LongType), end)

  val numElements: BigInt = {
    val safeStart = BigInt(start)
    val safeEnd = BigInt(end)
    if ((safeEnd - safeStart) % step == 0 || (safeEnd > safeStart) != (step > 0)) {
      (safeEnd - safeStart) / step
    } else {
      // the remainder has the same sign with range, could add 1 more
      (safeEnd - safeStart) / step + 1
    }
  }

  def toSQL(): String = {
    if (numSlices.isDefined) {
      s"SELECT id AS `${output.head.name}` FROM range($start, $end, $step, ${numSlices.get})"
    } else {
      s"SELECT id AS `${output.head.name}` FROM range($start, $end, $step)"
    }
  }

  override def newInstance(): Range = copy(output = output.map(_.newInstance()))

  override def simpleString(maxFields: Int): String = {
    s"Range ($start, $end, step=$step, splits=$numSlices)"
  }

  override def maxRows: Option[Long] = {
    if (numElements.isValidLong) {
      Some(numElements.toLong)
    } else {
      None
    }
  }

  override def maxRowsPerPartition: Option[Long] = {
    if (numSlices.isDefined) {
      var m = numElements / numSlices.get
      if (numElements % numSlices.get != 0) m += 1
      if (m.isValidLong) Some(m.toLong) else maxRows
    } else {
      maxRows
    }
  }

  override def computeStats(): Statistics = {
    if (numElements == 0) {
      Statistics(sizeInBytes = 0, rowCount = Some(0))
    } else {
      val (minVal, maxVal) = if (step > 0) {
        (start, start + (numElements - 1) * step)
      } else {
        (start + (numElements - 1) * step, start)
      }

      val histogram = if (conf.histogramEnabled) {
        Some(computeHistogramStatistics())
      } else {
        None
      }

      val colStat = ColumnStat(
        distinctCount = Some(numElements),
        max = Some(maxVal),
        min = Some(minVal),
        nullCount = Some(0),
        avgLen = Some(LongType.defaultSize),
        maxLen = Some(LongType.defaultSize),
        histogram = histogram)

      Statistics(
        sizeInBytes = LongType.defaultSize * numElements,
        rowCount = Some(numElements),
        attributeStats = AttributeMap(Seq(output.head -> colStat)))
    }
  }

  private def computeHistogramStatistics(): Histogram = {
    val numBins = conf.histogramNumBins
    val height = numElements.toDouble / numBins
    val percentileArray = (0 to numBins).map(i => i * height).toArray

    val lowerIndexInitial: Double = percentileArray.head
    val lowerBinValueInitial: Long = getRangeValue(0)
    val (_, _, binArray) = percentileArray.tail
      .foldLeft((lowerIndexInitial, lowerBinValueInitial, Seq.empty[HistogramBin])) {
        case ((lowerIndex, lowerBinValue, binAr), upperIndex) =>
          // Integer index for upper and lower values in the bin.
          val upperIndexPos = math.ceil(upperIndex).toInt - 1
          val lowerIndexPos = math.ceil(lowerIndex).toInt - 1

          val upperBinValue = getRangeValue(math.max(upperIndexPos, 0))
          val ndv = math.max(upperIndexPos - lowerIndexPos, 1)
          // Update the lowerIndex and lowerBinValue with upper ones for the next iteration.
          (upperIndex, upperBinValue, binAr :+ HistogramBin(lowerBinValue, upperBinValue, ndv))
      }
    Histogram(height, binArray.toArray)
  }

  // Utility method to compute histogram
  private def getRangeValue(index: Int): Long = {
    assert(index >= 0, "index must be greater than and equal to 0")
    if (step < 0) {
      // Reverse the range values for computing histogram, if the step size is negative.
      start + (numElements.toLong - index - 1) * step
    } else {
      start + index * step
    }
  }

  override def outputOrdering: Seq[SortOrder] = {
    val order = if (step > 0) {
      Ascending
    } else {
      Descending
    }
    output.map(a => SortOrder(a, order))
  }
}

/**
 * This is a Group by operator with the aggregate functions and projections.
 *
 * @param groupingExpressions expressions for grouping keys
 * @param aggregateExpressions expressions for a project list, which could contain
 *                             [[AggregateExpression]]s.
 *
 * Note: Currently, aggregateExpressions is the project list of this Group by operator. Before
 * separating projection from grouping and aggregate, we should avoid expression-level optimization
 * on aggregateExpressions, which could reference an expression in groupingExpressions.
 * For example, see the rule [[org.apache.spark.sql.catalyst.optimizer.SimplifyExtractValueOps]]
 */
case class Aggregate(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: LogicalPlan)
  extends UnaryNode {

  override lazy val resolved: Boolean = {
    val hasWindowExpressions = aggregateExpressions.exists ( _.collect {
        case window: WindowExpression => window
      }.nonEmpty
    )

    expressions.forall(_.resolved) && childrenResolved && !hasWindowExpressions
  }

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)
  override def metadataOutput: Seq[Attribute] = Nil
  override def maxRows: Option[Long] = {
    if (groupingExpressions.isEmpty) {
      Some(1L)
    } else {
      child.maxRows
    }
  }

  final override val nodePatterns : Seq[TreePattern] = Seq(AGGREGATE)

  override lazy val validConstraints: ExpressionSet = {
    val nonAgg = aggregateExpressions.filter(!_.exists(_.isInstanceOf[AggregateExpression]))
    getAllValidConstraints(nonAgg)
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): Aggregate =
    copy(child = newChild)

  // Whether this Aggregate operator is group only. For example: SELECT a, a FROM t GROUP BY a
  private[sql] def groupOnly: Boolean = {
    // aggregateExpressions can be empty through Dateset.agg,
    // so we should also check groupingExpressions is non empty
    groupingExpressions.nonEmpty && aggregateExpressions.map {
      case Alias(child, _) => child
      case e => e
    }.forall(a => a.foldable || groupingExpressions.exists(g => a.semanticEquals(g)))
  }
}

object Aggregate {
  def isAggregateBufferMutable(schema: StructType): Boolean = {
    schema.forall(f => UnsafeRow.isMutable(f.dataType))
  }

  def supportsHashAggregate(aggregateBufferAttributes: Seq[Attribute]): Boolean = {
    val aggregationBufferSchema = StructType.fromAttributes(aggregateBufferAttributes)
    isAggregateBufferMutable(aggregationBufferSchema)
  }

  def supportsObjectHashAggregate(aggregateExpressions: Seq[AggregateExpression]): Boolean = {
    aggregateExpressions.map(_.aggregateFunction).exists {
      case _: TypedImperativeAggregate[_] => true
      case _ => false
    }
  }
}

case class Window(
    windowExpressions: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: LogicalPlan) extends UnaryNode {
  override def maxRows: Option[Long] = child.maxRows
  override def output: Seq[Attribute] =
    child.output ++ windowExpressions.map(_.toAttribute)

  override def producedAttributes: AttributeSet = windowOutputSet

  final override val nodePatterns: Seq[TreePattern] = Seq(WINDOW)

  def windowOutputSet: AttributeSet = AttributeSet(windowExpressions.map(_.toAttribute))

  override protected def withNewChildInternal(newChild: LogicalPlan): Window =
    copy(child = newChild)
}

object Expand {
  /**
   * Build bit mask from attributes of selected grouping set. A bit in the bitmask is corresponding
   * to an attribute in group by attributes sequence, the selected attribute has corresponding bit
   * set to 0 and otherwise set to 1. For example, if we have GroupBy attributes (a, b, c, d), the
   * bitmask 5(whose binary form is 0101) represents grouping set (a, c).
   *
   * @param groupingSetAttrs The attributes of selected grouping set
   * @param attrMap Mapping group by attributes to its index in attributes sequence
   * @return The bitmask which represents the selected attributes out of group by attributes.
   */
  private def buildBitmask(
    groupingSetAttrs: Seq[Attribute],
    attrMap: Map[Attribute, Int]): Long = {
    val numAttributes = attrMap.size
    assert(numAttributes <= GroupingID.dataType.defaultSize * 8)
    val mask = if (numAttributes != 64) (1L << numAttributes) - 1 else 0xFFFFFFFFFFFFFFFFL
    // Calculate the attribute masks of selected grouping set. For example, if we have GroupBy
    // attributes (a, b, c, d), grouping set (a, c) will produce the following sequence:
    // (15, 7, 13), whose binary form is (1111, 0111, 1101)
    val masks = (mask +: groupingSetAttrs.map(attrMap).map(index =>
      // 0 means that the column at the given index is a grouping column, 1 means it is not,
      // so we unset the bit in bitmap.
      ~(1L << (numAttributes - 1 - index))
    ))
    // Reduce masks to generate an bitmask for the selected grouping set.
    masks.reduce(_ & _)
  }

  /**
   * Apply the all of the GroupExpressions to every input row, hence we will get
   * multiple output rows for an input row.
   *
   * @param groupingSetsAttrs The attributes of grouping sets
   * @param groupByAliases The aliased original group by expressions
   * @param groupByAttrs The attributes of aliased group by expressions
   * @param gid Attribute of the grouping id
   * @param child Child operator
   */
  def apply(
    groupingSetsAttrs: Seq[Seq[Attribute]],
    groupByAliases: Seq[Alias],
    groupByAttrs: Seq[Attribute],
    gid: Attribute,
    child: LogicalPlan): Expand = {
    val attrMap = Utils.toMapWithIndex(groupByAttrs)

    val hasDuplicateGroupingSets = groupingSetsAttrs.size !=
      groupingSetsAttrs.map(_.map(_.exprId).toSet).distinct.size

    // Create an array of Projections for the child projection, and replace the projections'
    // expressions which equal GroupBy expressions with Literal(null), if those expressions
    // are not set for this grouping set.
    val projections = groupingSetsAttrs.zipWithIndex.map { case (groupingSetAttrs, i) =>
      val projAttrs = child.output ++ groupByAttrs.map { attr =>
        if (!groupingSetAttrs.contains(attr)) {
          // if the input attribute in the Invalid Grouping Expression set of for this group
          // replace it with constant null
          Literal.create(null, attr.dataType)
        } else {
          attr
        }
      // groupingId is the last output, here we use the bit mask as the concrete value for it.
      } :+ {
        val bitMask = buildBitmask(groupingSetAttrs, attrMap)
        val dataType = GroupingID.dataType
        Literal.create(if (dataType.sameType(IntegerType)) bitMask.toInt else bitMask, dataType)
      }

      if (hasDuplicateGroupingSets) {
        // If `groupingSetsAttrs` has duplicate entries (e.g., GROUPING SETS ((key), (key))),
        // we add one more virtual grouping attribute (`_gen_grouping_pos`) to avoid
        // wrongly grouping rows with the same grouping ID.
        projAttrs :+ Literal.create(i, IntegerType)
      } else {
        projAttrs
      }
    }

    // the `groupByAttrs` has different meaning in `Expand.output`, it could be the original
    // grouping expression or null, so here we create new instance of it.
    val output = if (hasDuplicateGroupingSets) {
      val gpos = AttributeReference("_gen_grouping_pos", IntegerType, false)()
      child.output ++ groupByAttrs.map(_.newInstance) :+ gid :+ gpos
    } else {
      child.output ++ groupByAttrs.map(_.newInstance) :+ gid
    }
    Expand(projections, output, Project(child.output ++ groupByAliases, child))
  }
}

/**
 * Apply a number of projections to every input row, hence we will get multiple output rows for
 * an input row.
 *
 * @param projections to apply
 * @param output of all projections.
 * @param child operator.
 */
case class Expand(
    projections: Seq[Seq[Expression]],
    output: Seq[Attribute],
    child: LogicalPlan) extends UnaryNode {
  @transient
  override lazy val references: AttributeSet =
    AttributeSet(projections.flatten.flatMap(_.references))

  override def maxRows: Option[Long] = child.maxRows match {
    case Some(m) =>
      val n = BigInt(m) * projections.length
      if (n.isValidLong) Some(n.toLong) else None
    case _ => None
  }
  override def maxRowsPerPartition: Option[Long] = child.maxRowsPerPartition match {
    case Some(m) =>
      val n = BigInt(m) * projections.length
      if (n.isValidLong) Some(n.toLong) else None
    case _ => maxRows
  }

  override def metadataOutput: Seq[Attribute] = Nil

  override def producedAttributes: AttributeSet = AttributeSet(output diff child.output)

  // This operator can reuse attributes (for example making them null when doing a roll up) so
  // the constraints of the child may no longer be valid.
  override protected lazy val validConstraints: ExpressionSet = ExpressionSet()

  override protected def withNewChildInternal(newChild: LogicalPlan): Expand =
    copy(child = newChild)
}

/**
 * A logical offset, which may removing a specified number of rows from the beginning of the
 * output of child logical plan.
 */
case class Offset(offsetExpr: Expression, child: LogicalPlan) extends OrderPreservingUnaryNode {
  override def output: Seq[Attribute] = child.output
  override def maxRows: Option[Long] = {
    import scala.math.max
    offsetExpr match {
      case IntegerLiteral(offset) => child.maxRows.map { x => max(x - offset, 0) }
      case _ => None
    }
  }
  override protected def withNewChildInternal(newChild: LogicalPlan): Offset =
    copy(child = newChild)
}

/**
 * A constructor for creating a pivot, which will later be converted to a [[Project]]
 * or an [[Aggregate]] during the query analysis.
 *
 * @param groupByExprsOpt A sequence of group by expressions. This field should be None if coming
 *                        from SQL, in which group by expressions are not explicitly specified.
 * @param pivotColumn     The pivot column.
 * @param pivotValues     A sequence of values for the pivot column.
 * @param aggregates      The aggregation expressions, each with or without an alias.
 * @param child           Child operator
 */
case class Pivot(
    groupByExprsOpt: Option[Seq[NamedExpression]],
    pivotColumn: Expression,
    pivotValues: Seq[Expression],
    aggregates: Seq[Expression],
    child: LogicalPlan) extends UnaryNode {
  override lazy val resolved = false // Pivot will be replaced after being resolved.
  override def output: Seq[Attribute] = {
    val pivotAgg = aggregates match {
      case agg :: Nil =>
        pivotValues.map(value => AttributeReference(value.toString, agg.dataType)())
      case _ =>
        pivotValues.flatMap { value =>
          aggregates.map(agg => AttributeReference(value + "_" + agg.sql, agg.dataType)())
        }
    }
    groupByExprsOpt.getOrElse(Seq.empty).map(_.toAttribute) ++ pivotAgg
  }
  override def metadataOutput: Seq[Attribute] = Nil
  final override val nodePatterns: Seq[TreePattern] = Seq(PIVOT)

  override protected def withNewChildInternal(newChild: LogicalPlan): Pivot = copy(child = newChild)
}

/**
 * A constructor for creating an Unpivot, which will later be converted to an [[Expand]]
 * during the query analysis.
 *
 * An empty values array will be replaced during analysis with all resolved outputs of child except
 * the ids. This expansion allows to easily unpivot all non-id columns.
 *
 * @see `org.apache.spark.sql.catalyst.analysis.Analyzer.ResolveUnpivot`
 *
 * The type of the value column is derived from all value columns during analysis once all values
 * are resolved. All values' types have to be compatible, otherwise the result value column cannot
 * be assigned the individual values and an AnalysisException is thrown.
 *
 * @see `org.apache.spark.sql.catalyst.analysis.TypeCoercionBase.UnpivotCoercion`
 *
 * @param ids                Id columns
 * @param values             Value columns to unpivot
 * @param variableColumnName Name of the variable column
 * @param valueColumnName    Name of the value column
 * @param child              Child operator
 */
case class Unpivot(
    ids: Seq[NamedExpression],
    values: Seq[NamedExpression],
    variableColumnName: String,
    valueColumnName: String,
    child: LogicalPlan) extends UnaryNode {
  override lazy val resolved = false  // Unpivot will be replaced after being resolved.
  override def output: Seq[Attribute] = Nil
  override def metadataOutput: Seq[Attribute] = Nil
  final override val nodePatterns: Seq[TreePattern] = Seq(UNPIVOT)

  override protected def withNewChildInternal(newChild: LogicalPlan): Unpivot =
    copy(child = newChild)

  def valuesTypeCoercioned: Boolean = values.nonEmpty && values.forall(_.resolved) &&
    values.tail.forall(v => v.dataType.sameType(values.head.dataType))
}

/**
 * A constructor for creating a logical limit, which is split into two separate logical nodes:
 * a [[LocalLimit]], which is a partition local limit, followed by a [[GlobalLimit]].
 *
 * This muds the water for clean logical/physical separation, and is done for better limit pushdown.
 * In distributed query processing, a non-terminal global limit is actually an expensive operation
 * because it requires coordination (in Spark this is done using a shuffle).
 *
 * In most cases when we want to push down limit, it is often better to only push some partition
 * local limit. Consider the following:
 *
 *   GlobalLimit(Union(A, B))
 *
 * It is better to do
 *   GlobalLimit(Union(LocalLimit(A), LocalLimit(B)))
 *
 * than
 *   Union(GlobalLimit(A), GlobalLimit(B)).
 *
 * So we introduced LocalLimit and GlobalLimit in the logical plan node for limit pushdown.
 */
object Limit {
  def apply(limitExpr: Expression, child: LogicalPlan): UnaryNode = {
    GlobalLimit(limitExpr, LocalLimit(limitExpr, child))
  }

  def unapply(p: GlobalLimit): Option[(Expression, LogicalPlan)] = {
    p match {
      case GlobalLimit(le1, LocalLimit(le2, child)) if le1 == le2 => Some((le1, child))
      case _ => None
    }
  }
}

/**
 * A global (coordinated) limit. This operator can emit at most `limitExpr` number in total.
 *
 * See [[Limit]] for more information.
 *
 * Note that, we can not make it inherit [[OrderPreservingUnaryNode]] due to the different strategy
 * of physical plan. The output ordering of child will be broken if a shuffle exchange comes in
 * between the child and global limit, due to the fact that shuffle reader fetches blocks in random
 * order.
 */
case class GlobalLimit(limitExpr: Expression, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override def maxRows: Option[Long] = {
    limitExpr match {
      case IntegerLiteral(limit) => Some(limit)
      case _ => None
    }
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(LIMIT)

  override protected def withNewChildInternal(newChild: LogicalPlan): GlobalLimit =
    copy(child = newChild)
}

/**
 * A partition-local (non-coordinated) limit. This operator can emit at most `limitExpr` number
 * of tuples on each physical partition.
 *
 * See [[Limit]] for more information.
 */
case class LocalLimit(limitExpr: Expression, child: LogicalPlan) extends OrderPreservingUnaryNode {
  override def output: Seq[Attribute] = child.output

  override def maxRowsPerPartition: Option[Long] = {
    limitExpr match {
      case IntegerLiteral(limit) => Some(limit)
      case _ => None
    }
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(LIMIT)

  override protected def withNewChildInternal(newChild: LogicalPlan): LocalLimit =
    copy(child = newChild)
}

object OffsetAndLimit {
  def unapply(p: GlobalLimit): Option[(Int, Int, LogicalPlan)] = {
    p match {
      // Optimizer pushes local limit through offset, so we need to match the plan this way.
      case GlobalLimit(IntegerLiteral(globalLimit),
             Offset(IntegerLiteral(offset),
               LocalLimit(IntegerLiteral(localLimit), child)))
          if globalLimit + offset == localLimit =>
        Some((offset, globalLimit, child))
      case _ => None
    }
  }
}

object LimitAndOffset {
  def unapply(p: Offset): Option[(Int, Int, LogicalPlan)] = {
    p match {
      case Offset(IntegerLiteral(offset), Limit(IntegerLiteral(limit), child)) =>
        Some((limit, offset, child))
      case _ => None
    }
  }
}

/**
 * This is similar with [[Limit]] except:
 *
 * - It does not have plans for global/local separately because currently there is only single
 *   implementation which initially mimics both global/local tails. See
 *   `org.apache.spark.sql.execution.CollectTailExec` and
 *   `org.apache.spark.sql.execution.CollectLimitExec`
 *
 * - Currently, this plan can only be a root node.
 */
case class Tail(limitExpr: Expression, child: LogicalPlan) extends OrderPreservingUnaryNode {
  override def output: Seq[Attribute] = child.output
  override def maxRows: Option[Long] = {
    limitExpr match {
      case IntegerLiteral(limit) => Some(limit)
      case _ => None
    }
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): Tail = copy(child = newChild)
}

/**
 * Aliased subquery.
 *
 * @param identifier the alias identifier for this subquery.
 * @param child the logical plan of this subquery.
 */
case class SubqueryAlias(
    identifier: AliasIdentifier,
    child: LogicalPlan)
  extends OrderPreservingUnaryNode {

  def alias: String = identifier.name

  override def output: Seq[Attribute] = {
    val qualifierList = identifier.qualifier :+ alias
    child.output.map(_.withQualifier(qualifierList))
  }

  override def metadataOutput: Seq[Attribute] = {
    // Propagate metadata columns from leaf nodes through a chain of `SubqueryAlias`.
    if (child.isInstanceOf[LeafNode] || child.isInstanceOf[SubqueryAlias]) {
      val qualifierList = identifier.qualifier :+ alias
      val nonHiddenMetadataOutput = child.metadataOutput.filter(!_.qualifiedAccessOnly)
      nonHiddenMetadataOutput.map(_.withQualifier(qualifierList))
    } else {
      Nil
    }
  }

  override def maxRows: Option[Long] = child.maxRows

  override def doCanonicalize(): LogicalPlan = child.canonicalized

  final override val nodePatterns: Seq[TreePattern] = Seq(SUBQUERY_ALIAS)

  override protected def withNewChildInternal(newChild: LogicalPlan): SubqueryAlias =
    copy(child = newChild)
}

object SubqueryAlias {
  def apply(
      identifier: String,
      child: LogicalPlan): SubqueryAlias = {
    SubqueryAlias(AliasIdentifier(identifier), child)
  }

  def apply(
      identifier: String,
      database: String,
      child: LogicalPlan): SubqueryAlias = {
    SubqueryAlias(AliasIdentifier(identifier, Seq(database)), child)
  }

  def apply(
      multipartIdentifier: Seq[String],
      child: LogicalPlan): SubqueryAlias = {
    SubqueryAlias(AliasIdentifier(multipartIdentifier.last, multipartIdentifier.init), child)
  }
}
/**
 * Sample the dataset.
 *
 * @param lowerBound Lower-bound of the sampling probability (usually 0.0)
 * @param upperBound Upper-bound of the sampling probability. The expected fraction sampled
 *                   will be ub - lb.
 * @param withReplacement Whether to sample with replacement.
 * @param seed the random seed
 * @param child the LogicalPlan
 */
case class Sample(
    lowerBound: Double,
    upperBound: Double,
    withReplacement: Boolean,
    seed: Long,
    child: LogicalPlan) extends UnaryNode {

  val eps = RandomSampler.roundingEpsilon
  val fraction = upperBound - lowerBound
  if (withReplacement) {
    require(
      fraction >= 0.0 - eps,
      s"Sampling fraction ($fraction) must be nonnegative with replacement")
  } else {
    require(
      fraction >= 0.0 - eps && fraction <= 1.0 + eps,
      s"Sampling fraction ($fraction) must be on interval [0, 1] without replacement")
  }

  // when withReplacement is true, PoissonSampler is applied in SampleExec,
  // which may output more rows than child.
  override def maxRows: Option[Long] = {
    if (withReplacement) None else child.maxRows
  }
  override def maxRowsPerPartition: Option[Long] = {
    if (withReplacement) None else child.maxRowsPerPartition
  }

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: LogicalPlan): Sample =
    copy(child = newChild)
}

/**
 * Returns a new logical plan that dedups input rows.
 */
case class Distinct(child: LogicalPlan) extends UnaryNode {
  override def maxRows: Option[Long] = child.maxRows
  override def output: Seq[Attribute] = child.output
  final override val nodePatterns: Seq[TreePattern] = Seq(DISTINCT_LIKE)
  override protected def withNewChildInternal(newChild: LogicalPlan): Distinct =
    copy(child = newChild)
}

/**
 * A base interface for [[RepartitionByExpression]] and [[Repartition]]
 */
abstract class RepartitionOperation extends UnaryNode {
  def shuffle: Boolean
  def numPartitions: Int
  override final def maxRows: Option[Long] = child.maxRows
  override def output: Seq[Attribute] = child.output
  final override val nodePatterns: Seq[TreePattern] = Seq(REPARTITION_OPERATION)
  def partitioning: Partitioning
}

/**
 * Returns a new RDD that has exactly `numPartitions` partitions. Differs from
 * [[RepartitionByExpression]] as this method is called directly by DataFrame's, because the user
 * asked for `coalesce` or `repartition`. [[RepartitionByExpression]] is used when the consumer
 * of the output requires some specific ordering or distribution of the data.
 */
case class Repartition(numPartitions: Int, shuffle: Boolean, child: LogicalPlan)
  extends RepartitionOperation {
  require(numPartitions > 0, s"Number of partitions ($numPartitions) must be positive.")

  override def partitioning: Partitioning = {
    require(shuffle, "Partitioning can only be used in shuffle.")
    numPartitions match {
      case 1 => SinglePartition
      case _ => RoundRobinPartitioning(numPartitions)
    }
  }
  override protected def withNewChildInternal(newChild: LogicalPlan): Repartition =
    copy(child = newChild)
}

trait HasPartitionExpressions extends SQLConfHelper {

  val numPartitions = optNumPartitions.getOrElse(conf.numShufflePartitions)
  require(numPartitions > 0, s"Number of partitions ($numPartitions) must be positive.")

  def partitionExpressions: Seq[Expression]

  def optNumPartitions: Option[Int]

  protected def partitioning: Partitioning = if (partitionExpressions.isEmpty) {
    RoundRobinPartitioning(numPartitions)
  } else {
    val (sortOrder, nonSortOrder) = partitionExpressions.partition(_.isInstanceOf[SortOrder])
    require(sortOrder.isEmpty || nonSortOrder.isEmpty,
      s"${getClass.getSimpleName} expects that either all its `partitionExpressions` are of type " +
        "`SortOrder`, which means `RangePartitioning`, or none of them are `SortOrder`, which " +
        "means `HashPartitioning`. In this case we have:" +
        s"""
           |SortOrder: $sortOrder
           |NonSortOrder: $nonSortOrder
       """.stripMargin)
    if (sortOrder.nonEmpty) {
      RangePartitioning(sortOrder.map(_.asInstanceOf[SortOrder]), numPartitions)
    } else {
      HashPartitioning(partitionExpressions, numPartitions)
    }
  }
}

/**
 * This method repartitions data using [[Expression]]s into `optNumPartitions`, and receives
 * information about the number of partitions during execution. Used when a specific ordering or
 * distribution is expected by the consumer of the query result. Use [[Repartition]] for RDD-like
 * `coalesce` and `repartition`. If no `optNumPartitions` is given, by default it partitions data
 * into `numShufflePartitions` defined in `SQLConf`, and could be coalesced by AQE.
 */
case class RepartitionByExpression(
    partitionExpressions: Seq[Expression],
    child: LogicalPlan,
    optNumPartitions: Option[Int]) extends RepartitionOperation with HasPartitionExpressions {

  override val partitioning: Partitioning = {
    if (numPartitions == 1) {
      SinglePartition
    } else {
      super.partitioning
    }
  }

  override def shuffle: Boolean = true

  override protected def withNewChildInternal(newChild: LogicalPlan): RepartitionByExpression =
    copy(child = newChild)
}

object RepartitionByExpression {
  def apply(
      partitionExpressions: Seq[Expression],
      child: LogicalPlan,
      numPartitions: Int): RepartitionByExpression = {
    RepartitionByExpression(partitionExpressions, child, Some(numPartitions))
  }
}

/**
 * This operator is used to rebalance the output partitions of the given `child`, so that every
 * partition is of a reasonable size (not too small and not too big). It also try its best to
 * partition the child output by `partitionExpressions`. If there are skews, Spark will split the
 * skewed partitions, to make these partitions not too big. This operator is useful when you need
 * to write the result of `child` to a table, to avoid too small/big files.
 *
 * Note that, this operator only makes sense when AQE is enabled.
 */
case class RebalancePartitions(
    partitionExpressions: Seq[Expression],
    child: LogicalPlan,
    optNumPartitions: Option[Int] = None) extends UnaryNode with HasPartitionExpressions {
  override def maxRows: Option[Long] = child.maxRows
  override def output: Seq[Attribute] = child.output
  override val nodePatterns: Seq[TreePattern] = Seq(REBALANCE_PARTITIONS)

  override val partitioning: Partitioning = super.partitioning

  override protected def withNewChildInternal(newChild: LogicalPlan): RebalancePartitions =
    copy(child = newChild)
}

/**
 * A relation with one row. This is used in "SELECT ..." without a from clause.
 */
case class OneRowRelation() extends LeafNode {
  override def maxRows: Option[Long] = Some(1)
  override def output: Seq[Attribute] = Nil
  override def computeStats(): Statistics = Statistics(sizeInBytes = 1)

  /** [[org.apache.spark.sql.catalyst.trees.TreeNode.makeCopy()]] does not support 0-arg ctor. */
  override def makeCopy(newArgs: Array[AnyRef]): OneRowRelation = {
    val newCopy = OneRowRelation()
    newCopy.copyTagsFrom(this)
    newCopy
  }
}

/** A logical plan for `dropDuplicates`. */
case class Deduplicate(
    keys: Seq[Attribute],
    child: LogicalPlan) extends UnaryNode {
  override def maxRows: Option[Long] = child.maxRows
  override def output: Seq[Attribute] = child.output
  final override val nodePatterns: Seq[TreePattern] = Seq(DISTINCT_LIKE)
  override protected def withNewChildInternal(newChild: LogicalPlan): Deduplicate =
    copy(child = newChild)
}

/**
 * A trait to represent the commands that support subqueries.
 * This is used to allow such commands in the subquery-related checks.
 */
trait SupportsSubquery extends LogicalPlan

/**
 * Collect arbitrary (named) metrics from a dataset. As soon as the query reaches a completion
 * point (batch query completes or streaming query epoch completes) an event is emitted on the
 * driver which can be observed by attaching a listener to the spark session. The metrics are named
 * so we can collect metrics at multiple places in a single dataset.
 *
 * This node behaves like a global aggregate. All the metrics collected must be aggregate functions
 * or be literals.
 */
case class CollectMetrics(
    name: String,
    metrics: Seq[NamedExpression],
    child: LogicalPlan)
  extends UnaryNode {

  override lazy val resolved: Boolean = {
    name.nonEmpty && metrics.nonEmpty && metrics.forall(_.resolved) && childrenResolved
  }

  override def maxRows: Option[Long] = child.maxRows
  override def maxRowsPerPartition: Option[Long] = child.maxRowsPerPartition
  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: LogicalPlan): CollectMetrics =
    copy(child = newChild)
}

/**
 * A placeholder for domain join that can be added when decorrelating subqueries.
 * It should be rewritten during the optimization phase.
 */
case class DomainJoin(
    domainAttrs: Seq[Attribute],
    child: LogicalPlan,
    joinType: JoinType = Inner,
    condition: Option[Expression] = None) extends UnaryNode {

  require(Seq(Inner, LeftOuter).contains(joinType), s"Unsupported domain join type $joinType")

  override def output: Seq[Attribute] = joinType match {
    case LeftOuter => domainAttrs ++ child.output.map(_.withNullability(true))
    case _ => domainAttrs ++ child.output
  }

  override def producedAttributes: AttributeSet = AttributeSet(domainAttrs)

  override protected def withNewChildInternal(newChild: LogicalPlan): DomainJoin =
    copy(child = newChild)
}

/**
 * A logical plan for lateral join.
 */
case class LateralJoin(
    left: LogicalPlan,
    right: LateralSubquery,
    joinType: JoinType,
    condition: Option[Expression]) extends UnaryNode {

  require(Seq(Inner, LeftOuter, Cross).contains(joinType),
    s"Unsupported lateral join type $joinType")

  override def child: LogicalPlan = left

  override def output: Seq[Attribute] = {
    joinType match {
      case LeftOuter => left.output ++ right.plan.output.map(_.withNullability(true))
      case _ => left.output ++ right.plan.output
    }
  }

  private[this] lazy val childAttributes = AttributeSeq(left.output ++ right.plan.output)

  private[this] lazy val childMetadataAttributes =
    AttributeSeq(left.metadataOutput ++ right.plan.metadataOutput)

  /**
   * Optionally resolves the given strings to a [[NamedExpression]] using the input from
   * both the left plan and the lateral subquery's plan.
   */
  override def resolveChildren(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] = {
    childAttributes.resolve(nameParts, resolver)
      .orElse(childMetadataAttributes.resolve(nameParts, resolver))
  }

  override def childrenResolved: Boolean = left.resolved && right.resolved

  def duplicateResolved: Boolean = left.outputSet.intersect(right.plan.outputSet).isEmpty

  override lazy val resolved: Boolean = {
    childrenResolved &&
      expressions.forall(_.resolved) &&
      duplicateResolved &&
      condition.forall(_.dataType == BooleanType)
  }

  override def producedAttributes: AttributeSet = AttributeSet(right.plan.output)

  final override val nodePatterns: Seq[TreePattern] = Seq(LATERAL_JOIN)

  override protected def withNewChildInternal(newChild: LogicalPlan): LateralJoin = {
    copy(left = newChild)
  }
}

/**
 * A logical plan for as-of join.
 */
case class AsOfJoin(
    left: LogicalPlan,
    right: LogicalPlan,
    asOfCondition: Expression,
    condition: Option[Expression],
    joinType: JoinType,
    orderExpression: Expression,
    toleranceAssertion: Option[Expression]) extends BinaryNode {

  require(Seq(Inner, LeftOuter).contains(joinType),
    s"Unsupported as-of join type $joinType")

  override protected def stringArgs: Iterator[Any] = super.stringArgs.take(5)

  override def output: Seq[Attribute] = {
    joinType match {
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case _ =>
        left.output ++ right.output
    }
  }

  def duplicateResolved: Boolean = left.outputSet.intersect(right.outputSet).isEmpty

  override lazy val resolved: Boolean = {
    childrenResolved &&
      expressions.forall(_.resolved) &&
      duplicateResolved &&
      asOfCondition.dataType == BooleanType &&
      condition.forall(_.dataType == BooleanType) &&
      toleranceAssertion.forall { assertion =>
        assertion.foldable && assertion.eval().asInstanceOf[Boolean]
      }
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(AS_OF_JOIN)

  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan, newRight: LogicalPlan): AsOfJoin = {
    copy(left = newLeft, right = newRight)
  }
}

object AsOfJoin {

  def apply(
      left: LogicalPlan,
      right: LogicalPlan,
      leftAsOf: Expression,
      rightAsOf: Expression,
      condition: Option[Expression],
      joinType: JoinType,
      tolerance: Option[Expression],
      allowExactMatches: Boolean,
      direction: AsOfJoinDirection): AsOfJoin = {
    val asOfCond = makeAsOfCond(leftAsOf, rightAsOf, tolerance, allowExactMatches, direction)
    val orderingExpr = makeOrderingExpr(leftAsOf, rightAsOf, direction)
    AsOfJoin(left, right, asOfCond, condition, joinType,
      orderingExpr, tolerance.map(t => GreaterThanOrEqual(t, Literal.default(t.dataType))))
  }

  private def makeAsOfCond(
      leftAsOf: Expression,
      rightAsOf: Expression,
      tolerance: Option[Expression],
      allowExactMatches: Boolean,
      direction: AsOfJoinDirection): Expression = {
    val base = (allowExactMatches, direction) match {
      case (true, Backward) => GreaterThanOrEqual(leftAsOf, rightAsOf)
      case (false, Backward) => GreaterThan(leftAsOf, rightAsOf)
      case (true, Forward) => LessThanOrEqual(leftAsOf, rightAsOf)
      case (false, Forward) => LessThan(leftAsOf, rightAsOf)
      case (true, Nearest) => Literal.TrueLiteral
      case (false, Nearest) => Not(EqualTo(leftAsOf, rightAsOf))
    }
    tolerance match {
      case Some(tolerance) =>
        (allowExactMatches, direction) match {
          case (true, Backward) =>
            And(base, GreaterThanOrEqual(rightAsOf, Subtract(leftAsOf, tolerance)))
          case (false, Backward) =>
            And(base, GreaterThan(rightAsOf, Subtract(leftAsOf, tolerance)))
          case (true, Forward) =>
            And(base, LessThanOrEqual(rightAsOf, Add(leftAsOf, tolerance)))
          case (false, Forward) =>
            And(base, LessThan(rightAsOf, Add(leftAsOf, tolerance)))
          case (true, Nearest) =>
            And(GreaterThanOrEqual(rightAsOf, Subtract(leftAsOf, tolerance)),
              LessThanOrEqual(rightAsOf, Add(leftAsOf, tolerance)))
          case (false, Nearest) =>
            And(base,
              And(GreaterThan(rightAsOf, Subtract(leftAsOf, tolerance)),
                LessThan(rightAsOf, Add(leftAsOf, tolerance))))
        }
      case None => base
    }
  }

  private def makeOrderingExpr(
      leftAsOf: Expression,
      rightAsOf: Expression,
      direction: AsOfJoinDirection): Expression = {
    direction match {
      case Backward => Subtract(leftAsOf, rightAsOf)
      case Forward => Subtract(rightAsOf, leftAsOf)
      case Nearest =>
        If(GreaterThan(leftAsOf, rightAsOf),
          Subtract(leftAsOf, rightAsOf), Subtract(rightAsOf, leftAsOf))
    }
  }
}
