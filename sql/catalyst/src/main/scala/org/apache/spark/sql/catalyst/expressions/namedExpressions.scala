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

package org.apache.spark.sql.catalyst.expressions

import java.util.{Objects, UUID}

import org.apache.spark.{SparkException, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.catalyst.trees.TreePattern
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, METADATA_COL_ATTR_KEY}
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.BitSet
import org.apache.spark.util.collection.ImmutableBitSet

object NamedExpression {
  private val curId = new java.util.concurrent.atomic.AtomicLong()
  private[expressions] val jvmId = UUID.randomUUID()
  def newExprId: ExprId = ExprId(curId.getAndIncrement(), jvmId)
  def unapply(expr: NamedExpression): Option[(String, DataType)] = Some((expr.name, expr.dataType))
}

/**
 * A globally unique id for a given named expression.
 * Used to identify which attribute output by a relation is being
 * referenced in a subsequent computation.
 *
 * The `id` field is unique within a given JVM, while the `uuid` is used to uniquely identify JVMs.
 */
case class ExprId(id: Long, jvmId: UUID) {

  override def equals(other: Any): Boolean = other match {
    case ExprId(id, jvmId) => this.id == id && this.jvmId == jvmId
    case _ => false
  }

  override def hashCode(): Int = id.hashCode()

}

object ExprId {
  def apply(id: Long): ExprId = ExprId(id, NamedExpression.jvmId)
}

/**
 * An [[Expression]] that is named.
 */
trait NamedExpression extends Expression {

  /** We should never fold named expressions in order to not remove the alias. */
  override def foldable: Boolean = false

  def name: String
  def exprId: ExprId

  /**
   * Returns a dot separated fully qualified name for this attribute.  If the name or any qualifier
   * contains `dots`, it is quoted to avoid confusion.  Given that there can be multiple qualifiers,
   * it is possible that there are other possible way to refer to this attribute.
   */
  def qualifiedName: String = (qualifier :+ name).map(quoteIfNeeded).mkString(".")

  /**
   * Optional qualifier for the expression.
   * Qualifier can also contain the fully qualified information, for e.g, Sequence of string
   * containing the database and the table name
   *
   * For now, since we do not allow using original table name to qualify a column name once the
   * table is aliased, this can only be:
   *
   * 1. Empty Seq: when an attribute doesn't have a qualifier,
   *    e.g. top level attributes aliased in the SELECT clause, or column from a LocalRelation.
   * 2. Seq with a Single element: either the table name or the alias name of the table.
   * 3. Seq with 2 elements: database name and table name
   * 4. Seq with 3 elements: catalog name, database name and table name
   */
  def qualifier: Seq[String]

  def toAttribute: Attribute

  /** Returns the metadata when an expression is a reference to another expression with metadata. */
  def metadata: Metadata = Metadata.empty

  /** Returns a copy of this expression with a new `exprId`. */
  def newInstance(): NamedExpression
}

abstract class Attribute extends LeafExpression with NamedExpression with NullIntolerant {

  @transient
  override lazy val references: AttributeSet = AttributeSet(this)

  def withNullability(newNullability: Boolean): Attribute
  def withQualifier(newQualifier: Seq[String]): Attribute
  def withName(newName: String): Attribute
  def withMetadata(newMetadata: Metadata): Attribute
  def withExprId(newExprId: ExprId): Attribute
  def withDataType(newType: DataType): Attribute

  override def toAttribute: Attribute = this
  def newInstance(): Attribute

}

/**
 * Used to assign a new name to a computation.
 * For example the SQL expression "1 + 1 AS a" could be represented as follows:
 *  Alias(Add(Literal(1), Literal(1)), "a")()
 *
 * Note that exprId and qualifiers are in a separate parameter list because
 * we only pattern match on child and name.
 *
 * Note that when creating a new Alias, all the [[AttributeReference]] that refer to
 * the original alias should be updated to the new one.
 *
 * @param child The computation being performed
 * @param name The name to be associated with the result of computing [[child]].
 * @param exprId A globally unique id used to check if an [[AttributeReference]] refers to this
 *               alias. Auto-assigned if left blank.
 * @param qualifier An optional Seq of string that can be used to refer to this attribute in a
 *                  fully qualified way. Consider the examples tableName.name, subQueryAlias.name.
 *                  tableName and subQueryAlias are possible qualifiers.
 * @param explicitMetadata Explicit metadata associated with this alias that overwrites child's.
 * @param nonInheritableMetadataKeys Keys of metadata entries that are supposed to be removed when
 *                                   inheriting the metadata from the child.
 */
case class Alias(child: Expression, name: String)(
    val exprId: ExprId = NamedExpression.newExprId,
    val qualifier: Seq[String] = Seq.empty,
    val explicitMetadata: Option[Metadata] = None,
    val nonInheritableMetadataKeys: Seq[String] = Seq.empty)
  extends UnaryExpression with NamedExpression {

  final override val nodePatterns: Seq[TreePattern] = Seq(ALIAS)

  // Alias(Generator, xx) need to be transformed into Generate(generator, ...)
  override lazy val resolved =
    childrenResolved && checkInputDataTypes().isSuccess && !child.isInstanceOf[Generator]

  override def eval(input: InternalRow): Any = child.eval(input)

  /** Just a simple passthrough for code generation. */
  override def genCode(ctx: CodegenContext): ExprCode = child.genCode(ctx)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    throw SparkException.internalError("Alias.doGenCode should not be called.")
  }

  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable
  override def metadata: Metadata = {
    explicitMetadata.getOrElse {
      child match {
        case named: NamedExpression => removeNonInheritableMetadata(named.metadata)
        case structField: GetStructField => removeNonInheritableMetadata(structField.metadata)
        case _ => Metadata.empty
      }
    }
  }

  def withName(newName: String): NamedExpression = {
    Alias(child, newName)(
      exprId = exprId,
      qualifier = qualifier,
      explicitMetadata = explicitMetadata,
      nonInheritableMetadataKeys = nonInheritableMetadataKeys)
  }

  def newInstance(): NamedExpression =
    Alias(child, name)(
      qualifier = qualifier,
      explicitMetadata = explicitMetadata,
      nonInheritableMetadataKeys = nonInheritableMetadataKeys)

  override def toAttribute: Attribute = {
    if (resolved) {
      AttributeReference(name, child.dataType, child.nullable, metadata)(exprId, qualifier)
    } else {
      UnresolvedAttribute.quoted(name)
    }
  }

  /** Used to signal the column used to calculate an eventTime watermark (e.g. a#1-T{delayMs}) */
  private def delaySuffix = if (metadata.contains(EventTimeWatermark.delayKey)) {
    s"-T${metadata.getLong(EventTimeWatermark.delayKey)}ms"
  } else {
    ""
  }

  /**
   * This function is performance-sensitive, so we should avoid `MetadataBuilder` manipulation,
   * because it performs heavy operations on maps
   */
  private def removeNonInheritableMetadata(metadata: Metadata): Metadata = {
    if (metadata.isEmpty || nonInheritableMetadataKeys.forall(!metadata.contains(_))) {
      metadata
    } else {
      val builder = new MetadataBuilder().withMetadata(metadata)
      nonInheritableMetadataKeys.foreach(builder.remove)
      builder.build()
    }
  }

  override def toString: String = s"$child AS $name#${exprId.id}$typeSuffix$delaySuffix"

  override protected final def otherCopyArgs: Seq[AnyRef] = {
    exprId :: qualifier :: explicitMetadata :: nonInheritableMetadataKeys :: Nil
  }

  override def hashCode(): Int = {
    val state = Seq(name, exprId, child, qualifier, explicitMetadata)
    state.map(Objects.hashCode).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def equals(other: Any): Boolean = other match {
    case a: Alias =>
      name == a.name && exprId == a.exprId && child == a.child && qualifier == a.qualifier &&
        explicitMetadata == a.explicitMetadata &&
        nonInheritableMetadataKeys == a.nonInheritableMetadataKeys
    case _ => false
  }

  override def sql: String = {
    val qualifierPrefix =
      if (qualifier.nonEmpty) qualifier.map(quoteIfNeeded).mkString(".") + "." else ""
    s"${child.sql} AS $qualifierPrefix${quoteIfNeeded(name)}"
  }

  override protected def withNewChildInternal(newChild: Expression): Alias =
    copy(child = newChild)(exprId, qualifier, explicitMetadata, nonInheritableMetadataKeys)
}

// Singleton tree pattern BitSet for all AttributeReference instances.
object AttributeReferenceTreeBits {
  val bits: BitSet = new ImmutableBitSet(TreePattern.maxId, ATTRIBUTE_REFERENCE.id)
}

/**
 * A reference to an attribute produced by another operator in the tree.
 *
 * @param name The name of this attribute, should only be used during analysis or for debugging.
 * @param dataType The [[DataType]] of this attribute.
 * @param nullable True if null is a valid value for this attribute.
 * @param metadata The metadata of this attribute.
 * @param exprId A globally unique id used to check if different AttributeReferences refer to the
 *               same attribute.
 * @param qualifier An optional string that can be used to referred to this attribute in a fully
 *                  qualified way. Consider the examples tableName.name, subQueryAlias.name.
 *                  tableName and subQueryAlias are possible qualifiers.
 */
case class AttributeReference(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    override val metadata: Metadata = Metadata.empty)(
    val exprId: ExprId = NamedExpression.newExprId,
    val qualifier: Seq[String] = Seq.empty[String])
  extends Attribute with Unevaluable {

  override lazy val treePatternBits: BitSet = AttributeReferenceTreeBits.bits

  /**
   * Returns true iff the expression id is the same for both attributes.
   */
  def sameRef(other: AttributeReference): Boolean = this.exprId == other.exprId

  override def equals(other: Any): Boolean = other match {
    case ar: AttributeReference =>
      name == ar.name && dataType == ar.dataType && nullable == ar.nullable &&
        metadata == ar.metadata && exprId == ar.exprId && qualifier == ar.qualifier
    case _ => false
  }

  override def semanticHash(): Int = {
    this.exprId.hashCode()
  }

  override def hashCode: Int = {
    // See http://stackoverflow.com/questions/113511/hash-code-implementation
    var h = 17
    h = h * 37 + name.hashCode()
    h = h * 37 + dataType.hashCode()
    h = h * 37 + nullable.hashCode()
    h = h * 37 + metadata.hashCode()
    h = h * 37 + exprId.hashCode()
    h = h * 37 + qualifier.hashCode()
    h
  }

  override lazy val canonicalized: Expression = {
    AttributeReference("none", dataType)(exprId)
  }

  override def newInstance(): AttributeReference =
    AttributeReference(name, dataType, nullable, metadata)(qualifier = qualifier)

  /**
   * Returns a copy of this [[AttributeReference]] with changed nullability.
   */
  override def withNullability(newNullability: Boolean): AttributeReference = {
    if (nullable == newNullability) {
      this
    } else {
      AttributeReference(name, dataType, newNullability, metadata)(exprId, qualifier)
    }
  }

  override def withName(newName: String): AttributeReference = {
    if (name == newName) {
      this
    } else {
      AttributeReference(newName, dataType, nullable, metadata)(exprId, qualifier)
    }
  }

  /**
   * Returns a copy of this [[AttributeReference]] with new qualifier.
   */
  override def withQualifier(newQualifier: Seq[String]): AttributeReference = {
    if (newQualifier == qualifier) {
      this
    } else {
      AttributeReference(name, dataType, nullable, metadata)(exprId, newQualifier)
    }
  }

  override def withExprId(newExprId: ExprId): AttributeReference = {
    if (exprId == newExprId) {
      this
    } else {
      AttributeReference(name, dataType, nullable, metadata)(newExprId, qualifier)
    }
  }

  override def withMetadata(newMetadata: Metadata): AttributeReference = {
    AttributeReference(name, dataType, nullable, newMetadata)(exprId, qualifier)
  }

  override def withDataType(newType: DataType): AttributeReference = {
    AttributeReference(name, newType, nullable, metadata)(exprId, qualifier)
  }

  override protected final def otherCopyArgs: Seq[AnyRef] = {
    exprId :: qualifier :: Nil
  }

  /** Used to signal the column used to calculate an eventTime watermark (e.g. a#1-T{delayMs}) */
  private def delaySuffix = if (metadata.contains(EventTimeWatermark.delayKey)) {
    s"-T${metadata.getLong(EventTimeWatermark.delayKey)}ms"
  } else {
    ""
  }

  override def toString: String = s"$name#${exprId.id}$typeSuffix$delaySuffix"

  // Since the expression id is not in the first constructor it is missing from the default
  // tree string.
  override def simpleString(maxFields: Int): String = {
    s"$name#${exprId.id}: ${dataType.simpleString(maxFields)}"
  }

  override def sql: String = {
    val qualifierPrefix =
      if (qualifier.nonEmpty) qualifier.map(quoteIfNeeded).mkString(".") + "." else ""
    s"$qualifierPrefix${quoteIfNeeded(name)}"
  }
}

/**
 * A place holder used when printing expressions without debugging information such as the
 * expression id or the unresolved indicator.
 */
case class PrettyAttribute(
    name: String,
    dataType: DataType = NullType)
  extends Attribute with Unevaluable {

  def this(attribute: Attribute) = this(attribute.name, attribute match {
    case a: AttributeReference => a.dataType
    case a: PrettyAttribute => a.dataType
    case _ => NullType
  })

  override def toString: String = name
  override def sql: String = toString

  override def withNullability(newNullability: Boolean): Attribute =
    throw SparkUnsupportedOperationException()
  override def newInstance(): Attribute =
    throw SparkUnsupportedOperationException()
  override def withQualifier(newQualifier: Seq[String]): Attribute =
    throw SparkUnsupportedOperationException()
  override def withName(newName: String): Attribute =
    throw SparkUnsupportedOperationException()
  override def withMetadata(newMetadata: Metadata): Attribute =
    throw SparkUnsupportedOperationException()
  override def qualifier: Seq[String] =
    throw SparkUnsupportedOperationException()
  override def exprId: ExprId =
    throw SparkUnsupportedOperationException()
  override def withExprId(newExprId: ExprId): Attribute =
    throw SparkUnsupportedOperationException()
  override def withDataType(newType: DataType): Attribute =
    throw SparkUnsupportedOperationException()
  override def nullable: Boolean = true
}

/**
 * A place holder used to hold a reference that has been resolved to a field outside of the current
 * plan. This is used for correlated subqueries.
 */
case class OuterReference(e: NamedExpression)
  extends LeafExpression with NamedExpression with Unevaluable {
  override def dataType: DataType = e.dataType
  override def nullable: Boolean = e.nullable
  override def prettyName: String = "outer"

  override def sql: String = s"$prettyName(${e.sql})"
  override def name: String = e.name
  override def qualifier: Seq[String] = e.qualifier
  override def exprId: ExprId = e.exprId
  override def toAttribute: Attribute = e.toAttribute
  override def newInstance(): NamedExpression = OuterReference(e.newInstance())
  final override val nodePatterns: Seq[TreePattern] = Seq(OUTER_REFERENCE)
}

/**
 * A placeholder used to hold a [[NamedExpression]] that has been temporarily resolved as the
 * reference to a lateral column alias. It will be restored back to [[UnresolvedAttribute]] if
 * the lateral column alias can't be resolved, or become a normal resolved column in the rewritten
 * plan after lateral column resolution. There should be no [[LateralColumnAliasReference]] beyond
 * analyzer: if the plan passes all analysis check, then all [[LateralColumnAliasReference]] should
 * already be removed.
 *
 * @param ne the [[NamedExpression]] produced by column resolution. Can be [[UnresolvedAttribute]]
 *           if the referenced lateral column alias is not resolved yet.
 * @param nameParts the name parts of the original [[UnresolvedAttribute]]. Used to restore back
 *                  to [[UnresolvedAttribute]] when needed
 * @param a the attribute of referenced lateral column alias. Used to match alias when unwrapping
 *          and resolving lateral column aliases and rewriting the query plan.
 */
case class LateralColumnAliasReference(ne: NamedExpression, nameParts: Seq[String], a: Attribute)
  extends LeafExpression with NamedExpression with Unevaluable {
  assert(ne.resolved || ne.isInstanceOf[UnresolvedAttribute])
  override def name: String = ne.name
  override def exprId: ExprId = ne.exprId
  override def qualifier: Seq[String] = ne.qualifier
  override def toAttribute: Attribute = ne.toAttribute
  override lazy val resolved = ne.resolved
  override def newInstance(): NamedExpression =
    LateralColumnAliasReference(ne.newInstance(), nameParts, a)

  override def nullable: Boolean = ne.nullable
  override def dataType: DataType = ne.dataType
  override def prettyName: String = "lateralAliasReference"
  override def sql: String = s"$prettyName($name)"
  override def toString: String = sql

  final override val nodePatterns: Seq[TreePattern] = Seq(LATERAL_COLUMN_ALIAS_REFERENCE)
}

object VirtualColumn {
  // The attribute name used by Hive, which has different result than Spark, deprecated.
  val hiveGroupingIdName: String = "grouping__id"
  val groupingIdName: String = "spark_grouping_id"
  val groupingIdAttribute: UnresolvedAttribute = UnresolvedAttribute(groupingIdName)
}

/**
 * The internal representation of the MetadataAttribute,
 * it sets `__metadata_col` to `true` in AttributeReference metadata
 * - apply() will create a metadata attribute reference
 * - unapply() will check if an attribute reference is the metadata attribute reference
 */
object MetadataAttribute {

  def apply(name: String, dataType: DataType, nullable: Boolean = true): AttributeReference =
    AttributeReference(name, dataType, nullable, metadata(name))()

  def unapply(attr: AttributeReference): Option[AttributeReference] =
    if (isValid(attr.metadata)) Some(attr) else None

  def metadata(name: String): Metadata = new MetadataBuilder()
    .putString(METADATA_COL_ATTR_KEY, name)
    .build()

  def isValid(metadata: Metadata): Boolean =
    metadata.contains(METADATA_COL_ATTR_KEY)
}

/**
 * A [[MetadataAttribute]] that works even if the attribute was renamed to avoid a conflict with
 * some column name in the schema. See also [[LogicalPlan.getMetadataAttributeByName]].
 *
 * - apply() creates a logically-named attribute with the given physical name.
 * - unapply() matches a [[MetadataAttribute]] and also returns its logical name.
 */
object MetadataAttributeWithLogicalName {
  def unapply(attr: AttributeReference): Option[(AttributeReference, String)] = attr match {
    case MetadataAttribute(a) => Some(a -> a.metadata.getString(METADATA_COL_ATTR_KEY))
    case _ => None
  }
}

object MetadataStructFieldWithLogicalName {
  def unapply(field: StructField): Option[(StructField, String)] = MetadataAttributeWithLogicalName
    .unapply(DataTypeUtils.toAttribute(field))
    .map { case (_, name) => field -> name }
}

/**
 * The internal representation of the FileSourceMetadataAttribute, it sets `__metadata_col`
 * and `__file_source_metadata_col` to `true` in AttributeReference's metadata.
 * This is a super type of [[FileSourceConstantMetadataAttribute]] and
 * [[FileSourceGeneratedMetadataAttribute]].
 *
 * - apply() will create a file source metadata attribute reference
 * - unapply() will check if an attribute reference is any file source metadata attribute reference
 */
object FileSourceMetadataAttribute {

  val FILE_SOURCE_METADATA_COL_ATTR_KEY = "__file_source_metadata_col"

  /**
   * Removes the internal field metadata.
   */
  def cleanupFileSourceMetadataInformation(attr: Attribute): Attribute =
    attr.withMetadata(removeInternalMetadata(attr.metadata))

  /**
   * Removes the internal field metadata.
   */
  def cleanupFileSourceMetadataInformation(field: StructField): StructField =
    field.copy(metadata = removeInternalMetadata(field.metadata))

  def apply(name: String, dataType: DataType, nullable: Boolean = false): AttributeReference =
    AttributeReference(name, dataType, nullable = nullable, metadata(name))()

  /** Matches if attr is any File source metadata attribute (including constant and generated). */
  def unapply(attr: AttributeReference): Option[AttributeReference] =
    if (isValid(attr.metadata)) Some(attr) else None

  def metadata(name: String): Metadata = new MetadataBuilder()
    .withMetadata(MetadataAttribute.metadata(name))
    .putBoolean(FILE_SOURCE_METADATA_COL_ATTR_KEY, value = true)
    .build()

  def isValid(metadata: Metadata): Boolean = {
    MetadataAttribute.isValid(metadata) &&
    metadata.contains(FILE_SOURCE_METADATA_COL_ATTR_KEY) &&
    metadata.getBoolean(FILE_SOURCE_METADATA_COL_ATTR_KEY)
  }

  /**
   * True if the given data type is supported in file source metadata attributes.
   *
   * The set of supported types is limited by [[ColumnVectorUtils.populate]], which the constant
   * file metadata implementation relies on. In general, types that can be partition columns are
   * supported (including most primitive types). Notably unsupported types include [[ObjectType]],
   * [[UserDefinedType]], and the complex types ([[StructType]], [[MapType]], [[ArrayType]]).
   */
  def isSupportedType(dataType: DataType): Boolean = PhysicalDataType(dataType) match {
    // PhysicalPrimitiveType covers: Boolean, Byte, Double, Float, Integer, Long, Null, Short
    case _: PhysicalPrimitiveType | _: PhysicalDecimalType => true
    case PhysicalBinaryType | PhysicalStringType(_) | PhysicalCalendarIntervalType => true
    case _ => false
  }

  /** Returns the type unchanged if valid; otherwise throws [[IllegalArgumentException]]. */
  def validateType(dataType: DataType): DataType = {
    require(isSupportedType(dataType), s"Unsupported data type: $dataType")
    dataType
  }

  private def removeInternalMetadata(metadata: Metadata) = new MetadataBuilder()
    .withMetadata(metadata)
    .remove(METADATA_COL_ATTR_KEY)
    .remove(FILE_SOURCE_METADATA_COL_ATTR_KEY)
    .remove(FileSourceConstantMetadataStructField.FILE_SOURCE_CONSTANT_METADATA_COL_ATTR_KEY)
    .remove(FileSourceGeneratedMetadataStructField.FILE_SOURCE_GENERATED_METADATA_COL_ATTR_KEY)
    .build()
}

/**
 * A [[StructField]] that describes a file source constant metadata struct field -- a member of the
 * `_metadata` struct whose value is constant for a whole file (e.g. file name). Values are usually
 * appended to the output and not generated per row.
 */
object FileSourceConstantMetadataStructField {

  val FILE_SOURCE_CONSTANT_METADATA_COL_ATTR_KEY = "__file_source_constant_metadata_col"

  /** Constructs a new metadata struct field of the given type; nullable by default */
  def apply(name: String, dataType: DataType, nullable: Boolean = true): StructField =
    StructField(name, FileSourceMetadataAttribute.validateType(dataType), nullable, metadata(name))

  def unapply(field: StructField): Option[StructField] =
    if (isValid(field.dataType, field.metadata)) Some(field) else None

  def metadata(name: String): Metadata = new MetadataBuilder()
    .withMetadata(FileSourceMetadataAttribute.metadata(name))
    .putBoolean(FILE_SOURCE_CONSTANT_METADATA_COL_ATTR_KEY, value = true)
    .build()

  def isValid(dataType: DataType, metadata: Metadata): Boolean = {
    FileSourceMetadataAttribute.isSupportedType(dataType) &&
    FileSourceMetadataAttribute.isValid(metadata) &&
    metadata.contains(FILE_SOURCE_CONSTANT_METADATA_COL_ATTR_KEY) &&
    metadata.getBoolean(FILE_SOURCE_CONSTANT_METADATA_COL_ATTR_KEY)
  }
}

/**
 * Matches the internal representation of a file source constant metadata attribute -- a member of
 * the `_metadata` struct whose value is constant for a whole file (e.g. file name). Values are
 * usually appended to the output and not generated per row.
 */
object FileSourceConstantMetadataAttribute {
  def unapply(attr: AttributeReference): Option[AttributeReference] = {
    val valid = FileSourceConstantMetadataStructField.isValid(attr.dataType, attr.metadata)
    if (valid) Some(attr) else None
  }
}

/**
 * A [[StructField]] that describes a file source generated metadata struct field -- a member of the
 * `metadata` struct that maps to some internal column the scanner returns.
 *
 * - apply() wil create a file source generated metadata struct field
 * - unapply() matches a file source generated metadata struct field, and returns its internal name
 */
object FileSourceGeneratedMetadataStructField {

  val FILE_SOURCE_GENERATED_METADATA_COL_ATTR_KEY = "__file_source_generated_metadata_col"

  /**
   * We keep generated metadata attributes nullability configurable here:
   * 1. Before passing to readers, we create generated metadata attributes as nullable;
   *    Because, for row_index, the readers do not consider the column required.
   *    row_index can be generated with null in the process by readers.
   * 2. When applying the projection, we restore the nullability specified here;
   *    For row_index, it is generated with nulls which are then replaced,
   *    so it will not be null in the returned output.
   *    See `FileSourceStrategy` for more information
   */
  def apply(
      name: String,
      internalName: String,
      dataType: DataType,
      nullable: Boolean = true): StructField = {
    val dt = FileSourceMetadataAttribute.validateType(dataType)
    StructField(name, dt, nullable, metadata(name, internalName))
  }

  def unapply(field: StructField): Option[(StructField, String)] =
    getInternalNameIfValid(field.dataType, field.metadata).map(field -> _)

  def metadata(name: String, internalName: String): Metadata = new MetadataBuilder()
    .withMetadata(FileSourceMetadataAttribute.metadata(name))
    .putString(FILE_SOURCE_GENERATED_METADATA_COL_ATTR_KEY, internalName)
    .build()

  def isValid(dataType: DataType, metadata: Metadata): Boolean = {
    FileSourceMetadataAttribute.isSupportedType(dataType) &&
    FileSourceMetadataAttribute.isValid(metadata) &&
    metadata.contains(FILE_SOURCE_GENERATED_METADATA_COL_ATTR_KEY)
  }

  def getInternalNameIfValid(dataType: DataType, metadata: Metadata): Option[String] = {
    if (isValid(dataType, metadata)) {
      Some(metadata.getString(FILE_SOURCE_GENERATED_METADATA_COL_ATTR_KEY))
    } else None
  }
}

/**
 * Matches the internal representation of a file source generated metadata attribute -- a member of
 * the `metadata` struct that maps to some internal column the scanner returns.
 */
object FileSourceGeneratedMetadataAttribute {
  def unapply(attr: AttributeReference): Option[(AttributeReference, String)] = {
    FileSourceGeneratedMetadataStructField
      .getInternalNameIfValid(attr.dataType, attr.metadata)
      .map(attr -> _)
  }
}
