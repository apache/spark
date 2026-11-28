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

package org.apache.spark.sql.catalyst.types.ops

import javax.annotation.Nullable

import org.apache.arrow.vector.ValueVector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.WalkedTypePath
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, MutableValue}
import org.apache.spark.sql.catalyst.types.{PhysicalArrayType, PhysicalBooleanType, PhysicalByteType, PhysicalDataType, PhysicalFloatType, PhysicalIntegerType, PhysicalMapType, PhysicalShortType, PhysicalStructType}
import org.apache.spark.sql.execution.arrow.ArrowFieldWriter
import org.apache.spark.sql.types.{ArrayType, DataType, FileType, MapType, StructType, TimestampLTZNanosType, TimestampNTZNanosType, TimeType}

/**
 * Server-side (catalyst) type operations for the Types Framework.
 *
 * This trait consolidates all server-side operations that a data type must implement to function in
 * the Spark SQL engine. Mandatory methods (physical type, literals, external conversion) must be
 * implemented by every type. Optional methods (serialization, Arrow writer) return Option and
 * default to None - types implement them as they expand their integration coverage.
 *
 * Extends [[TypeApiOps]] (the api-side trait) so that callers holding an `Option[TypeOps]` can
 * invoke client-side methods (`format`, `toSQLValue`, `getEncoder`, ...) directly via `.map`,
 * instead of an `isInstanceOf[TypeApiOps]` runtime narrowing. Concrete `*TypeOps` classes typically
 * extend `*TypeApiOps` to inherit api-side method implementations -- this trait makes that
 * relationship part of the type system rather than a runtime invariant.
 *
 * USAGE - integration points use TypeOps(dt) which returns Option[TypeOps]:
 * {{{
 * def getPhysicalType(dt: DataType): PhysicalDataType =
 *   TypeOps(dt).map(_.getPhysicalType).getOrElse {
 *     dt match {
 *       case DateType => PhysicalIntegerType
 *       // ... legacy types
 *     }
 *   }
 * }}}
 *
 * PHYSICAL TYPE CONTRACT: getPhysicalType must return the raw physical storage type (e.g.,
 * PhysicalLongType for Long-backed types, PhysicalStructType for struct-backed types). The
 * row infrastructure reaches a framework type through one of two routes:
 *   - Sites that dispatch on the physical type (SpecializedGettersReader,
 *     InternalRow.getAccessor, CodeGenerator.getValue and javaType, the field writers of
 *     InterpretedUnsafeProjection, ColumnarRow and ColumnarBatchRow) handle the raw type
 *     through getPhysicalType.
 *   - Sites that dispatch on the logical type (code generation for equality, comparison,
 *     copies, safe and unsafe projections, InternalRow.getWriter and InterpretedSafeProjection)
 *     handle a struct-, array- or map-backed type only through rowRepresentationType.
 * Logical conversion (raw value <-> external type) happens at boundaries via toScala/toCatalyst.
 *
 * IMPLEMENTATION - to add a new type to the framework:
 *   1. Create a case class extending the matching `*TypeApiOps` and mixing in `TypeOps`
 *      (e.g., `case class FooTypeOps(...) extends FooTypeApiOps(...) with TypeOps`)
 *   2. Implement getPhysicalType. getElementSize and rowRepresentationType are derived from
 *      it, so a struct-, array- or map-backed type reaches the logical-type sites without
 *      declaring its layout again.
 *   3. Register it in TypeOps.apply() below - single registration point
 *
 * Column vectors (WritableColumnVector, ConstantColumnVector) build one child vector per field
 * for a struct-backed framework type, but not yet child vectors for an array- or map-backed one.
 *
 * @see
 *   TimeTypeOps for a reference implementation
 * @since 4.2.0
 */
trait TypeOps extends TypeApiOps {

  // ==================== Physical Type Representation ====================

  /**
   * Returns the physical data type representation.
   *
   * Determines how values are stored in memory and accessed from InternalRow. Must return the
   * raw physical type (e.g., PhysicalLongType, PhysicalStructType) - not a wrapper. Sites that
   * dispatch on PhysicalDataType handle these types through it, on both the interpreted and
   * the code generation paths; see the trait documentation for the sites that use
   * [[rowRepresentationType]] instead.
   *
   * For struct-backed types, return PhysicalStructType(fields) where fields describes the
   * physical struct layout. The internal representation will be InternalRow, and
   * [[rowRepresentationType]] is StructType(fields).
   *
   * @return
   *   PhysicalDataType (e.g., PhysicalLongType for TimeType, PhysicalStructType for a
   *   struct-backed type)
   */
  def getPhysicalType: PhysicalDataType

  /**
   * Returns the Java class used for code generation.
   *
   * @return
   *   Java class (e.g., classOf[Long] for TimeType)
   */
  def getJavaClass: Class[_]

  /**
   * Returns the boxed Java class used in code generation where a nullable, reference-typed value
   * is required (e.g., array/map elements, external-value validation). Defaults to
   * [[getJavaClass]], which is correct for object-backed types; primitive-backed types must
   * override it to return the corresponding boxed class (e.g., classOf[java.lang.Long] for a
   * Long-backed type).
   *
   * @return
   *   boxed Java class
   */
  def getBoxedJavaClass: Class[_] = getJavaClass

  /**
   * Returns a MutableValue instance for use in SpecificInternalRow.
   *
   * @return
   *   MutableValue instance (e.g., MutableLong for TimeType)
   */
  def getMutableValue: MutableValue

  /**
   * Returns a writer function for setting values in an InternalRow.
   *
   * Not used by InternalRow.getWriter for a type with a [[rowRepresentationType]], which is
   * written as that representation.
   *
   * @param ordinal
   *   the column index to write to
   * @return
   *   writer function (InternalRow, Any) => Unit
   */
  def getRowWriter(ordinal: Int): (InternalRow, Any) => Unit

  /**
   * Returns the number of bytes this type occupies in the fixed-size region of an
   * [[org.apache.spark.sql.catalyst.expressions.UnsafeArrayData UnsafeArrayData]].
   *
   * Variable-length types (struct, array, map, binary, string) use 8 bytes because
   * the fixed region stores an offset+size pointer into the variable-length region.
   * Fixed-size primitive types use their natural size (1 for byte/boolean, 2 for short,
   * 4 for int/float, 8 for long/double).
   *
   * Consumed by [[org.apache.spark.sql.catalyst.expressions.InterpretedUnsafeProjection]]
   * when writing arrays or maps that contain framework type elements. Derived from
   * [[getPhysicalType]], matching the slot size that code generation uses.
   *
   * @return
   *   element size in bytes
   */
  def getElementSize: Int = getPhysicalType match {
    case PhysicalBooleanType | PhysicalByteType => 1
    case PhysicalShortType => 2
    case PhysicalIntegerType | PhysicalFloatType => 4
    case _ => 8
  }

  /**
   * Returns an equivalent built-in DataType for infrastructure dispatch.
   *
   * Framework types backed by complex physical storage (struct, array, map) return
   * the corresponding built-in DataType so that row infrastructure code (codegen,
   * projection, comparison, etc.) can handle them through existing code paths via
   * self-recursion - no per-physical-type matching needed at call sites.
   *
   * Every other type returns None, and the logical-type sites then see the type itself: a
   * primitive-backed type such as TimeType goes through their existing primitive handling, and
   * a type that is neither primitive- nor complex-backed, such as the nanosecond timestamp
   * types, needs handling of its own there, as those types have.
   *
   * This is NOT a type alias - the framework type retains its identity in schemas,
   * expressions, proto serialization, and error messages. This is solely a dispatch
   * hint for row infrastructure internals.
   *
   * Derived from [[getPhysicalType]]: PhysicalStructType(fields) is StructType(fields), and
   * PhysicalArrayType and PhysicalMapType are the matching ArrayType and MapType. The result is
   * always a built-in DataType, never another framework type, as some sites look the
   * representation up only once.
   *
   * @return Some(DataType) for struct-, array- and map-backed types, None for every other type
   */
  def rowRepresentationType: Option[DataType] = getPhysicalType match {
    case PhysicalStructType(fields) => Some(StructType(fields))
    case PhysicalArrayType(elementType, containsNull) => Some(ArrayType(elementType, containsNull))
    case PhysicalMapType(keyType, valueType, valueContainsNull) =>
      Some(MapType(keyType, valueType, valueContainsNull))
    case _ => None
  }

  // ==================== Literal Creation ====================

  /**
   * Returns the default literal value for this type.
   *
   * Used by Literal.default() for ALTER TABLE ADD COLUMN, optimizer, etc.
   *
   * @return
   *   Literal with the default value and correct type
   */
  def getDefaultLiteral: Literal

  /**
   * Returns the Java literal representation for code generation.
   *
   * @param v
   *   the internal value to represent
   * @return
   *   Java literal string (e.g., "37800000000000L")
   */
  def getJavaLiteral(v: Any): String

  // ==================== External Type Conversion ====================

  /**
   * Converts an external (Scala/Java) value to its internal Catalyst representation.
   *
   * Handles null checking and Option unwrapping automatically.
   *
   * @param maybeScalaValue
   *   the external value (may be null or Option)
   * @return
   *   the internal representation, or null if input was null/None
   */
  final def toCatalyst(@Nullable maybeScalaValue: Any): Any = {
    maybeScalaValue match {
      case null | None => null
      case opt: Some[_] => toCatalystImpl(opt.get)
      case other => toCatalystImpl(other)
    }
  }

  /**
   * Converts a non-null external value to its internal representation.
   *
   * @param scalaValue
   *   the external value (guaranteed non-null)
   * @return
   *   the internal Catalyst representation
   */
  def toCatalystImpl(scalaValue: Any): Any

  /**
   * Converts an internal Catalyst value to its external representation.
   *
   * @param catalystValue
   *   the internal value (may be null)
   * @return
   *   the external representation, or null if input was null
   */
  def toScala(@Nullable catalystValue: Any): Any

  /**
   * Extracts a value from an InternalRow and converts to external representation.
   *
   * @param row
   *   the InternalRow containing the value
   * @param column
   *   the column index
   * @return
   *   the external representation
   */
  def toScalaImpl(row: InternalRow, column: Int): Any

  /**
   * Extracts a value from an InternalRow with null checking.
   */
  final def toScala(row: InternalRow, column: Int): Any = {
    if (row.isNullAt(column)) null else toScalaImpl(row, column)
  }

  // ==================== Serialization (optional) ====================

  /** Creates a serializer expression (external -> internal). */
  def createSerializer(input: Expression): Option[Expression] = None

  /**
   * Creates a deserializer expression (internal -> external).
   * Most types override this simple version.
   */
  def createDeserializer(path: Expression): Option[Expression] = None

  /** Creates a deserializer with full context. Default delegates to the simple version. */
  def createDeserializer(
      path: Expression,
      walkedTypePath: WalkedTypePath,
      isTopLevel: Boolean): Option[Expression] = createDeserializer(path)

  // ==================== Arrow Writer (optional) ====================

  /** Creates an ArrowFieldWriter for this type. */
  def createArrowFieldWriter(vector: ValueVector): Option[ArrowFieldWriter] = None
}

/**
 * Factory object for creating TypeOps instances.
 *
 * Returns Option to serve as both lookup and existence check - callers use getOrElse to fall
 * through to legacy handling for types the framework does not manage.
 *
 * Uses pattern matching (not Set enumeration) to support parameterized types like
 * TimeType(precision) or DecimalType(precision, scale).
 */
object TypeOps {

  /**
   * Returns a TypeOps instance for the given DataType, if supported by the framework.
   *
   * Returns None if the type is not supported. This is the single registration point for all
   * server-side type operations.
   *
   * @param dt
   *   the DataType to get operations for
   * @return
   *   Some(TypeOps) if supported, None otherwise
   */
  def apply(dt: DataType): Option[TypeOps] = dt match {
    case tt: TimeType => Some(TimeTypeOps(tt))
    case t: TimestampNTZNanosType => Some(TimestampNTZNanosTypeOps(t))
    case t: TimestampLTZNanosType => Some(TimestampLTZNanosTypeOps(t))
    case ft: FileType => Some(FileTypeOps(ft))
    // Add new types here - single registration point
    case ft: TestableFrameworkType => Some(ft.createTypeOps())
    case _ => None
  }

  /**
   * Returns the row representation of `dt` (see [[TypeOps.rowRepresentationType]]), or None
   * when `dt` is not a framework type or has none.
   */
  def rowRepresentation(dt: DataType): Option[DataType] =
    apply(dt).flatMap(_.rowRepresentationType)

  /**
   * Returns the row representation of `dt` (see [[TypeOps.rowRepresentationType]]), or `dt`
   * itself when it has none. For callers in Java, such as the column vectors.
   */
  def rowRepresentationOrSelf(dt: DataType): DataType = rowRepresentation(dt).getOrElse(dt)
}

/**
 * Base class for test-only DataTypes that participate in the Types Framework.
 *
 * Test DataTypes extend this class to register themselves with TypeOps.apply()
 * without requiring mutable global state. Each test type carries its own TypeOps
 * factory, making this safe for concurrent test execution (SBT parallel tests).
 *
 * This is a class rather than a trait so that its arm in TypeOps.apply() is a class check
 * like the others: TypeOps.apply() runs per value on some interpreted paths, and a failing
 * interface check can cost more than a failing class check.
 *
 * Production types (TimeType, the nanosecond timestamp types, etc.) do NOT use this class -
 * they are registered directly in the TypeOps.apply() match.
 */
private[ops] abstract class TestableFrameworkType extends DataType {
  def createTypeOps(): TypeOps
}
