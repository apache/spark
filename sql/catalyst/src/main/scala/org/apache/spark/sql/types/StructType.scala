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

package org.apache.spark.sql.types

import scala.collection.mutable.ArrayBuffer

import org.json4s.JsonDSL._

import org.apache.spark.SparkException
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, InterpretedOrdering}


/**
 * :: DeveloperApi ::
 * A [[StructType]] object can be constructed by
 * {{{
 * StructType(fields: Seq[StructField])
 * }}}
 * For a [[StructType]] object, one or multiple [[StructField]]s can be extracted by names.
 * If multiple [[StructField]]s are extracted, a [[StructType]] object will be returned.
 * If a provided name does not have a matching field, it will be ignored. For the case
 * of extracting a single StructField, a `null` will be returned.
 * Example:
 * {{{
 * import org.apache.spark.sql._
 *
 * val struct =
 *   StructType(
 *     StructField("a", IntegerType, true) ::
 *     StructField("b", LongType, false) ::
 *     StructField("c", BooleanType, false) :: Nil)
 *
 * // Extract a single StructField.
 * val singleField = struct("b")
 * // singleField: StructField = StructField(b,LongType,false)
 *
 * // This struct does not have a field called "d". null will be returned.
 * val nonExisting = struct("d")
 * // nonExisting: StructField = null
 *
 * // Extract multiple StructFields. Field names are provided in a set.
 * // A StructType object will be returned.
 * val twoFields = struct(Set("b", "c"))
 * // twoFields: StructType =
 * //   StructType(List(StructField(b,LongType,false), StructField(c,BooleanType,false)))
 *
 * // Any names without matching fields will be ignored.
 * // For the case shown below, "d" will be ignored and
 * // it is treated as struct(Set("b", "c")).
 * val ignoreNonExisting = struct(Set("b", "c", "d"))
 * // ignoreNonExisting: StructType =
 * //   StructType(List(StructField(b,LongType,false), StructField(c,BooleanType,false)))
 * }}}
 *
 * A [[org.apache.spark.sql.Row]] object is used as a value of the StructType.
 * Example:
 * {{{
 * import org.apache.spark.sql._
 *
 * val innerStruct =
 *   StructType(
 *     StructField("f1", IntegerType, true) ::
 *     StructField("f2", LongType, false) ::
 *     StructField("f3", BooleanType, false) :: Nil)
 *
 * val struct = StructType(
 *   StructField("a", innerStruct, true) :: Nil)
 *
 * // Create a Row with the schema defined by struct
 * val row = Row(Row(1, 2, true))
 * // row: Row = [[1,2,true]]
 * }}}
 */
@DeveloperApi
case class StructType(fields: Array[StructField]) extends DataType with Seq[StructField] {

  /** No-arg constructor for kryo. */
  def this() = this(Array.empty[StructField])

  /** Returns all field names in an array. */
  def fieldNames: Array[String] = fields.map(_.name)

  private lazy val fieldNamesSet: Set[String] = fieldNames.toSet
  private lazy val nameToField: Map[String, StructField] = fields.map(f => f.name -> f).toMap
  private lazy val nameToIndex: Map[String, Int] = fieldNames.zipWithIndex.toMap

  /**
   * Creates a new [[StructType]] by adding a new field.
   * {{{
   * val struct = (new StructType)
   *   .add(StructField("a", IntegerType, true))
   *   .add(StructField("b", LongType, false))
   *   .add(StructField("c", StringType, true))
   *}}}
   */
  def add(field: StructField): StructType = {
    StructType(fields :+ field)
  }

  /**
   * Creates a new [[StructType]] by adding a new nullable field with no metadata.
   *
   * val struct = (new StructType)
   *   .add("a", IntegerType)
   *   .add("b", LongType)
   *   .add("c", StringType)
   */
  def add(name: String, dataType: DataType): StructType = {
    StructType(fields :+ new StructField(name, dataType, nullable = true, Metadata.empty))
  }

  /**
   * Creates a new [[StructType]] by adding a new field with no metadata.
   *
   * val struct = (new StructType)
   *   .add("a", IntegerType, true)
   *   .add("b", LongType, false)
   *   .add("c", StringType, true)
   */
  def add(name: String, dataType: DataType, nullable: Boolean): StructType = {
    StructType(fields :+ new StructField(name, dataType, nullable, Metadata.empty))
  }

  /**
   * Creates a new [[StructType]] by adding a new field and specifying metadata.
   * {{{
   * val struct = (new StructType)
   *   .add("a", IntegerType, true, Metadata.empty)
   *   .add("b", LongType, false, Metadata.empty)
   *   .add("c", StringType, true, Metadata.empty)
   * }}}
   */
  def add(
      name: String,
      dataType: DataType,
      nullable: Boolean,
      metadata: Metadata): StructType = {
    StructType(fields :+ new StructField(name, dataType, nullable, metadata))
  }

  /**
   * Creates a new [[StructType]] by adding a new nullable field with no metadata where the
   * dataType is specified as a String.
   *
   * {{{
   * val struct = (new StructType)
   *   .add("a", "int")
   *   .add("b", "long")
   *   .add("c", "string")
   * }}}
   */
  def add(name: String, dataType: String): StructType = {
    add(name, DataTypeParser.parse(dataType), nullable = true, Metadata.empty)
  }

  /**
   * Creates a new [[StructType]] by adding a new field with no metadata where the
   * dataType is specified as a String.
   *
   * {{{
   * val struct = (new StructType)
   *   .add("a", "int", true)
   *   .add("b", "long", false)
   *   .add("c", "string", true)
   * }}}
   */
  def add(name: String, dataType: String, nullable: Boolean): StructType = {
    add(name, DataTypeParser.parse(dataType), nullable, Metadata.empty)
  }

  /**
   * Creates a new [[StructType]] by adding a new field and specifying metadata where the
   * dataType is specified as a String.
   * {{{
   * val struct = (new StructType)
   *   .add("a", "int", true, Metadata.empty)
   *   .add("b", "long", false, Metadata.empty)
   *   .add("c", "string", true, Metadata.empty)
   * }}}
   */
  def add(
      name: String,
      dataType: String,
      nullable: Boolean,
      metadata: Metadata): StructType = {
    add(name, DataTypeParser.parse(dataType), nullable, metadata)
  }

  /**
   * Extracts a [[StructField]] of the given name. If the [[StructType]] object does not
   * have a name matching the given name, `null` will be returned.
   */
  def apply(name: String): StructField = {
    nameToField.getOrElse(name,
      throw new IllegalArgumentException(s"""Field "$name" does not exist."""))
  }

  /**
   * Returns a [[StructType]] containing [[StructField]]s of the given names, preserving the
   * original order of fields. Those names which do not have matching fields will be ignored.
   */
  def apply(names: Set[String]): StructType = {
    val nonExistFields = names -- fieldNamesSet
    if (nonExistFields.nonEmpty) {
      throw new IllegalArgumentException(
        s"Field ${nonExistFields.mkString(",")} does not exist.")
    }
    // Preserve the original order of fields.
    StructType(fields.filter(f => names.contains(f.name)))
  }

  /**
   * Returns index of a given field
   */
  def fieldIndex(name: String): Int = {
    nameToIndex.getOrElse(name,
      throw new IllegalArgumentException(s"""Field "$name" does not exist."""))
  }

  private[sql] def getFieldIndex(name: String): Option[Int] = {
    nameToIndex.get(name)
  }

  protected[sql] def toAttributes: Seq[AttributeReference] =
    map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())

  def treeString: String = {
    val builder = new StringBuilder
    builder.append("root\n")
    val prefix = " |"
    fields.foreach(field => field.buildFormattedString(prefix, builder))

    builder.toString()
  }

  // scalastyle:off println
  def printTreeString(): Unit = println(treeString)
  // scalastyle:on println

  private[sql] def buildFormattedString(prefix: String, builder: StringBuilder): Unit = {
    fields.foreach(field => field.buildFormattedString(prefix, builder))
  }

  override private[sql] def jsonValue =
    ("type" -> typeName) ~
      ("fields" -> map(_.jsonValue))

  override def apply(fieldIndex: Int): StructField = fields(fieldIndex)

  override def length: Int = fields.length

  override def iterator: Iterator[StructField] = fields.iterator

  /**
   * The default size of a value of the StructType is the total default sizes of all field types.
   */
  override def defaultSize: Int = fields.map(_.dataType.defaultSize).sum

  override def simpleString: String = {
    val fieldTypes = fields.map(field => s"${field.name}:${field.dataType.simpleString}")
    s"struct<${fieldTypes.mkString(",")}>"
  }

  /**
   * Merges with another schema (`StructType`).  For a struct field A from `this` and a struct field
   * B from `that`,
   *
   * 1. If A and B have the same name and data type, they are merged to a field C with the same name
   *    and data type.  C is nullable if and only if either A or B is nullable.
   * 2. If A doesn't exist in `that`, it's included in the result schema.
   * 3. If B doesn't exist in `this`, it's also included in the result schema.
   * 4. Otherwise, `this` and `that` are considered as conflicting schemas and an exception would be
   *    thrown.
   */
  private[sql] def merge(that: StructType): StructType =
    StructType.merge(this, that).asInstanceOf[StructType]

  override private[spark] def asNullable: StructType = {
    val newFields = fields.map {
      case StructField(name, dataType, nullable, metadata) =>
        StructField(name, dataType.asNullable, nullable = true, metadata)
    }

    StructType(newFields)
  }

  override private[spark] def existsRecursively(f: (DataType) => Boolean): Boolean = {
    f(this) || fields.exists(field => field.dataType.existsRecursively(f))
  }

  @transient
  private[sql] lazy val interpretedOrdering =
    InterpretedOrdering.forSchema(this.fields.map(_.dataType))
}

object StructType extends AbstractDataType {

  override private[sql] def defaultConcreteType: DataType = new StructType

  override private[sql] def acceptsType(other: DataType): Boolean = {
    other.isInstanceOf[StructType]
  }

  override private[sql] def simpleString: String = "struct"

  private[sql] def fromString(raw: String): StructType = DataType.fromString(raw) match {
    case t: StructType => t
    case _ => throw new RuntimeException(s"Failed parsing StructType: $raw")
  }

  def apply(fields: Seq[StructField]): StructType = StructType(fields.toArray)

  def apply(fields: java.util.List[StructField]): StructType = {
    import scala.collection.JavaConverters._
    StructType(fields.asScala)
  }

  protected[sql] def fromAttributes(attributes: Seq[Attribute]): StructType =
    StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

  private[sql] def merge(left: DataType, right: DataType): DataType =
    (left, right) match {
      case (ArrayType(leftElementType, leftContainsNull),
      ArrayType(rightElementType, rightContainsNull)) =>
        ArrayType(
          merge(leftElementType, rightElementType),
          leftContainsNull || rightContainsNull)

      case (MapType(leftKeyType, leftValueType, leftContainsNull),
      MapType(rightKeyType, rightValueType, rightContainsNull)) =>
        MapType(
          merge(leftKeyType, rightKeyType),
          merge(leftValueType, rightValueType),
          leftContainsNull || rightContainsNull)

      case (StructType(leftFields), StructType(rightFields)) =>
        val newFields = ArrayBuffer.empty[StructField]

        val rightMapped = fieldsMap(rightFields)
        leftFields.foreach {
          case leftField @ StructField(leftName, leftType, leftNullable, _) =>
            rightMapped.get(leftName)
              .map { case rightField @ StructField(_, rightType, rightNullable, _) =>
              leftField.copy(
                dataType = merge(leftType, rightType),
                nullable = leftNullable || rightNullable)
            }
              .orElse(Some(leftField))
              .foreach(newFields += _)
        }

        val leftMapped = fieldsMap(leftFields)
        rightFields
          .filterNot(f => leftMapped.get(f.name).nonEmpty)
          .foreach(newFields += _)

        StructType(newFields)

      case (DecimalType.Fixed(leftPrecision, leftScale),
        DecimalType.Fixed(rightPrecision, rightScale)) =>
        if ((leftPrecision == rightPrecision) && (leftScale == rightScale)) {
          DecimalType(leftPrecision, leftScale)
        } else if ((leftPrecision != rightPrecision) && (leftScale != rightScale)) {
          throw new SparkException("Failed to merge Decimal Tpes with incompatible " +
            s"precision $leftPrecision and $rightPrecision & scale $leftScale and $rightScale")
        } else if (leftPrecision != rightPrecision) {
          throw new SparkException("Failed to merge Decimal Tpes with incompatible " +
            s"precision $leftPrecision and $rightPrecision")
        } else {
          throw new SparkException("Failed to merge Decimal Tpes with incompatible " +
            s"scala $leftScale and $rightScale")
        }

      case (leftUdt: UserDefinedType[_], rightUdt: UserDefinedType[_])
        if leftUdt.userClass == rightUdt.userClass => leftUdt

      case (leftType, rightType) if leftType == rightType =>
        leftType

      case _ =>
        throw new SparkException(s"Failed to merge incompatible data types $left and $right")
    }

  private[sql] def fieldsMap(fields: Array[StructField]): Map[String, StructField] = {
    import scala.collection.breakOut
    fields.map(s => (s.name, s))(breakOut)
  }
}
