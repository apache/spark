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

package org.apache.spark.sql.catalyst.encoders

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.catalyst.{DeserializerBuildHelper, InternalRow, JavaTypeInference, ScalaReflection, SerializerBuildHelper}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, GetColumnByOrdinal, SimpleAnalyzer, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder.{Deserializer, Serializer}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.{AssertNotNull, InitializeJavaBean, NewInstance}
import org.apache.spark.sql.catalyst.optimizer.{ReassignLambdaVariableID, SimplifyCasts}
import org.apache.spark.sql.catalyst.plans.logical.{CatalystSerde, DeserializeToObject, LeafNode, LocalRelation}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{DataType, ObjectType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * A factory for constructing encoders that convert objects and primitives to and from the
 * internal row format using catalyst expressions and code generation.  By default, the
 * expressions used to retrieve values from an input row when producing an object will be created as
 * follows:
 *  - Classes will have their sub fields extracted by name using [[UnresolvedAttribute]] expressions
 *    and [[UnresolvedExtractValue]] expressions.
 *  - Tuples will have their subfields extracted by position using [[BoundReference]] expressions.
 *  - Primitives will have their values extracted from the first ordinal with a schema that defaults
 *    to the name `value`.
 */
object ExpressionEncoder {

  def apply[T : TypeTag](): ExpressionEncoder[T] = {
    apply(ScalaReflection.encoderFor[T])
  }

  def apply[T](enc: AgnosticEncoder[T]): ExpressionEncoder[T] = {
    new ExpressionEncoder[T](
      enc,
      SerializerBuildHelper.createSerializer(enc),
      DeserializerBuildHelper.createDeserializer(enc))
  }

  def apply(schema: StructType): ExpressionEncoder[Row] = apply(schema, lenient = false)

  def apply(schema: StructType, lenient: Boolean): ExpressionEncoder[Row] = {
    apply(RowEncoder.encoderFor(schema, lenient))
  }

  // TODO: improve error message for java bean encoder.
  def javaBean[T](beanClass: Class[T]): ExpressionEncoder[T] = {
     apply(JavaTypeInference.encoderFor(beanClass))
  }

  private val anyObjectType = ObjectType(classOf[Any])

  /**
   * Function that deserializes an [[InternalRow]] into an object of type `T`. This class is not
   * thread-safe.
   */
  class Deserializer[T](val expressions: Seq[Expression])
    extends (InternalRow => T) with Serializable {
    @transient
    private[this] var constructProjection: Projection = _

    override def apply(row: InternalRow): T = try {
      if (constructProjection == null) {
        constructProjection = SafeProjection.create(expressions)
      }
      constructProjection(row).get(0, anyObjectType).asInstanceOf[T]
    } catch {
      case e: SparkRuntimeException if e.getErrorClass == "NOT_NULL_ASSERT_VIOLATION" =>
        throw e
      case e: Exception =>
        throw QueryExecutionErrors.expressionDecodingError(e, expressions)
    }
  }

  /**
   * Function that serializes an object of type `T` to an [[InternalRow]]. This class is not
   * thread-safe. Note that multiple calls to `apply(..)` return the same actual [[InternalRow]]
   * object.  Thus, the caller should copy the result before making another call if required.
   */
  class Serializer[T](private val expressions: Seq[Expression])
    extends (T => InternalRow) with Serializable {
    @transient
    private[this] var inputRow: GenericInternalRow = _

    @transient
    private[this] var extractProjection: UnsafeProjection = _

    override def apply(t: T): InternalRow = try {
      if (extractProjection == null) {
        inputRow = new GenericInternalRow(1)
        extractProjection = UnsafeProjection.create(expressions)
      }
      inputRow(0) = t
      extractProjection(inputRow)
    } catch {
      case e: SparkRuntimeException if e.getErrorClass == "NOT_NULL_ASSERT_VIOLATION" =>
        throw e
      case e: Exception =>
        throw QueryExecutionErrors.expressionEncodingError(e, expressions)
    }
  }
}

/**
 * A generic encoder for JVM objects that uses Catalyst Expressions for a `serializer`
 * and a `deserializer`.
 *
 * @param encoder the `AgnosticEncoder` for type `T`.
 * @param objSerializer An expression that can be used to encode a raw object to corresponding
 *                   Spark SQL representation that can be a primitive column, array, map or a
 *                   struct. This represents how Spark SQL generally serializes an object of
 *                   type `T`.
 * @param objDeserializer An expression that will construct an object given a Spark SQL
 *                        representation. This represents how Spark SQL generally deserializes
 *                        a serialized value in Spark SQL representation back to an object of
 *                        type `T`.
 */
case class ExpressionEncoder[T](
    encoder: AgnosticEncoder[T],
    objSerializer: Expression,
    objDeserializer: Expression)
  extends Encoder[T]
  with ToAgnosticEncoder[T] {

  override def clsTag: ClassTag[T] = encoder.clsTag

  /**
   * A sequence of expressions, one for each top-level field that can be used to
   * extract the values from a raw object into an [[InternalRow]]:
   * 1. If `serializer` encodes a raw object to a struct, strip the outer If-IsNull and get
   *    the `CreateNamedStruct`.
   * 2. For other cases, wrap the single serializer with `CreateNamedStruct`.
   */
  val serializer: Seq[NamedExpression] = {
    val clsName = Utils.getSimpleName(clsTag.runtimeClass)

    if (isSerializedAsStructForTopLevel) {
      val nullSafeSerializer = objSerializer.transformUp {
        case r: BoundReference =>
          // For input object of Product type, we can't encode it to row if it's null, as Spark SQL
          // doesn't allow top-level row to be null, only its columns can be null.
          AssertNotNull(r, Seq("top level Product or row object"))
      }
      nullSafeSerializer match {
        case If(_: IsNull, _, s: CreateNamedStruct) => s
        case s: CreateNamedStruct => s
        case _ =>
          throw QueryExecutionErrors.classHasUnexpectedSerializerError(clsName, objSerializer)
      }
    } else {
      // For other input objects like primitive, array, map, etc., we construct a struct to wrap
      // the serializer which is a column of an row.
      //
      // Note: Because Spark SQL doesn't allow top-level row to be null, to encode
      // top-level Option[Product] type, we make it as a top-level struct column.
      CreateNamedStruct(Literal("value") :: objSerializer :: Nil)
    }
  }.flatten

  /**
   * Returns an expression that can be used to deserialize an input row to an object of type `T`
   * with a compatible schema. Fields of the row will be extracted using `UnresolvedAttribute`.
   * of the same name as the constructor arguments.
   *
   * For complex objects that are encoded to structs, Fields of the struct will be extracted using
   * `GetColumnByOrdinal` with corresponding ordinal.
   */
  val deserializer: Expression = {
    if (isSerializedAsStructForTopLevel) {
      // We serialized this kind of objects to root-level row. The input of general deserializer
      // is a `GetColumnByOrdinal(0)` expression to extract first column of a row. We need to
      // transform attributes accessors.
      objDeserializer.transform {
        case UnresolvedExtractValue(GetColumnByOrdinal(0, _),
            Literal(part: UTF8String, StringType)) =>
          UnresolvedAttribute.quoted(part.toString)
        case GetStructField(GetColumnByOrdinal(0, dt), ordinal, _) =>
          GetColumnByOrdinal(ordinal, dt)
        case If(IsNull(GetColumnByOrdinal(0, _)), _, n: NewInstance) => n
        case If(IsNull(GetColumnByOrdinal(0, _)), _, i: InitializeJavaBean) => i
      }
    } else {
      // For other input objects like primitive, array, map, etc., we deserialize the first column
      // of a row to the object.
      objDeserializer
    }
  }

  // The schema after converting `T` to a Spark SQL row. This schema is dependent on the given
  // serializer.
  val schema: StructType = StructType(serializer.map { s =>
    StructField(s.name, DataType.udtToSqlType(s.dataType), s.nullable)
  })

  /**
   * Returns true if the type `T` is serialized as a struct by `objSerializer`.
   */
  def isSerializedAsStruct: Boolean = objSerializer.dataType.isInstanceOf[StructType]

  /**
   * If the type `T` is serialized as a struct, when it is encoded to a Spark SQL row, fields in
   * the struct are naturally mapped to top-level columns in a row. In other words, the serialized
   * struct is flattened to row. But in case of the `T` is also an `Option` type, it can't be
   * flattened to top-level row, because in Spark SQL top-level row can't be null. This method
   * returns true if `T` is serialized as struct and is not `Option` type.
   */
  def isSerializedAsStructForTopLevel: Boolean = {
    isSerializedAsStruct && !classOf[Option[_]].isAssignableFrom(clsTag.runtimeClass)
  }

  // serializer expressions are used to encode an object to a row, while the object is usually an
  // intermediate value produced inside an operator, not from the output of the child operator. This
  // is quite different from normal expressions, and `AttributeReference` doesn't work here
  // (intermediate value is not an attribute). We assume that all serializer expressions use the
  // same `BoundReference` to refer to the object, and throw exception if they don't.
  assert(serializer.forall(_.references.isEmpty), "serializer cannot reference any attributes.")
  assert(serializer.flatMap { ser =>
    val boundRefs = ser.collect { case b: BoundReference => b }
    assert(boundRefs.nonEmpty || isEmptyStruct(ser),
      "each serializer expression should contain at least one `BoundReference` or it " +
      "should be an empty struct. This is required to ensure that there is a reference point " +
      "for the serialized object or that the serialized object is intentionally left empty."
    )
    boundRefs
  }.distinct.length <= 1, "all serializer expressions must use the same BoundReference.")

  private def isEmptyStruct(expr: NamedExpression): Boolean = expr.dataType match {
    case struct: StructType => struct.isEmpty
    case _ => false
  }

  /**
   * Returns a new copy of this encoder, where the `deserializer` is resolved and bound to the
   * given schema.
   *
   * Note that, ideally encoder is used as a container of serde expressions, the resolution and
   * binding stuff should happen inside query framework.  However, in some cases we need to
   * use encoder as a function to do serialization directly(e.g. Dataset.collect), then we can use
   * this method to do resolution and binding outside of query framework.
   */
  def resolveAndBind(
      attrs: Seq[Attribute] = DataTypeUtils.toAttributes(schema),
      analyzer: Analyzer = SimpleAnalyzer): ExpressionEncoder[T] = {
    val dummyPlan = CatalystSerde.deserialize(LocalRelation(attrs))(this)
    val analyzedPlan = analyzer.execute(dummyPlan)
    analyzer.checkAnalysis(analyzedPlan)
    val resolved = SimplifyCasts(analyzedPlan).asInstanceOf[DeserializeToObject].deserializer
    val bound = BindReferences.bindReference(resolved, attrs)
    copy(objDeserializer = bound)
  }

  @transient
  private lazy val optimizedDeserializer: Seq[Expression] = {
    // When using `ExpressionEncoder` directly, we will skip the normal query processing steps
    // (analyzer, optimizer, etc.). Here we apply the ReassignLambdaVariableID rule, as it's
    // important to codegen performance.
    val optimizedPlan = ReassignLambdaVariableID.apply(DummyExpressionHolder(Seq(deserializer)))
    optimizedPlan.asInstanceOf[DummyExpressionHolder].exprs
  }

  @transient
  private lazy val optimizedSerializer = {
    // When using `ExpressionEncoder` directly, we will skip the normal query processing steps
    // (analyzer, optimizer, etc.). Here we apply the ReassignLambdaVariableID rule, as it's
    // important to codegen performance.
    val optimizedPlan = ReassignLambdaVariableID.apply(DummyExpressionHolder(serializer))
    optimizedPlan.asInstanceOf[DummyExpressionHolder].exprs
  }

  /**
   * Returns a new set (with unique ids) of [[NamedExpression]] that represent the serialized form
   * of this object.
   */
  def namedExpressions: Seq[NamedExpression] = schema.map(_.name).zip(serializer).map {
    case (_, ne: NamedExpression) => ne.newInstance()
    case (name, e) => Alias(e, name)()
  }

  /**
   * Create a serializer that can convert an object of type `T` to a Spark SQL Row.
   *
   * Note that the returned [[Serializer]] is not thread safe. Multiple calls to
   * `serializer.apply(..)` are allowed to return the same actual [[InternalRow]] object.  Thus,
   *  the caller should copy the result before making another call if required.
   */
  def createSerializer(): Serializer[T] = new Serializer[T](optimizedSerializer)

  /**
   * Create a deserializer that can convert a Spark SQL Row into an object of type `T`.
   *
   * Note that you must `resolveAndBind` an encoder to a specific schema before you can create a
   * deserializer.
   */
  def createDeserializer(): Deserializer[T] = new Deserializer[T](optimizedDeserializer)

  /**
   * The process of resolution to a given schema throws away information about where a given field
   * is being bound by ordinal instead of by name.  This method checks to make sure this process
   * has not been done already in places where we plan to do later composition of encoders.
   */
  def assertUnresolved(): Unit = {
    (deserializer +: serializer).foreach(_.foreach {
      case a: AttributeReference if a.name != "loopVar" =>
        throw QueryExecutionErrors.notExpectedUnresolvedEncoderError(a)
      case _ =>
    })
  }

  protected val attrs = serializer.flatMap(_.collect {
    case _: UnresolvedAttribute => ""
    case a: Attribute => s"#${a.exprId}"
    case b: BoundReference => s"[${b.ordinal}]"
  })

  protected val schemaString =
    schema
      .zip(attrs)
      .map { case(f, a) => s"${f.name}$a: ${f.dataType.simpleString}"}.mkString(", ")

  override def toString: String = s"class[$schemaString]"
}

// A dummy logical plan that can hold expressions and go through optimizer rules.
case class DummyExpressionHolder(exprs: Seq[Expression]) extends LeafNode {
  override lazy val resolved = true
  override def output: Seq[Attribute] = Nil
}
