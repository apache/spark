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

import java.util.concurrent.ConcurrentMap

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{typeTag, TypeTag}

import org.apache.spark.sql.{AnalysisException, Encoder}
import org.apache.spark.sql.catalyst.{InternalRow, JavaTypeInference, ScalaReflection}
import org.apache.spark.sql.catalyst.analysis.{SimpleAnalyzer, UnresolvedAttribute, UnresolvedDeserializer, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateSafeProjection, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, NewInstance}
import org.apache.spark.sql.catalyst.optimizer.SimplifyCasts
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.types.{ObjectType, StructField, StructType}
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
    // We convert the not-serializable TypeTag into StructType and ClassTag.
    val mirror = typeTag[T].mirror
    val tpe = typeTag[T].tpe
    val cls = mirror.runtimeClass(tpe)
    val flat = !ScalaReflection.definedByConstructorParams(tpe)

    val inputObject = BoundReference(0, ScalaReflection.dataTypeFor[T], nullable = false)
    val serializer = ScalaReflection.serializerFor[T](inputObject)
    val deserializer = ScalaReflection.deserializerFor[T]

    val schema = ScalaReflection.schemaFor[T] match {
      case ScalaReflection.Schema(s: StructType, _) => s
      case ScalaReflection.Schema(dt, nullable) => new StructType().add("value", dt, nullable)
    }

    new ExpressionEncoder[T](
      schema,
      flat,
      serializer.flatten,
      deserializer,
      ClassTag[T](cls))
  }

  // TODO: improve error message for java bean encoder.
  def javaBean[T](beanClass: Class[T]): ExpressionEncoder[T] = {
    val schema = JavaTypeInference.inferDataType(beanClass)._1
    assert(schema.isInstanceOf[StructType])

    val serializer = JavaTypeInference.serializerFor(beanClass)
    val deserializer = JavaTypeInference.deserializerFor(beanClass)

    new ExpressionEncoder[T](
      schema.asInstanceOf[StructType],
      flat = false,
      serializer.flatten,
      deserializer,
      ClassTag[T](beanClass))
  }

  /**
   * Given a set of N encoders, constructs a new encoder that produce objects as items in an
   * N-tuple.  Note that these encoders should be unresolved so that information about
   * name/positional binding is preserved.
   */
  def tuple(encoders: Seq[ExpressionEncoder[_]]): ExpressionEncoder[_] = {
    encoders.foreach(_.assertUnresolved())

    val schema = StructType(encoders.zipWithIndex.map {
      case (e, i) =>
        val (dataType, nullable) = if (e.flat) {
          e.schema.head.dataType -> e.schema.head.nullable
        } else {
          e.schema -> true
        }
        StructField(s"_${i + 1}", dataType, nullable)
    })

    val cls = Utils.getContextOrSparkClassLoader.loadClass(s"scala.Tuple${encoders.size}")

    val serializer = encoders.map {
      case e if e.flat => e.serializer.head
      case other => CreateStruct(other.serializer)
    }.zipWithIndex.map { case (expr, index) =>
      expr.transformUp {
        case BoundReference(0, t, _) =>
          Invoke(
            BoundReference(0, ObjectType(cls), nullable = true),
            s"_${index + 1}",
            t)
      }
    }

    val childrenDeserializers = encoders.zipWithIndex.map { case (enc, index) =>
      if (enc.flat) {
        enc.deserializer.transform {
          case b: BoundReference => b.copy(ordinal = index)
        }
      } else {
        val input = BoundReference(index, enc.schema, nullable = true)
        enc.deserializer.transformUp {
          case UnresolvedAttribute(nameParts) =>
            assert(nameParts.length == 1)
            UnresolvedExtractValue(input, Literal(nameParts.head))
          case BoundReference(ordinal, dt, _) => GetStructField(input, ordinal)
        }
      }
    }

    val deserializer =
      NewInstance(cls, childrenDeserializers, ObjectType(cls), propagateNull = false)

    new ExpressionEncoder[Any](
      schema,
      flat = false,
      serializer,
      deserializer,
      ClassTag(cls))
  }

  def tuple[T1, T2](
      e1: ExpressionEncoder[T1],
      e2: ExpressionEncoder[T2]): ExpressionEncoder[(T1, T2)] =
    tuple(Seq(e1, e2)).asInstanceOf[ExpressionEncoder[(T1, T2)]]

  def tuple[T1, T2, T3](
      e1: ExpressionEncoder[T1],
      e2: ExpressionEncoder[T2],
      e3: ExpressionEncoder[T3]): ExpressionEncoder[(T1, T2, T3)] =
    tuple(Seq(e1, e2, e3)).asInstanceOf[ExpressionEncoder[(T1, T2, T3)]]

  def tuple[T1, T2, T3, T4](
      e1: ExpressionEncoder[T1],
      e2: ExpressionEncoder[T2],
      e3: ExpressionEncoder[T3],
      e4: ExpressionEncoder[T4]): ExpressionEncoder[(T1, T2, T3, T4)] =
    tuple(Seq(e1, e2, e3, e4)).asInstanceOf[ExpressionEncoder[(T1, T2, T3, T4)]]

  def tuple[T1, T2, T3, T4, T5](
      e1: ExpressionEncoder[T1],
      e2: ExpressionEncoder[T2],
      e3: ExpressionEncoder[T3],
      e4: ExpressionEncoder[T4],
      e5: ExpressionEncoder[T5]): ExpressionEncoder[(T1, T2, T3, T4, T5)] =
    tuple(Seq(e1, e2, e3, e4, e5)).asInstanceOf[ExpressionEncoder[(T1, T2, T3, T4, T5)]]
}

/**
 * A generic encoder for JVM objects.
 *
 * @param schema The schema after converting `T` to a Spark SQL row.
 * @param serializer A set of expressions, one for each top-level field that can be used to
 *                   extract the values from a raw object into an [[InternalRow]].
 * @param deserializer An expression that will construct an object given an [[InternalRow]].
 * @param clsTag A classtag for `T`.
 */
case class ExpressionEncoder[T](
    schema: StructType,
    flat: Boolean,
    serializer: Seq[Expression],
    deserializer: Expression,
    clsTag: ClassTag[T])
  extends Encoder[T] {

  if (flat) require(serializer.size == 1)

  @transient
  private lazy val extractProjection = GenerateUnsafeProjection.generate(serializer)

  @transient
  private lazy val inputRow = new GenericMutableRow(1)

  @transient
  private lazy val constructProjection = GenerateSafeProjection.generate(deserializer :: Nil)

  /**
   * Returns this encoder where it has been bound to its own output (i.e. no remaping of columns
   * is performed).
   */
  def defaultBinding: ExpressionEncoder[T] = {
    val attrs = schema.toAttributes
    resolve(attrs, OuterScopes.outerScopes).bind(attrs)
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
   * Returns an encoded version of `t` as a Spark SQL row.  Note that multiple calls to
   * toRow are allowed to return the same actual [[InternalRow]] object.  Thus, the caller should
   * copy the result before making another call if required.
   */
  def toRow(t: T): InternalRow = try {
    inputRow(0) = t
    extractProjection(inputRow)
  } catch {
    case e: Exception =>
      throw new RuntimeException(
        s"Error while encoding: $e\n${serializer.map(_.treeString).mkString("\n")}", e)
  }

  /**
   * Returns an object of type `T`, extracting the required values from the provided row.  Note that
   * you must `resolve` and `bind` an encoder to a specific schema before you can call this
   * function.
   */
  def fromRow(row: InternalRow): T = try {
    constructProjection(row).get(0, ObjectType(clsTag.runtimeClass)).asInstanceOf[T]
  } catch {
    case e: Exception =>
      throw new RuntimeException(s"Error while decoding: $e\n${deserializer.treeString}", e)
  }

  /**
   * The process of resolution to a given schema throws away information about where a given field
   * is being bound by ordinal instead of by name.  This method checks to make sure this process
   * has not been done already in places where we plan to do later composition of encoders.
   */
  def assertUnresolved(): Unit = {
    (deserializer +:  serializer).foreach(_.foreach {
      case a: AttributeReference if a.name != "loopVar" =>
        sys.error(s"Unresolved encoder expected, but $a was found.")
      case _ =>
    })
  }

  /**
   * Validates `deserializer` to make sure it can be resolved by given schema, and produce
   * friendly error messages to explain why it fails to resolve if there is something wrong.
   */
  def validate(schema: Seq[Attribute]): Unit = {
    def fail(st: StructType, maxOrdinal: Int): Unit = {
      throw new AnalysisException(s"Try to map ${st.simpleString} to Tuple${maxOrdinal + 1}, " +
        "but failed as the number of fields does not line up.\n" +
        " - Input schema: " + StructType.fromAttributes(schema).simpleString + "\n" +
        " - Target schema: " + this.schema.simpleString)
    }

    // If this is a tuple encoder or tupled encoder, which means its leaf nodes are all
    // `BoundReference`, make sure their ordinals are all valid.
    var maxOrdinal = -1
    deserializer.foreach {
      case b: BoundReference => if (b.ordinal > maxOrdinal) maxOrdinal = b.ordinal
      case _ =>
    }
    if (maxOrdinal >= 0 && maxOrdinal != schema.length - 1) {
      fail(StructType.fromAttributes(schema), maxOrdinal)
    }

    // If we have nested tuple, the `fromRowExpression` will contains `GetStructField` instead of
    // `UnresolvedExtractValue`, so we need to check if their ordinals are all valid.
    // Note that, `BoundReference` contains the expected type, but here we need the actual type, so
    // we unbound it by the given `schema` and propagate the actual type to `GetStructField`, after
    // we resolve the `fromRowExpression`.
    val resolved = SimpleAnalyzer.resolveExpression(
      deserializer,
      LocalRelation(schema),
      throws = true)

    val unbound = resolved transform {
      case b: BoundReference => schema(b.ordinal)
    }

    val exprToMaxOrdinal = scala.collection.mutable.HashMap.empty[Expression, Int]
    unbound.foreach {
      case g: GetStructField =>
        val maxOrdinal = exprToMaxOrdinal.getOrElse(g.child, -1)
        if (maxOrdinal < g.ordinal) {
          exprToMaxOrdinal.update(g.child, g.ordinal)
        }
      case _ =>
    }
    exprToMaxOrdinal.foreach {
      case (expr, maxOrdinal) =>
        val schema = expr.dataType.asInstanceOf[StructType]
        if (maxOrdinal != schema.length - 1) {
          fail(schema, maxOrdinal)
        }
    }
  }

  /**
   * Returns a new copy of this encoder, where the `deserializer` is resolved to the given schema.
   */
  def resolve(
      schema: Seq[Attribute],
      outerScopes: ConcurrentMap[String, AnyRef]): ExpressionEncoder[T] = {
    // Make a fake plan to wrap the deserializer, so that we can go though the whole analyzer, check
    // analysis, go through optimizer, etc.
    val plan = Project(
      Alias(UnresolvedDeserializer(deserializer, schema), "")() :: Nil,
      LocalRelation(schema))
    val analyzedPlan = SimpleAnalyzer.execute(plan)
    SimpleAnalyzer.checkAnalysis(analyzedPlan)
    copy(deserializer = SimplifyCasts(analyzedPlan).expressions.head.children.head)
  }

  /**
   * Returns a copy of this encoder where the `deserializer` has been bound to the
   * ordinals of the given schema.  Note that you need to first call resolve before bind.
   */
  def bind(schema: Seq[Attribute]): ExpressionEncoder[T] = {
    copy(deserializer = BindReferences.bindReference(deserializer, schema))
  }

  /**
   * Returns a new encoder with input columns shifted by `delta` ordinals
   */
  def shift(delta: Int): ExpressionEncoder[T] = {
    copy(deserializer = deserializer transform {
      case r: BoundReference => r.copy(ordinal = r.ordinal + delta)
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
