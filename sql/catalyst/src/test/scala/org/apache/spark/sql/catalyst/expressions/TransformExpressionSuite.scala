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

import org.apache.logging.log4j.Level

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction}
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, DateType, Decimal, DecimalType, DoubleType, IntegerType, LongType, MapType, StringType, StructType, UserDefinedType, VarcharType}
import org.apache.spark.unsafe.types.UTF8String

class TransformExpressionSuite extends SparkFunSuite {

  /**
   * A bound function with a stable canonical name and no `equals` of its own. A plain class, NOT a
   * case class: a case class would bring structural `equals`, which is the connector-provided
   * comparison these tests are trying to do without.
   */
  private class NamedFunction(canonical: String) extends ScalarFunction[Int] {
    override def inputTypes(): Array[DataType] = Array(IntegerType)
    override def resultType(): DataType = IntegerType
    override def name(): String = canonical
    override def canonicalName(): String = canonical
  }

  /** Honours the contract in `BoundFunction#equals`. */
  private class ComparableFunction extends NamedFunction("test.comparable") {
    override def equals(other: Any): Boolean = other.isInstanceOf[ComparableFunction]
    override def hashCode(): Int = canonicalName().hashCode
  }

  private val a = AttributeReference("a", IntegerType)()
  private val b = AttributeReference("b", IntegerType)()

  private def bucket(function: BoundFunction, child: Expression, numBuckets: Int = 4) =
    TransformExpression(function, Seq(child), Some(numBuckets))

  test("SPARK-58769: expression equality follows the function's own equals") {
    // Spark does not derive transform identity itself -- it defers to the connector, because only
    // the connector knows which of its state matters. Separate instances of a function that does
    // not implement `equals` therefore yield expressions that do not compare equal. See
    // BoundFunction#equals.
    assert(
      bucket(new NamedFunction("test.bucket"), a) != bucket(new NamedFunction("test.bucket"), a),
      "no equals on the function means no equality across binds")

    val shared = new NamedFunction("test.bucket")
    assert(bucket(shared, a) == bucket(shared, a), "a shared instance is equal either way")

    // A function that does implement it gets the deduplication.
    val left = bucket(new ComparableFunction, a)
    val right = bucket(new ComparableFunction, a)
    assert(left.function ne right.function, "the fixture must use distinct function instances")
    assert(left == right)
    assert(left.semanticEquals(right))
    assert(ExpressionSet(Seq(left, right)).size == 1)
  }

  test("SPARK-58769: the two comparisons agree for a function that follows the contract") {
    // `equals` is the finer comparison and the canonical name the coarser one, so a function that
    // overrides both answers both consistently. They still differ in what they take into account:
    // `isSameFunction` ignores the arguments, because a join compares bucket(4, left.id) against
    // bucket(4, right.id) and recovers the positions separately, while equality does not.
    val left = bucket(new ComparableFunction, a)
    val right = bucket(new ComparableFunction, b)
    assert(left.function ne right.function, "the fixture must use distinct function instances")
    assert(left.function == right.function)
    assert(left.function.canonicalName() == right.function.canonicalName())
    assert(left.isSameFunction(right), "the same partition function, arguments aside")
    assert(left != right, "but not the same expression, since the arguments differ")
  }

  test("SPARK-58769: the function's equals does not override the arguments") {
    // A coarse comparison on the connector's side does not have to carry the whole identity: the
    // transform's arguments and bucket count are compared separately, by Spark.
    assert(bucket(new ComparableFunction, a) != bucket(new ComparableFunction, b),
      "different argument")
    assert(bucket(new ComparableFunction, a, 4) != bucket(new ComparableFunction, a, 8),
      "different bucket count")
  }

  test("SPARK-59121: hasSameReducedKeys recognises the pairing that reduced both sides") {
    // A join that reduces both sides leaves their keys in a space neither transform names, and the
    // pairing that produced it is the only thing that tells two such spaces apart.
    val fn = new NamedFunction("test.bucket")
    val b12 = bucket(fn, a, 12)
    val b8 = bucket(fn, a, 8)
    val b18 = bucket(fn, a, 18)
    val left = b12.reducedTogetherWith(b8)
    val right = b8.reducedTogetherWith(b12)

    assert(TransformExpression.hasReducedKeys(left) && TransformExpression.hasReducedKeys(right))
    assert(left.hasSameReducedKeys(left), "an expression shares its own key space")
    assert(left.hasSameReducedKeys(right), "the two sides of one reduce share theirs")
    assert(right.hasSameReducedKeys(left), "and the relation is symmetric")

    assert(!left.hasSameReducedKeys(b12.reducedTogetherWith(b18)),
      "another pairing is another key space")
    assert(!left.hasSameReducedKeys(b12), "an unreduced side never shares one")
    assert(!b12.hasSameReducedKeys(left))
    assert(!b12.hasSameReducedKeys(b8), "nor do two unreduced ones")
  }

  test("SPARK-60027: eval casts only a narrower numeric argument to the function's input type") {
    // Reads the value at the declared `BIGINT`, so it fails on an `INT` that is not cast.
    val longBucket = new ScalarFunction[Int] {
      override def inputTypes(): Array[DataType] = Array(IntegerType, LongType)
      override def resultType(): DataType = IntegerType
      override def name(): String = "bucket"
      override def canonicalName(): String = "test.bucket"
      override def produceResult(input: InternalRow): Int =
        Math.floorMod(input.getLong(1), input.getInt(0).toLong).toInt
    }
    Seq(Literal(7), Literal(7L), Literal(7.toShort)).foreach { value =>
      val transform = bucket(longBucket, value)
      assert(transform.evaluableFunction.isDefined, value.dataType)
      assert(transform.eval(InternalRow.empty) == 3, value.dataType)
    }
    // A wider floating-point type too, as a magic `invoke` got it from Java's widening.
    val doubleBucket = new ScalarFunction[Int] {
      override def inputTypes(): Array[DataType] = Array(IntegerType, DoubleType)
      override def resultType(): DataType = IntegerType
      override def name(): String = "bucket"
      override def canonicalName(): String = "test.bucket"
      override def produceResult(input: InternalRow): Int =
        Math.floorMod(input.getDouble(1).toLong, input.getInt(0).toLong).toInt
    }
    Seq(Literal(7), Literal(7L), Literal(7.0f)).foreach { value =>
      val transform = bucket(doubleBucket, value)
      assert(transform.evaluableFunction.isDefined, value.dataType)
      assert(transform.eval(InternalRow.empty) == 3, value.dataType)
    }

    // Wider than declared, not numeric, or the wrong number of arguments: refused.
    val intBucket = new ScalarFunction[Int] {
      override def inputTypes(): Array[DataType] = Array(IntegerType, IntegerType)
      override def resultType(): DataType = IntegerType
      override def name(): String = "bucket"
      override def canonicalName(): String = "test.bucket"
      override def produceResult(input: InternalRow): Int = input.getInt(1)
    }
    Seq(
      bucket(intBucket, Literal(7L)),
      bucket(longBucket, Literal("7")),
      TransformExpression(longBucket, Seq(Literal(7L))),
      TransformExpression(longBucket, Seq(Literal(7L), Literal(7L)), Some(4))
    ).foreach { transform =>
      assert(transform.evaluableFunction.isEmpty, transform)
      intercept[SparkException](transform.eval(InternalRow.empty))
    }

    // A decimal that a declared decimal of its scale can hold, and a `VARCHAR` against a plain
    // string, go as they are, with the value a function read before this check.
    val decimalBucket = new ScalarFunction[Int] {
      override def inputTypes(): Array[DataType] = Array(IntegerType, DecimalType(38, 2))
      override def resultType(): DataType = IntegerType
      override def name(): String = "bucket"
      override def canonicalName(): String = "test.bucket"
      override def produceResult(input: InternalRow): Int =
        input.getDecimal(1, 38, 2).toUnscaledLong.toInt
    }
    val decimal = bucket(decimalBucket, Literal(Decimal(BigDecimal("12.34"), 10, 2)))
    assert(!decimal.evaluableFunction.get.exists(_.isInstanceOf[Cast]))
    assert(decimal.eval(InternalRow.empty) == 1234, "the unscaled digits at the declared scale")
    // At another scale its unscaled digits would be another number, so it is refused.
    assert(bucket(decimalBucket, Literal(Decimal(BigDecimal("12.3"), 10, 1)))
      .evaluableFunction.isEmpty)
    val stringBucket = new ScalarFunction[Int] {
      override def inputTypes(): Array[DataType] = Array(IntegerType, StringType)
      override def resultType(): DataType = IntegerType
      override def name(): String = "bucket"
      override def canonicalName(): String = "test.bucket"
      override def produceResult(input: InternalRow): Int = input.getUTF8String(1).numChars()
    }
    val varchar = bucket(stringBucket,
      Literal.create(UTF8String.fromString("abc"), VarcharType(10)))
    assert(!varchar.evaluableFunction.get.exists(_.isInstanceOf[Cast]))
    assert(varchar.eval(InternalRow.empty) == 3)

    // The write path coerces the arguments itself, so it keeps the uncast call.
    val stringArgument = bucket(longBucket, Literal("7"))
    assert(stringArgument.resolvedFunction.isDefined)
    assert(!stringArgument.resolvedFunction.get.collectFirst { case c: Cast => c }.isDefined)
  }

  test("SPARK-60027: an argument is taken as it is when it is stored as the declared type is") {
    // A function declaring `declared` reads a value of `argumentType` as it is, field by field and
    // by position, when the two are stored alike, as it did before the input type check. The
    // value is read from an unsafe row, as a one-side shuffle reads it, since that is where a
    // nested decimal is stored by its precision.
    class IntUDT extends UserDefinedType[Integer] {
      override def sqlType: DataType = IntegerType
      override def serialize(obj: Integer): Any = obj
      override def deserialize(datum: Any): Integer = datum.asInstanceOf[Integer]
      override def userClass: Class[Integer] = classOf[Integer]
    }
    def function(declared: DataType)(read: InternalRow => Int): ScalarFunction[Int] =
      new ScalarFunction[Int] {
        override def inputTypes(): Array[DataType] = Array(IntegerType, declared)
        override def resultType(): DataType = IntegerType
        override def name(): String = "bucket"
        override def canonicalName(): String = "test.bucket"
        override def produceResult(input: InternalRow): Int =
          Math.floorMod(read(input), input.getInt(0))
      }
    def struct(field: String, dataType: DataType): StructType =
      new StructType().add(field, dataType)
    val structField = (row: InternalRow) => row.getStruct(1, 1).getInt(0)
    val fiveCents = Decimal(BigDecimal("0.05"), 10, 2)
    def unscaled(decimal: InternalRow => Decimal): InternalRow => Int =
      row => decimal(row).toUnscaledLong.toInt
    // (what, the argument's type, the declared type, the value, how the function reads it,
    //  whether it is taken)
    Seq(
      ("a field name in another case", struct("a", IntegerType), struct("A", IntegerType),
        InternalRow(5), structField, true),
      ("a renamed field", struct("x", IntegerType), struct("y", IntegerType), InternalRow(5),
        structField, true),
      ("a DATE field against an INT one", struct("d", DateType), struct("d", IntegerType),
        InternalRow(5), structField, true),
      ("a user-defined type against its sqlType", new IntUDT, IntegerType, 5,
        (row: InternalRow) => row.getInt(1), true),
      // At the top level the function gets the `Decimal` itself, so a key of the declared scale
      // with more precision, e.g. a cogroup's, is taken.
      ("a decimal with more precision at the declared scale",
        DecimalType(12, 2), DecimalType(10, 2), Decimal(BigDecimal("123.45"), 12, 2),
        unscaled(_.getDecimal(1, 10, 2)), true),
      // Nested in an unsafe row, a decimal of up to 18 digits is a `long`, and a wider one bytes,
      // and it is read at the declared precision. So it is taken only with no more precision than
      // declared, and on the same side of 18 digits.
      ("a nested decimal with more precision on the long side",
        struct("d", DecimalType(10, 2)), struct("d", DecimalType(18, 2)), InternalRow(fiveCents),
        unscaled(_.getStruct(1, 1).getDecimal(0, 18, 2)), true),
      ("an array element decimal with more precision on the long side",
        ArrayType(DecimalType(10, 2)), ArrayType(DecimalType(18, 2)),
        new GenericArrayData(Array[Any](fiveCents)),
        unscaled(_.getArray(1).getDecimal(0, 18, 2)), true),
      ("a map value decimal with more precision on the long side",
        MapType(IntegerType, DecimalType(10, 2)), MapType(IntegerType, DecimalType(18, 2)),
        ArrayBasedMapData(Map(1 -> fiveCents)),
        unscaled(_.getMap(1).valueArray().getDecimal(0, 18, 2)), true),
      ("a nested decimal across the long and the bytes storage",
        struct("d", DecimalType(10, 2)), struct("d", DecimalType(38, 2)), InternalRow(fiveCents),
        structField, false),
      ("an array element decimal across the long and the bytes storage",
        ArrayType(DecimalType(10, 2)), ArrayType(DecimalType(38, 2)),
        new GenericArrayData(Array[Any](fiveCents)), structField, false),
      ("a map value decimal across the long and the bytes storage",
        MapType(IntegerType, DecimalType(10, 2)), MapType(IntegerType, DecimalType(38, 2)),
        ArrayBasedMapData(Map(1 -> fiveCents)), structField, false),
      ("a nested decimal with more precision than declared",
        struct("d", DecimalType(12, 2)), struct("d", DecimalType(10, 2)),
        InternalRow(Decimal(BigDecimal("0.05"), 12, 2)), structField, false),
      ("a field of another physical type", struct("d", DateType), struct("d", LongType),
        InternalRow(5), structField, false),
      ("a struct with another number of fields", struct("a", IntegerType),
        struct("a", IntegerType).add("b", IntegerType), InternalRow(5), structField, false)
    ).foreach { case (what, argumentType, declared, value, read, taken) =>
      val argument = BoundReference(0, argumentType, nullable = true)
      val transform = TransformExpression(function(declared)(read), Seq(argument), Some(4))
      assert(transform.evaluableFunction.isDefined == taken, what)
      if (taken) {
        val unsafe = UnsafeProjection.create(Seq(argument)).apply(InternalRow(value))
        assert(transform.eval(unsafe) == 1, what)
      }
    }
  }

  test("SPARK-60027: a user-defined type is refused where a magic invoke cannot take it") {
    // A user-defined type goes as its `sqlType`, and `resolveScalarFunction` finds a magic
    // `invoke` from the declared types. But `Invoke` and `StaticInvoke` look the method up again
    // from the arguments' classes, a user-defined type's being `Object`, so that call cannot be
    // made and is refused, as the argument mismatch it is: logged at info level, not warned about
    // as a call that fails to resolve. A `produceResult` reads the row, so it takes the value.
    class IntUDT extends UserDefinedType[Integer] {
      override def sqlType: DataType = IntegerType
      override def serialize(obj: Integer): Any = obj
      override def deserialize(datum: Any): Integer = datum.asInstanceOf[Integer]
      override def userClass: Class[Integer] = classOf[Integer]
    }
    val withInvoke = new ScalarFunction[Int] {
      override def inputTypes(): Array[DataType] = Array(IntegerType, IntegerType)
      override def resultType(): DataType = IntegerType
      override def name(): String = "udt_bucket"
      override def canonicalName(): String = "test.bucket"
      def invoke(numBuckets: Int, value: Int): Int = Math.floorMod(value, numBuckets)
    }
    val withProduceResult = new ScalarFunction[Int] {
      override def inputTypes(): Array[DataType] = Array(IntegerType, IntegerType)
      override def resultType(): DataType = IntegerType
      override def name(): String = "bucket"
      override def canonicalName(): String = "test.bucket"
      override def produceResult(input: InternalRow): Int =
        Math.floorMod(input.getInt(1), input.getInt(0))
    }
    val argument = BoundReference(0, new IntUDT, nullable = true)
    val appender = new LogAppender("a user-defined type a magic invoke cannot take")
    withLogAppender(appender, level = Some(Level.INFO)) {
      assert(bucket(withInvoke, argument).evaluableFunction.isEmpty, "a magic invoke")
    }
    val logged = appender.loggingEvents
      .filter(_.getLoggerName.endsWith("TransformExpression"))
      .filter(_.getMessage.getFormattedMessage.contains("udt_bucket"))
    assert(logged.map(_.getLevel) == Seq(Level.INFO), logged.map(_.getMessage.getFormattedMessage))
    assert(logged.head.getMessage.getFormattedMessage.contains("which do not take arguments"))
    val transform = bucket(withProduceResult, argument)
    assert(transform.evaluableFunction.isDefined, "produceResult")
    assert(transform.eval(InternalRow(5)) == 1)
  }

  test("SPARK-60027: a transform whose call fails to resolve is not evaluable, and says why") {
    // Connector code can throw while Spark resolves the call. That counts as cannot evaluate, and
    // the answer is kept, so the connector is asked once. `eval` rethrows what it threw.
    var calls = 0
    val failing = new ScalarFunction[Int] {
      override def inputTypes(): Array[DataType] = {
        calls += 1
        throw new IllegalStateException("connector bug")
      }
      override def resultType(): DataType = IntegerType
      override def name(): String = "bucket"
      override def canonicalName(): String = "test.bucket"
      override def produceResult(input: InternalRow): Int = 0
    }
    val transform = bucket(failing, Literal(7))
    assert(transform.evaluableFunction.isEmpty)
    assert(transform.evaluableFunction.isEmpty)
    assert(calls == 1, "asked once")
    val e = intercept[IllegalStateException](transform.eval(InternalRow.empty))
    assert(e.getMessage == "connector bug")
  }

  test("SPARK-60027: a function whose call fails to resolve is warned about once") {
    // The planner asks this of layouts it does not pick, on every query, so another transform
    // over the same function class is not warned about again.
    val failing = new ScalarFunction[Int] {
      override def inputTypes(): Array[DataType] = throw new IllegalStateException("connector bug")
      override def resultType(): DataType = IntegerType
      override def name(): String = "bucket"
      override def canonicalName(): String = "test.bucket"
      override def produceResult(input: InternalRow): Int = 0
    }
    val appender = new LogAppender("cannot resolve a partition transform call")
    withLogAppender(appender, level = Some(Level.WARN)) {
      Seq(Literal(1), Literal(2), Literal(3)).foreach { value =>
        assert(bucket(failing, value).evaluableFunction.isEmpty)
      }
    }
    val warnings = appender.loggingEvents
      .filter(e => e.getLevel == Level.WARN && e.getLoggerName.endsWith("TransformExpression"))
      .map(_.getMessage.getFormattedMessage)
    assert(warnings.size == 1, warnings)
    assert(warnings.head.contains("connector bug"))
  }

  test("SPARK-60027: a function that cannot be linked is not evaluable, nor fails the logging") {
    // A connector class whose dependency is missing throws a `LinkageError` while Spark looks up
    // its methods, which `Try` would let through, and its `name()` can throw too.
    val unlinked = new ScalarFunction[Int] {
      override def inputTypes(): Array[DataType] = {
        // scalastyle:off throwerror
        throw new NoClassDefFoundError("missing/Dep")
        // scalastyle:on throwerror
      }
      override def resultType(): DataType = IntegerType
      override def name(): String = throw new IllegalStateException("no name")
      override def canonicalName(): String = "test.bucket"
      override def produceResult(input: InternalRow): Int = 0
    }
    val transform = bucket(unlinked, Literal(7))
    assert(transform.evaluableFunction.isEmpty)
    intercept[NoClassDefFoundError](transform.eval(InternalRow.empty))
  }

  test("SPARK-60027: a function whose name and failure cannot be linked still logs its failure") {
    // `name()` and the failure's message are connector code too, and a missing dependency can make
    // them throw a `LinkageError`. The warning falls back to the class names instead.
    class UnlinkedFailure extends IllegalStateException {
      override def getMessage: String = {
        // scalastyle:off throwerror
        throw new NoClassDefFoundError("missing/Message")
        // scalastyle:on throwerror
      }
    }
    val unlinked = new ScalarFunction[Int] {
      override def inputTypes(): Array[DataType] = throw new UnlinkedFailure
      override def resultType(): DataType = IntegerType
      override def name(): String = {
        // scalastyle:off throwerror
        throw new NoClassDefFoundError("missing/Name")
        // scalastyle:on throwerror
      }
      override def canonicalName(): String = "test.bucket"
      override def produceResult(input: InternalRow): Int = 0
    }
    val appender = new LogAppender("partition transform call failure with unlinked text")
    withLogAppender(appender, level = Some(Level.WARN)) {
      assert(bucket(unlinked, Literal(7)).evaluableFunction.isEmpty)
    }
    val warnings = appender.loggingEvents
      .filter(e => e.getLevel == Level.WARN && e.getLoggerName.endsWith("TransformExpression"))
      .map(_.getMessage.getFormattedMessage)
    assert(warnings.size == 1, warnings)
    assert(warnings.head.contains(unlinked.getClass.getName), warnings.head)
    assert(warnings.head.contains(classOf[UnlinkedFailure].getName), warnings.head)
  }

  test("SPARK-60027: arguments that do not suit the input types are logged once per types") {
    // A mismatch makes a storage-partitioned join shuffle where it would not otherwise, so it is
    // logged, once per function class and argument types.
    class IntBucket extends ScalarFunction[Int] {
      override def inputTypes(): Array[DataType] = Array(IntegerType, IntegerType)
      override def resultType(): DataType = IntegerType
      override def name(): String = "int_bucket"
      override def canonicalName(): String = "test.bucket"
      override def produceResult(input: InternalRow): Int = 0
    }
    val appender = new LogAppender("partition transform argument mismatches")
    withLogAppender(appender, level = Some(Level.INFO)) {
      Seq(Literal(7L), Literal(8L), Literal("7")).foreach { value =>
        assert(bucket(new IntBucket, value).evaluableFunction.isEmpty, value.dataType)
      }
      assert(bucket(new IntBucket, Literal(7)).evaluableFunction.isDefined, "matching types")
    }
    val logged = appender.loggingEvents
      .filter(e => e.getLevel == Level.INFO && e.getLoggerName.endsWith("TransformExpression"))
      .map(_.getMessage.getFormattedMessage)
    assert(logged.size == 2, logged)
    assert(logged.exists(m => m.contains("int_bucket") && m.contains("(int, int)") &&
      m.contains("(int, bigint)")), logged)
    assert(logged.exists(_.contains("(int, string)")), logged)
  }

  test("SPARK-60027: a function that is not a ScalarFunction is logged once per class") {
    // A class of its own, since the set of classes logged for is shared by the JVM.
    class NotScalar extends BoundFunction {
      override def inputTypes(): Array[DataType] = Array(IntegerType, LongType)
      override def resultType(): DataType = IntegerType
      override def name(): String = "bucket"
      override def canonicalName(): String = "test.bucket"
    }
    val appender = new LogAppender("partition transform that is not a ScalarFunction")
    withLogAppender(appender, level = Some(Level.INFO)) {
      Seq(Literal(1L), Literal(2L)).foreach { value =>
        assert(bucket(new NotScalar, value).evaluableFunction.isEmpty)
      }
    }
    val logged = appender.loggingEvents
      .filter(e => e.getLevel == Level.INFO && e.getLoggerName.endsWith("TransformExpression"))
      .map(_.getMessage.getFormattedMessage)
      .filter(_.contains("is not a ScalarFunction"))
    assert(logged.size == 1, logged)
  }

  test("SPARK-60027: a mismatch is logged once per argument and declared types") {
    // One class can declare other input types per instance, e.g. one per column type, so each
    // declared signature a mismatch is logged for gets its own line.
    class TypedBucket(valueType: DataType) extends ScalarFunction[Int] {
      override def inputTypes(): Array[DataType] = Array(IntegerType, valueType)
      override def resultType(): DataType = IntegerType
      override def name(): String = "typed_bucket"
      override def canonicalName(): String = "test.bucket"
      override def produceResult(input: InternalRow): Int = 0
    }
    val appender = new LogAppender("partition transform mismatches per declared types")
    withLogAppender(appender, level = Some(Level.INFO)) {
      Seq(StringType, BinaryType, StringType).foreach { declared =>
        assert(bucket(new TypedBucket(declared), Literal(7L)).evaluableFunction.isEmpty)
      }
    }
    val logged = appender.loggingEvents
      .filter(e => e.getLevel == Level.INFO && e.getLoggerName.endsWith("TransformExpression"))
      .map(_.getMessage.getFormattedMessage)
      .filter(_.contains("typed_bucket"))
    assert(logged.size == 2, logged)
    assert(logged.exists(_.contains("(int, string)")) && logged.exists(_.contains("(int, binary)")))
  }

  test("SPARK-60027: a function class that fails in another way is warned about again") {
    class Failing(failure: () => Throwable) extends ScalarFunction[Int] {
      override def inputTypes(): Array[DataType] = throw failure()
      override def resultType(): DataType = IntegerType
      override def name(): String = "bucket"
      override def canonicalName(): String = "test.bucket"
      override def produceResult(input: InternalRow): Int = 0
    }
    val appender = new LogAppender("partition transform call failures by kind")
    withLogAppender(appender, level = Some(Level.WARN)) {
      Seq[() => Throwable](
        () => new IllegalStateException("first"),
        () => new IllegalArgumentException("second"),
        () => new IllegalStateException("first again")
      ).foreach { failure =>
        assert(bucket(new Failing(failure), Literal(7)).evaluableFunction.isEmpty)
      }
    }
    val warnings = appender.loggingEvents
      .filter(e => e.getLevel == Level.WARN && e.getLoggerName.endsWith("TransformExpression"))
      .map(_.getMessage.getFormattedMessage)
    assert(warnings.size == 2, warnings)
    assert(warnings.exists(_.contains("first")) && warnings.exists(_.contains("second")))
  }
}
