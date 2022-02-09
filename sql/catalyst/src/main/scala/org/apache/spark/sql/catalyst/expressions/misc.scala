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

import org.apache.spark.{SPARK_REVISION, SPARK_VERSION_SHORT}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedSeed
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.trees.TreePattern.{CURRENT_LIKE, TreePattern}
import org.apache.spark.sql.catalyst.util.RandomUUIDGenerator
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Print the result of an expression to stderr (used for debugging codegen).
 */
case class PrintToStderr(child: Expression) extends UnaryExpression {

  override def dataType: DataType = child.dataType

  protected override def nullSafeEval(input: Any): Any = {
    // scalastyle:off println
    System.err.println(outputPrefix + input)
    // scalastyle:on println
    input
  }

  private val outputPrefix = s"Result of ${child.simpleString(SQLConf.get.maxToStringFields)} is "

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val outputPrefixField = ctx.addReferenceObj("outputPrefix", outputPrefix)
    nullSafeCodeGen(ctx, ev, c =>
      s"""
         | System.err.println($outputPrefixField + $c);
         | ${ev.value} = $c;
       """.stripMargin)
  }

  override protected def withNewChildInternal(newChild: Expression): PrintToStderr =
    copy(child = newChild)
}

/**
 * Throw with the result of an expression (used for debugging).
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Throws an exception with `expr`.",
  examples = """
    Examples:
      > SELECT _FUNC_('custom error message');
       java.lang.RuntimeException
       custom error message
  """,
  since = "3.1.0",
  group = "misc_funcs")
case class RaiseError(child: Expression, dataType: DataType)
  extends UnaryExpression with ImplicitCastInputTypes {

  def this(child: Expression) = this(child, NullType)

  override def foldable: Boolean = false
  override def nullable: Boolean = true
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def prettyName: String = "raise_error"

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      throw new RuntimeException()
    }
    throw new RuntimeException(value.toString)
  }

  // if (true) is to avoid codegen compilation exception that statement is unreachable
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    ExprCode(
      code = code"""${eval.code}
        |if (true) {
        |  if (${eval.isNull}) {
        |    throw new RuntimeException();
        |  }
        |  throw new RuntimeException(${eval.value}.toString());
        |}""".stripMargin,
      isNull = TrueLiteral,
      value = JavaCode.defaultLiteral(dataType)
    )
  }

  override protected def withNewChildInternal(newChild: Expression): RaiseError =
    copy(child = newChild)
}

object RaiseError {
  def apply(child: Expression): RaiseError = new RaiseError(child)
}

/**
 * A function that throws an exception if 'condition' is not true.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Throws an exception if `expr` is not true.",
  examples = """
    Examples:
      > SELECT _FUNC_(0 < 1);
       NULL
  """,
  since = "2.0.0",
  group = "misc_funcs")
case class AssertTrue(left: Expression, right: Expression, child: Expression)
  extends RuntimeReplaceable {

  override def prettyName: String = "assert_true"

  def this(left: Expression, right: Expression) = {
    this(left, right, If(left, Literal(null), RaiseError(right)))
  }

  def this(left: Expression) = {
    this(left, Literal(s"'${left.simpleString(SQLConf.get.maxToStringFields)}' is not true!"))
  }

  override def flatArguments: Iterator[Any] = Iterator(left, right)
  override def exprsReplaced: Seq[Expression] = Seq(left, right)

  override protected def withNewChildInternal(newChild: Expression): AssertTrue =
    copy(child = newChild)
}

object AssertTrue {
  def apply(left: Expression): AssertTrue = new AssertTrue(left)
}

/**
 * Returns the current database of the SessionCatalog.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the current database.",
  examples = """
    Examples:
      > SELECT _FUNC_();
       default
  """,
  since = "1.6.0",
  group = "misc_funcs")
case class CurrentDatabase() extends LeafExpression with Unevaluable {
  override def dataType: DataType = StringType
  override def nullable: Boolean = false
  override def prettyName: String = "current_database"
  final override val nodePatterns: Seq[TreePattern] = Seq(CURRENT_LIKE)
}

/**
 * Returns the current catalog.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the current catalog.",
  examples = """
    Examples:
      > SELECT _FUNC_();
       spark_catalog
  """,
  since = "3.1.0",
  group = "misc_funcs")
case class CurrentCatalog() extends LeafExpression with Unevaluable {
  override def dataType: DataType = StringType
  override def nullable: Boolean = false
  override def prettyName: String = "current_catalog"
  final override val nodePatterns: Seq[TreePattern] = Seq(CURRENT_LIKE)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """_FUNC_() - Returns an universally unique identifier (UUID) string. The value is returned as a canonical UUID 36-character string.""",
  examples = """
    Examples:
      > SELECT _FUNC_();
       46707d92-02f4-4817-8116-a4c3b23e6266
  """,
  note = """
    The function is non-deterministic.
  """,
  since = "2.3.0",
  group = "misc_funcs")
// scalastyle:on line.size.limit
case class Uuid(randomSeed: Option[Long] = None) extends LeafExpression with Stateful
    with ExpressionWithRandomSeed {

  def this() = this(None)

  override def seedExpression: Expression = randomSeed.map(Literal.apply).getOrElse(UnresolvedSeed)

  override def withNewSeed(seed: Long): Uuid = Uuid(Some(seed))

  override lazy val resolved: Boolean = randomSeed.isDefined

  override def nullable: Boolean = false

  override def dataType: DataType = StringType

  @transient private[this] var randomGenerator: RandomUUIDGenerator = _

  override protected def initializeInternal(partitionIndex: Int): Unit =
    randomGenerator = RandomUUIDGenerator(randomSeed.get + partitionIndex)

  override protected def evalInternal(input: InternalRow): Any =
    randomGenerator.getNextUUIDUTF8String()

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val randomGen = ctx.freshName("randomGen")
    ctx.addMutableState("org.apache.spark.sql.catalyst.util.RandomUUIDGenerator", randomGen,
      forceInline = true,
      useFreshName = false)
    ctx.addPartitionInitializationStatement(s"$randomGen = " +
      "new org.apache.spark.sql.catalyst.util.RandomUUIDGenerator(" +
      s"${randomSeed.get}L + partitionIndex);")
    ev.copy(code = code"final UTF8String ${ev.value} = $randomGen.getNextUUIDUTF8String();",
      isNull = FalseLiteral)
  }

  override def freshCopy(): Uuid = Uuid(randomSeed)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """_FUNC_() - Returns the Spark version. The string contains 2 fields, the first being a release version and the second being a git revision.""",
  examples = """
    Examples:
      > SELECT _FUNC_();
       3.1.0 a6d6ea3efedbad14d99c24143834cd4e2e52fb40
  """,
  since = "3.0.0",
  group = "misc_funcs")
// scalastyle:on line.size.limit
case class SparkVersion() extends LeafExpression with CodegenFallback {
  override def nullable: Boolean = false
  override def foldable: Boolean = true
  override def dataType: DataType = StringType
  override def prettyName: String = "version"
  override def eval(input: InternalRow): Any = {
    UTF8String.fromString(SPARK_VERSION_SHORT + " " + SPARK_REVISION)
  }
}

@ExpressionDescription(
  usage = """_FUNC_(expr) - Return DDL-formatted type string for the data type of the input.""",
  examples = """
    Examples:
      > SELECT _FUNC_(1);
       int
      > SELECT _FUNC_(array(1));
       array<int>
  """,
  since = "3.0.0",
  group = "misc_funcs")
case class TypeOf(child: Expression) extends UnaryExpression {
  override def nullable: Boolean = false
  override def foldable: Boolean = true
  override def dataType: DataType = StringType
  override def eval(input: InternalRow): Any = UTF8String.fromString(child.dataType.catalogString)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, _ => s"""UTF8String.fromString(${child.dataType.catalogString})""")
  }

  override protected def withNewChildInternal(newChild: Expression): TypeOf = copy(child = newChild)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """_FUNC_() - user name of current execution context.""",
  examples = """
    Examples:
      > SELECT _FUNC_();
       mockingjay
  """,
  since = "3.2.0",
  group = "misc_funcs")
// scalastyle:on line.size.limit
case class CurrentUser() extends LeafExpression with Unevaluable {
  override def nullable: Boolean = false
  override def dataType: DataType = StringType
  override def prettyName: String = "current_user"
  final override val nodePatterns: Seq[TreePattern] = Seq(CURRENT_LIKE)
}

/**
 * A function that encrypts input using AES. Key lengths of 128, 192 or 256 bits can be used.
 * For versions prior to JDK 8u161, 192 and 256 bits keys can be used
 * if Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy Files are installed.
 * If either argument is NULL or the key length is not one of the permitted values,
 * the return value is NULL.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, key[, mode[, padding]]) - Returns an encrypted value of `expr` using AES in given `mode` with the specified `padding`.
      Key lengths of 16, 24 and 32 bits are supported. Supported combinations of (`mode`, `padding`) are ('ECB', 'PKCS') and ('GCM', 'NONE').
      The default mode is GCM.
  """,
  arguments = """
    Arguments:
      * expr - The binary value to encrypt.
      * key - The passphrase to use to encrypt the data.
      * mode - Specifies which block cipher mode should be used to encrypt messages.
               Valid modes: ECB, GCM.
      * padding - Specifies how to pad messages whose length is not a multiple of the block size.
                  Valid values: PKCS, NONE, DEFAULT. The DEFAULT padding means PKCS for ECB and NONE for GCM.
  """,
  examples = """
    Examples:
      > SELECT hex(_FUNC_('Spark', '0000111122223333'));
       83F16B2AA704794132802D248E6BFD4E380078182D1544813898AC97E709B28A94
      > SELECT hex(_FUNC_('Spark SQL', '0000111122223333', 'GCM'));
       6E7CA17BBB468D3084B5744BCA729FB7B2B7BCB8E4472847D02670489D95FA97DBBA7D3210
      > SELECT base64(_FUNC_('Spark SQL', '1234567890abcdef', 'ECB', 'PKCS'));
       3lmwu+Mw0H3fi5NDvcu9lg==
  """,
  since = "3.3.0",
  group = "misc_funcs")
case class AesEncrypt(
    input: Expression,
    key: Expression,
    mode: Expression,
    padding: Expression,
    child: Expression)
  extends RuntimeReplaceable {

  def this(input: Expression, key: Expression, mode: Expression, padding: Expression) = {
    this(
      input,
      key,
      mode,
      padding,
      StaticInvoke(
        classOf[ExpressionImplUtils],
        BinaryType,
        "aesEncrypt",
        Seq(input, key, mode, padding),
        Seq(BinaryType, BinaryType, StringType, StringType)))
  }
  def this(input: Expression, key: Expression, mode: Expression) =
    this(input, key, mode, Literal("DEFAULT"))
  def this(input: Expression, key: Expression) =
    this(input, key, Literal("GCM"))

  def exprsReplaced: Seq[Expression] = Seq(input, key, mode, padding)
  protected def withNewChildInternal(newChild: Expression): AesEncrypt =
    copy(child = newChild)
}

/**
 * A function that decrypts input using AES. Key lengths of 128, 192 or 256 bits can be used.
 * For versions prior to JDK 8u161, 192 and 256 bits keys can be used
 * if Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy Files are installed.
 * If either argument is NULL or the key length is not one of the permitted values,
 * the return value is NULL.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr, key[, mode[, padding]]) - Returns a decrypted value of `expr` using AES in `mode` with `padding`.
      Key lengths of 16, 24 and 32 bits are supported. Supported combinations of (`mode`, `padding`) are ('ECB', 'PKCS') and ('GCM', 'NONE').
      The default mode is GCM.
  """,
  arguments = """
    Arguments:
      * expr - The binary value to decrypt.
      * key - The passphrase to use to decrypt the data.
      * mode - Specifies which block cipher mode should be used to decrypt messages.
               Valid modes: ECB, GCM.
      * padding - Specifies how to pad messages whose length is not a multiple of the block size.
                  Valid values: PKCS, NONE, DEFAULT. The DEFAULT padding means PKCS for ECB and NONE for GCM.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(unhex('83F16B2AA704794132802D248E6BFD4E380078182D1544813898AC97E709B28A94'), '0000111122223333');
       Spark
      > SELECT _FUNC_(unhex('6E7CA17BBB468D3084B5744BCA729FB7B2B7BCB8E4472847D02670489D95FA97DBBA7D3210'), '0000111122223333', 'GCM');
       Spark SQL
      > SELECT _FUNC_(unbase64('3lmwu+Mw0H3fi5NDvcu9lg=='), '1234567890abcdef', 'ECB', 'PKCS');
       Spark SQL
  """,
  since = "3.3.0",
  group = "misc_funcs")
case class AesDecrypt(
    input: Expression,
    key: Expression,
    mode: Expression,
    padding: Expression,
    child: Expression)
  extends RuntimeReplaceable {

  def this(input: Expression, key: Expression, mode: Expression, padding: Expression) = {
    this(
      input,
      key,
      mode,
      padding,
      StaticInvoke(
        classOf[ExpressionImplUtils],
        BinaryType,
        "aesDecrypt",
        Seq(input, key, mode, padding),
        Seq(BinaryType, BinaryType, StringType, StringType)))
  }
  def this(input: Expression, key: Expression, mode: Expression) =
    this(input, key, mode, Literal("DEFAULT"))
  def this(input: Expression, key: Expression) =
    this(input, key, Literal("GCM"))

  def exprsReplaced: Seq[Expression] = Seq(input, key)
  protected def withNewChildInternal(newChild: Expression): AesDecrypt =
    copy(child = newChild)
}
// scalastyle:on line.size.limit
