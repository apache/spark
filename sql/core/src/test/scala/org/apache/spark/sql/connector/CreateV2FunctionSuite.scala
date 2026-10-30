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

package org.apache.spark.sql.connector

import java.io.File
import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.ClassTag

import test.org.apache.spark.sql.connector.catalog.functions.{JavaInitTrackingStrLen, JavaStaticInitFailingStrLen}
import test.org.apache.spark.sql.connector.catalog.functions.JavaStrLen.JavaStrLenNoImpl

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, SimpleFunction, UnboundFunction}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType}
import org.apache.spark.util.{SparkTestUtils, Utils}

/**
 * Tests `CREATE [TEMPORARY] FUNCTION name AS 'class'` where the class is a V2 function, i.e. it
 * implements [[UnboundFunction]].
 */
class CreateV2FunctionSuite extends QueryTest with SharedSparkSession {

  private val strLenClass = classOf[V2StrLen].getName
  private val strLenMagicClass = classOf[V2StrLenMagic].getName
  private val avgClass = classOf[V2IntegralAverage].getName

  // Runs on executors, which must load the class from the jar too.
  private val jarStrLenQuery = "SELECT jar_strlen(CAST(id AS STRING)) FROM range(9, 11)"

  /**
   * Compiles a V2 string length function named `className` into a jar that is not on the
   * classpath, and returns the jar URI. The jar is named after the class, as Spark rejects
   * registering the same jar name with a different path. With `magic`, the function implements
   * the magic `invoke` method, otherwise `produceResult`.
   */
  private def createStrLenJar(className: String, magic: Boolean = true): String = {
    val dir = Utils.createTempDir()
    val cp = ManagementFactory.getRuntimeMXBean.getClassPath
      .split(File.pathSeparator).map(p => new File(p).toURI.toURL).toSeq
    val simpleName = className.split('.').last
    val method = if (magic) {
      "public int invoke(UTF8String s) { return s.numChars(); }"
    } else {
      "public Integer produceResult(InternalRow r) { return r.getUTF8String(0).numChars(); }"
    }
    val source =
      s"""package ${className.stripSuffix("." + simpleName)};
         |
         |import org.apache.spark.sql.catalyst.InternalRow;
         |import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
         |import org.apache.spark.sql.connector.catalog.functions.SimpleFunction;
         |import org.apache.spark.sql.types.DataType;
         |import org.apache.spark.sql.types.DataTypes;
         |import org.apache.spark.unsafe.types.UTF8String;
         |
         |public class $simpleName implements SimpleFunction, ScalarFunction<Integer> {
         |  public DataType[] inputTypes() { return new DataType[] { DataTypes.StringType }; }
         |  public DataType resultType() { return DataTypes.IntegerType; }
         |  public String name() { return "jar_strlen"; }
         |  public String description() { return "jar_strlen(string) -> int"; }
         |  $method
         |}
         |""".stripMargin
    SparkTestUtils.createJarWithJavaSources(
      Map(className -> source), new File(dir, s"$simpleName.jar"), cp).toURI.toString
  }

  test("temporary scalar function") {
    withUserDefinedFunction("strlen" -> true) {
      sql(s"CREATE TEMPORARY FUNCTION strlen AS '$strLenClass'")
      checkAnswer(sql("SELECT strlen('abc')"), Row(3))
    }
  }

  test("temporary scalar function with magic method") {
    withUserDefinedFunction("strlen" -> true) {
      sql(s"CREATE TEMPORARY FUNCTION strlen AS '$strLenMagicClass'")
      checkAnswer(sql("SELECT strlen('abcd')"), Row(4))
    }
  }

  test("persistent scalar function") {
    withUserDefinedFunction("default.strlen" -> false) {
      sql(s"CREATE FUNCTION strlen AS '$strLenClass'")
      checkAnswer(sql("SELECT strlen('abc')"), Row(3))
      checkAnswer(sql("SELECT default.strlen('abcde')"), Row(5))
      checkAnswer(sql("SELECT spark_catalog.default.strlen('ab')"), Row(2))
    }
  }

  test("CREATE OR REPLACE FUNCTION") {
    withUserDefinedFunction("default.f" -> false) {
      sql(s"CREATE FUNCTION f AS '$strLenClass'")
      checkAnswer(sql("SELECT f('abc')"), Row(3))
      sql(s"CREATE OR REPLACE FUNCTION f AS '$avgClass'")
      checkAnswer(sql("SELECT f(id) FROM VALUES (1), (3) AS t(id)"), Row(2))
    }
  }

  test("aggregate function") {
    withUserDefinedFunction("iavg" -> true) {
      sql(s"CREATE TEMPORARY FUNCTION iavg AS '$avgClass'")
      checkAnswer(sql("SELECT iavg(id) FROM VALUES (1), (2), (3), (5) AS t(id)"), Row(2))
      // DISTINCT and FILTER are supported by aggregate functions.
      checkAnswer(
        sql("SELECT iavg(DISTINCT id) FROM VALUES (1), (1), (5) AS t(id)"), Row(3))
      checkAnswer(
        sql("SELECT iavg(id) FILTER (WHERE id > 1) FROM VALUES (1), (2), (4) AS t(id)"), Row(3))
      // The bound function is chosen by the input type.
      checkAnswer(sql("SELECT iavg(id) FROM VALUES (1L), (4L) AS t(id)"), Row(2L))
    }
  }

  test("SimpleFunction") {
    withUserDefinedFunction("strlen" -> true) {
      sql(s"CREATE TEMPORARY FUNCTION strlen AS '${classOf[V2SimpleStrLen].getName}'")
      checkAnswer(sql("SELECT strlen('abc')"), Row(3))
      // Without `bind`, the arguments are implicitly cast to the declared input types.
      checkAnswer(sql("SELECT strlen(1234)"), Row(4))
      val e = intercept[AnalysisException](sql("SELECT strlen('a', 'b')"))
      assert(e.getCondition == "_LEGACY_ERROR_TEMP_1199")
    }
  }

  test("temporary function USING JAR") {
    val className = "test.v2udf.TempStrLen"
    assert(!Utils.classIsLoadable(className))
    val jar = createStrLenJar(className)
    withUserDefinedFunction("jar_strlen" -> true) {
      sql(s"CREATE TEMPORARY FUNCTION jar_strlen AS '$className' USING JAR '$jar'")
      checkAnswer(sql(jarStrLenQuery), Row(1) :: Row(2) :: Nil)
    }
  }

  test("temporary function after ADD JAR") {
    val className = "test.v2udf.AddJarStrLen"
    assert(!Utils.classIsLoadable(className))
    sql(s"ADD JAR '${createStrLenJar(className)}'")
    withUserDefinedFunction("jar_strlen" -> true) {
      sql(s"CREATE TEMPORARY FUNCTION jar_strlen AS '$className'")
      checkAnswer(sql(jarStrLenQuery), Row(1) :: Row(2) :: Nil)
    }
  }

  test("persistent function USING JAR") {
    val className = "test.v2udf.PersistentStrLen"
    assert(!Utils.classIsLoadable(className))
    // Without the magic method, so that the generated code does not refer to the class.
    val jar = createStrLenJar(className, magic = false)
    Utils.withContextClassLoader(Utils.getSparkClassLoader) {
      withUserDefinedFunction("default.jar_strlen" -> false) {
        sql(s"CREATE FUNCTION jar_strlen AS '$className' USING JAR '$jar'")
        def describe(): Seq[String] =
          sql("DESCRIBE FUNCTION jar_strlen").collect().map(_.getString(0)).toSeq
        // The jar is loaded on first invocation, so the description is not available yet.
        assert(!describe().exists(_.contains("jar_strlen(string) -> int")))

        checkAnswer(sql(jarStrLenQuery), Row(1) :: Row(2) :: Nil)
        assert(describe().exists(_.contains("jar_strlen(string) -> int")))

        // Loading the jar switches the context classloader; the function must still work
        // after it is rolled back, as spark-shell does.
        Thread.currentThread().setContextClassLoader(Utils.getSparkClassLoader)
        checkAnswer(sql(jarStrLenQuery), Row(1) :: Row(2) :: Nil)
      }
    }
  }

  test("DESCRIBE FUNCTION reports the V2 function description") {
    withUserDefinedFunction("default.strlen" -> false) {
      sql(s"CREATE FUNCTION strlen AS '$strLenClass'")
      val output = sql("DESCRIBE FUNCTION default.strlen").collect().map(_.getString(0))
      assert(output.exists(_.contains(strLenClass)))
      assert(output.exists(_.contains("strlen(string) -> int")))
    }
  }

  test("persistent existence and first invocation do not construct unused metadata instances") {
    V2MetadataTrackingStrLen.constructions.set(0)
    val className = classOf[V2MetadataTrackingStrLen].getName
    withUserDefinedFunction("default.metadata_len" -> false) {
      sql(s"CREATE FUNCTION metadata_len AS '$className'")
      assert(spark.catalog.functionExists("metadata_len"))
      assert(spark.catalog.functionExists("default", "metadata_len"))
      assert(V2MetadataTrackingStrLen.constructions.get() == 0)
      checkAnswer(sql("SELECT metadata_len('abc')"), Row(3))
      assert(V2MetadataTrackingStrLen.constructions.get() == 1)
      assert(spark.catalog.getFunction("metadata_len").className == className)
      assert(V2MetadataTrackingStrLen.constructions.get() == 1)
    }
  }

  test("persistent metadata-only reads recover after ADD JAR") {
    val className = "test.v2udf.MetadataAfterAddJarStrLen"
    assert(!Utils.classIsLoadable(className))
    withUserDefinedFunction("default.metadata_len" -> false) {
      sql(s"CREATE FUNCTION metadata_len AS '$className'")
      assert(spark.catalog.getFunction("metadata_len").description == "N/A.")
      sql(s"ADD JAR '${createStrLenJar(className)}'")
      assert(spark.catalog.getFunction("metadata_len").description == "jar_strlen(string) -> int")
      assert(sql("DESCRIBE FUNCTION metadata_len").collect().map(_.getString(0))
        .exists(_.contains("jar_strlen(string) -> int")))
    }
  }

  gridTest("class whose description throws")(
      Seq(classOf[V2FunctionThrowingDescription].getName,
        classOf[V2FunctionLinkageErrorDescription].getName)) { className =>
    withUserDefinedFunction("tstrlen" -> true, "default.pstrlen" -> false) {
      sql(s"CREATE TEMPORARY FUNCTION tstrlen AS '$className'")
      sql(s"CREATE FUNCTION pstrlen AS '$className'")

      def checkUnavailableDescription(name: String): Unit = {
        val output = sql(s"DESCRIBE FUNCTION $name").collect().map(_.getString(0))
        assert(output.contains(s"Class: $className"))
        assert(output.contains("Usage: N/A."))
        assert(spark.catalog.getFunction(name).className == className)
        val info = spark.sessionState.catalog.lookupFunctionInfo(FunctionIdentifier(name))
        assert(info.getSource == "v2_function")
      }

      checkUnavailableDescription("tstrlen")
      checkUnavailableDescription("pstrlen")
      checkAnswer(sql("SELECT tstrlen(CAST(id AS STRING)) FROM range(9, 11)"),
        Row(1) :: Row(2) :: Nil)
      checkAnswer(sql("SELECT pstrlen(CAST(id AS STRING)) FROM range(9, 11)"),
        Row(1) :: Row(2) :: Nil)
      checkUnavailableDescription("pstrlen")

      sql("REFRESH FUNCTION pstrlen")
      checkAnswer(sql("SELECT pstrlen('abc')"), Row(3))
      checkUnavailableDescription("pstrlen")
    }
  }

  test("DROP FUNCTION") {
    sql(s"CREATE FUNCTION strlen AS '$strLenClass'")
    checkAnswer(sql("SELECT strlen('abc')"), Row(3))
    sql("DROP FUNCTION strlen")
    intercept[AnalysisException](sql("SELECT strlen('abc')"))
  }

  test("unsupported clauses and column names use the function name") {
    withUserDefinedFunction("strlen" -> true, "iavg" -> true, "default.pstrlen" -> false) {
      sql(s"CREATE TEMPORARY FUNCTION strlen AS '$strLenClass'")
      sql(s"CREATE TEMPORARY FUNCTION iavg AS '$avgClass'")
      sql(s"CREATE FUNCTION pstrlen AS '$strLenClass'")
      assert(sql("SELECT strlen('a'), pstrlen('a'), iavg(id) FROM range(3)").columns.toSeq ==
        Seq("strlen(a)", "spark_catalog.default.pstrlen(a)", "iavg(id)"))

      def checkUnsupported(call: String, name: String, syntax: String): Unit = {
        checkError(
          exception = intercept[AnalysisException](sql(s"SELECT $call FROM range(3)")),
          condition = "INVALID_SQL_SYNTAX.FUNCTION_WITH_UNSUPPORTED_SYNTAX",
          parameters = Map("prettyName" -> name, "syntax" -> syntax),
          context = ExpectedContext(call, 7, 6 + call.length))
      }
      checkUnsupported("strlen(DISTINCT 'a')", "`strlen`", "DISTINCT")
      checkUnsupported("pstrlen(DISTINCT 'a')", "`spark_catalog`.`default`.`pstrlen`", "DISTINCT")
      checkUnsupported("strlen('a') FILTER (WHERE true)", "`strlen`", "FILTER CLAUSE")
      checkUnsupported("iavg(id) IGNORE NULLS", "`iavg`", "IGNORE NULLS")
      checkUnsupported(
        "iavg(id) WITHIN GROUP (ORDER BY id)", "`iavg`", "WITHIN GROUP (ORDER BY ...)")
    }
  }

  gridTest("bind-time errors use the registered function name")(for {
    isTemp <- Seq(true, false)
    funcName <- Seq("my_len", "my.len", "my`len")
  } yield (isTemp, funcName)) { case (isTemp, funcName) =>
    val ident = FunctionIdentifier(funcName, if (isTemp) None else Some("default"))
    val name = ident.quotedString
    val registeredIdent = if (isTemp) ident else ident.copy(catalog = Some("spark_catalog"))
    val registeredName = registeredIdent.unquotedString
    val quotedName = registeredIdent.quotedString
    val create = if (isTemp) "CREATE TEMPORARY FUNCTION" else "CREATE FUNCTION"

    def checkBindingError(
        className: String,
        args: String,
        condition: String,
        parameters: Map[String, String]): Unit = {
      withUserDefinedFunction(name -> isTemp) {
        sql(s"$create $name AS '$className'")
        val call = s"$name($args)"
        checkError(
          exception = intercept[AnalysisException](sql(s"SELECT $call")),
          condition = condition,
          parameters = parameters,
          context = ExpectedContext(call, 7, 6 + call.length))
      }
    }

    checkBindingError(strLenClass, "1", "_LEGACY_ERROR_TEMP_1198",
      Map("unbound" -> registeredName, "arguments" -> "int", "unsupported" -> "Expect StringType"))
    checkBindingError(classOf[V2SimpleStrLen].getName, "'a', 'b'", "_LEGACY_ERROR_TEMP_1199",
      Map("bound" -> registeredName, "argsLen" -> "2", "inputTypesLen" -> "1"))
    checkBindingError(classOf[V2StrLenBadBound].getName, "'abc'", "INVALID_UDF_IMPLEMENTATION",
      Map("funcName" -> quotedName))
    checkBindingError(
      classOf[V2StrLenNoImpl].getName, "'abc'", "SCALAR_FUNCTION_NOT_FULLY_IMPLEMENTED",
      Map("scalarFunc" -> quotedName))
  }

  test("input type that the function cannot bind") {
    withUserDefinedFunction("strlen" -> true) {
      sql(s"CREATE TEMPORARY FUNCTION strlen AS '$strLenClass'")
      val e = intercept[AnalysisException](sql("SELECT strlen(1)"))
      assert(e.getMessage.contains("Expect StringType"))
      val e2 = intercept[AnalysisException](sql("SELECT strlen('a', 'b')"))
      assert(e2.getMessage.contains("Expect exactly one argument"))
    }
  }

  /**
   * Checks that a broken persistent function `f` can be created, listed and described like a
   * Hive UDF, and only fails when invoked.
   */
  private def checkFailsOnlyWhenInvoked[T <: Throwable : ClassTag](createSql: String)(
      check: T => Unit): Unit = {
    withUserDefinedFunction("default.f" -> false) {
      sql(createSql)
      assert(sql("SHOW USER FUNCTIONS").collect().map(_.getString(0)).contains(
        "spark_catalog.default.f"))
      assert(spark.catalog.listFunctions().collect().exists(_.name == "f"))
      assert(spark.catalog.getFunction("f").className == createSql.split("'")(1))
      sql("DESCRIBE FUNCTION EXTENDED f").collect()
      check(intercept[T](sql("SELECT f('abc')").collect()))
    }
  }

  /** Checks that a class that cannot be instantiated fails as `CANNOT_INSTANTIATE_...`. */
  private def checkCannotInstantiate(className: String, msg: String): Unit = {
    checkError(
      exception = intercept[AnalysisException] {
        sql(s"CREATE TEMPORARY FUNCTION f AS '$className'")
      },
      condition = "CANNOT_INSTANTIATE_FUNCTION_CLASS",
      parameters = Map("className" -> className, "functionName" -> "`f`", "msg" -> msg))

    checkFailsOnlyWhenInvoked[AnalysisException](s"CREATE FUNCTION f AS '$className'") { e =>
      checkError(
        exception = e,
        condition = "CANNOT_INSTANTIATE_FUNCTION_CLASS",
        parameters = Map(
          "className" -> className,
          "functionName" -> "`spark_catalog`.`default`.`f`",
          "msg" -> msg),
        context = ExpectedContext("f('abc')", 7, 14))
    }
  }

  test("class without a public no-arg constructor") {
    val className = classOf[V2FunctionWithoutNoArgConstructor].getName
    checkCannotInstantiate(className, s"$className.<init>()")
  }

  test("class whose constructor throws") {
    checkCannotInstantiate(classOf[V2FunctionThrowingConstructor].getName, "constructor failed")
  }

  test("class whose constructor throws a linkage error") {
    checkCannotInstantiate(
      classOf[V2FunctionLinkageErrorConstructor].getName, "constructor dependency")
  }

  gridTest("fatal constructor failures propagate")(
      Seq(
        (classOf[V2FunctionOutOfMemoryConstructor].getName, classOf[OutOfMemoryError]),
        (classOf[V2FunctionStackOverflowConstructor].getName, classOf[StackOverflowError]),
        (classOf[V2FunctionInterruptedConstructor].getName, classOf[InterruptedException]))) {
    case (className, errorClass) =>
      def checkFatal(operation: => Any): Unit = {
        val e = intercept[Throwable](operation)
        assert(e.getClass == errorClass)
        assert(e.getMessage == "constructor failed")
      }

      checkFatal(sql(s"CREATE TEMPORARY FUNCTION f AS '$className'"))
      withUserDefinedFunction("default.f" -> false) {
        sql(s"CREATE FUNCTION f AS '$className'")
        checkFatal(sql("DESCRIBE FUNCTION f").collect())
        checkFatal(spark.catalog.getFunction("f"))
        checkFatal(spark.catalog.listFunctions().collect())
        checkFatal(sql("SELECT f('abc')").collect())
        checkFatal(sql("REFRESH FUNCTION f"))
      }
  }

  test("class that fails to initialize") {
    val className = classOf[JavaStaticInitFailingStrLen].getName
    checkFailsOnlyWhenInvoked[LinkageError](s"CREATE FUNCTION f AS '$className'") { _ => }
  }

  test("USING resources are loaded before the class is initialized") {
    val className = classOf[JavaInitTrackingStrLen].getName
    val initialized = "spark.test.JavaInitTrackingStrLen.initialized"
    // Initializing the class requires a class in this jar, so it fails if done before the jar
    // is loaded.
    val jar = createStrLenJar("test.v2udf.UsingResourceStrLen")
    withUserDefinedFunction("default.f" -> false) {
      sql(s"CREATE FUNCTION f AS '$className' USING JAR '$jar'")
      assert(spark.catalog.functionExists("f"))
      spark.catalog.listFunctions().collect()
      spark.catalog.getFunction("f")
      val describe = sql("DESCRIBE FUNCTION f").collect().map(_.getString(0))
      assert(!describe.exists(_.contains("strlen(string) -> int")))
      assert(System.getProperty(initialized) == null)

      checkAnswer(sql("SELECT f('abc')"), Row(3))
      assert(System.getProperty(initialized) == "true")
      assert(sql("DESCRIBE FUNCTION f").collect().map(_.getString(0))
        .exists(_.contains("strlen(string) -> int")))
    }
  }

  test("class that is not on the classpath") {
    checkFailsOnlyWhenInvoked[AnalysisException]("CREATE FUNCTION f AS 'not.a.Class'") { e =>
      checkError(
        exception = e,
        condition = "CANNOT_LOAD_FUNCTION_CLASS",
        parameters = Map(
          "className" -> "not.a.Class",
          "functionName" -> "`spark_catalog`.`default`.`f`"),
        context = ExpectedContext("f('abc')", 7, 14))
    }
  }

  test("jar that is not accessible") {
    val jar = new File(Utils.createTempDir(), "missing.jar").toURI.toString
    checkFailsOnlyWhenInvoked[AnalysisException](
        s"CREATE FUNCTION f AS 'test.v2udf.MissingJarStrLen' USING JAR '$jar'") { e =>
      assert(e.getCondition == "CANNOT_LOAD_FUNCTION_CLASS", e.getMessage)
    }
  }
}

class V2StrLen extends UnboundFunction {
  override def name(): String = "strlen"

  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.fields.length != 1) {
      throw new UnsupportedOperationException("Expect exactly one argument")
    }
    inputType.fields(0).dataType match {
      case _: StringType => StrLenDefault
      case _ => throw new UnsupportedOperationException("Expect StringType")
    }
  }

  override def description(): String = "strlen: strlen(string) -> int"
}

object V2MetadataTrackingStrLen {
  val constructions = new AtomicInteger
}

class V2MetadataTrackingStrLen extends V2StrLen {
  V2MetadataTrackingStrLen.constructions.incrementAndGet()
}

class V2StrLenMagic extends V2StrLen {
  override def bind(inputType: StructType): BoundFunction = {
    super.bind(inputType)
    StrLenMagic
  }
}

class V2StrLenBadBound extends V2StrLen {
  override def bind(inputType: StructType): BoundFunction = BadBoundFunction
}

class V2StrLenNoImpl extends V2StrLen {
  override def bind(inputType: StructType): BoundFunction = new JavaStrLenNoImpl
}

class V2SimpleStrLen extends SimpleFunction with ScalarFunction[Int] {
  override def inputTypes(): Array[DataType] = Array(StringType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "strlen"
  override def description(): String = "strlen: strlen(string) -> int"
  override def produceResult(input: InternalRow): Int = input.getUTF8String(0).numChars()
}

class V2IntegralAverage extends UnboundFunction {
  override def name(): String = IntegralAverage.name()
  override def bind(inputType: StructType): BoundFunction = IntegralAverage.bind(inputType)
  override def description(): String = IntegralAverage.description()
}

class V2FunctionThrowingDescription extends V2StrLen {
  override def description(): String = throw new UnsupportedOperationException("description failed")
}

class V2FunctionLinkageErrorDescription extends V2StrLen {
  // scalastyle:off throwerror
  override def description(): String = throw new NoClassDefFoundError("description dependency")
  // scalastyle:on throwerror
}

class V2FunctionThrowingConstructor extends V2StrLen {
  throw new IllegalStateException("constructor failed")
}

class V2FunctionLinkageErrorConstructor extends V2StrLen {
  // scalastyle:off throwerror
  throw new NoClassDefFoundError("constructor dependency")
  // scalastyle:on throwerror
}

class V2FunctionOutOfMemoryConstructor extends V2StrLen {
  // scalastyle:off throwerror
  throw new OutOfMemoryError("constructor failed")
  // scalastyle:on throwerror
}

class V2FunctionStackOverflowConstructor extends V2StrLen {
  // scalastyle:off throwerror
  throw new StackOverflowError("constructor failed")
  // scalastyle:on throwerror
}

class V2FunctionInterruptedConstructor extends V2StrLen {
  throw new InterruptedException("constructor failed")
}

class V2FunctionWithoutNoArgConstructor(name: String) extends UnboundFunction {
  override def name(): String = name
  override def bind(inputType: StructType): BoundFunction = StrLenDefault
  override def description(): String = name
}
