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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, SimpleFunction, UnboundFunction}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType}
import org.apache.spark.util.{SparkTestUtils, Utils}

/**
 * Tests `CREATE [TEMPORARY] FUNCTION name AS 'class'` where the class is a DataSource V2
 * function, i.e. it implements [[UnboundFunction]].
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

  test("DROP FUNCTION") {
    sql(s"CREATE FUNCTION strlen AS '$strLenClass'")
    checkAnswer(sql("SELECT strlen('abc')"), Row(3))
    sql("DROP FUNCTION strlen")
    intercept[AnalysisException](sql("SELECT strlen('abc')"))
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
  private def checkFailsOnlyWhenInvoked(createSql: String)(
      check: AnalysisException => Unit): Unit = {
    withUserDefinedFunction("default.f" -> false) {
      sql(createSql)
      assert(sql("SHOW USER FUNCTIONS").collect().map(_.getString(0)).contains(
        "spark_catalog.default.f"))
      assert(spark.catalog.listFunctions().collect().exists(_.name == "f"))
      sql("DESCRIBE FUNCTION EXTENDED f").collect()
      check(intercept[AnalysisException](sql("SELECT f('abc')").collect()))
    }
  }

  test("class without a public no-arg constructor") {
    val className = classOf[V2FunctionWithoutNoArgConstructor].getName
    val e = intercept[AnalysisException] {
      sql(s"CREATE TEMPORARY FUNCTION f AS '$className'")
    }
    assert(e.getCondition == "CANNOT_INSTANTIATE_FUNCTION_CLASS")
    assert(e.getMessageParameters.get("className") == className)

    checkFailsOnlyWhenInvoked(s"CREATE FUNCTION f AS '$className'") { e =>
      assert(e.getCondition == "CANNOT_INSTANTIATE_FUNCTION_CLASS")
    }
  }

  test("class that is not on the classpath") {
    checkFailsOnlyWhenInvoked("CREATE FUNCTION f AS 'not.a.Class'") { e =>
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
    checkFailsOnlyWhenInvoked(
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

class V2StrLenMagic extends V2StrLen {
  override def bind(inputType: StructType): BoundFunction = {
    super.bind(inputType)
    StrLenMagic
  }
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

class V2FunctionWithoutNoArgConstructor(name: String) extends UnboundFunction {
  override def name(): String = name
  override def bind(inputType: StructType): BoundFunction = StrLenDefault
  override def description(): String = name
}
