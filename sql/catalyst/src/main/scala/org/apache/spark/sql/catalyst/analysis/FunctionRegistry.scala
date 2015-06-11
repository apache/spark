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

package org.apache.spark.sql.catalyst.analysis

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.StringKeyHashMap


/** A catalog for looking up user defined functions, used by an [[Analyzer]]. */
trait FunctionRegistry {

  def registerFunction(name: String, builder: FunctionBuilder): Unit

  @throws[AnalysisException]("If function does not exist")
  def lookupFunction(name: String, children: Seq[Expression]): Expression
}

class OverrideFunctionRegistry(underlying: FunctionRegistry) extends FunctionRegistry {

  private val functionBuilders = StringKeyHashMap[FunctionBuilder](caseSensitive = false)

  override def registerFunction(name: String, builder: FunctionBuilder): Unit = {
    functionBuilders.put(name, builder)
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    functionBuilders.get(name).map(_(children)).getOrElse(underlying.lookupFunction(name, children))
  }
}

class SimpleFunctionRegistry extends FunctionRegistry {

  private val functionBuilders = StringKeyHashMap[FunctionBuilder](caseSensitive = false)

  override def registerFunction(name: String, builder: FunctionBuilder): Unit = {
    functionBuilders.put(name, builder)
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    val func = functionBuilders.get(name).getOrElse {
      throw new AnalysisException(s"undefined function $name")
    }
    func(children)
  }
}

/**
 * A trivial catalog that returns an error when a function is requested. Used for testing when all
 * functions are already filled in and the analyzer needs only to resolve attribute references.
 */
object EmptyFunctionRegistry extends FunctionRegistry {
  override def registerFunction(name: String, builder: FunctionBuilder): Unit = {
    throw new UnsupportedOperationException
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    throw new UnsupportedOperationException
  }
}


object FunctionRegistry {

  type FunctionBuilder = Seq[Expression] => Expression

  val expressions: Map[String, FunctionBuilder] = Map(
    // Non aggregate functions
    expression[Abs]("abs"),
    expression[CreateArray]("array"),
    expression[Coalesce]("coalesce"),
    expression[Explode]("explode"),
    expression[Rand]("rand"),
    expression[Randn]("randn"),
    expression[CreateStruct]("struct"),
    expression[Sqrt]("sqrt"),

    // Math functions
    expression[Acos]("acos"),
    expression[Asin]("asin"),
    expression[Atan]("atan"),
    expression[Atan2]("atan2"),
    expression[Bin]("bin"),
    expression[Cbrt]("cbrt"),
    expression[Ceil]("ceil"),
    expression[Cos]("cos"),
    expression[EulerNumber]("e"),
    expression[Exp]("exp"),
    expression[Expm1]("expm1"),
    expression[Floor]("floor"),
    expression[Hypot]("hypot"),
    expression[Log]("log"),
    expression[Log10]("log10"),
    expression[Log1p]("log1p"),
    expression[Pi]("pi"),
    expression[Log2]("log2"),
    expression[Pow]("pow"),
    expression[Rint]("rint"),
    expression[Signum]("signum"),
    expression[Sin]("sin"),
    expression[Sinh]("sinh"),
    expression[Tan]("tan"),
    expression[Tanh]("tanh"),
    expression[ToDegrees]("todegrees"),
    expression[ToRadians]("toradians"),

    // aggregate functions
    expression[Average]("avg"),
    expression[Count]("count"),
    expression[First]("first"),
    expression[Last]("last"),
    expression[Max]("max"),
    expression[Min]("min"),
    expression[Sum]("sum"),

    // string functions
    expression[Lower]("lower"),
    expression[StringLength]("length"),
    expression[Substring]("substr"),
    expression[Substring]("substring"),
    expression[Upper]("upper")
  )

  val builtin: FunctionRegistry = {
    val fr = new SimpleFunctionRegistry
    expressions.foreach { case (name, builder) => fr.registerFunction(name, builder) }
    fr
  }

  /** See usage above. */
  private def expression[T <: Expression](name: String)
      (implicit tag: ClassTag[T]): (String, FunctionBuilder) = {
    // Use the companion class to find apply methods.
    val objectClass = Class.forName(tag.runtimeClass.getName + "$")
    val companionObj = objectClass.getDeclaredField("MODULE$").get(null)

    // See if we can find an apply that accepts Seq[Expression]
    val varargApply = Try(objectClass.getDeclaredMethod("apply", classOf[Seq[_]])).toOption

    val builder = (expressions: Seq[Expression]) => {
      if (varargApply.isDefined) {
        // If there is an apply method that accepts Seq[Expression], use that one.
        varargApply.get.invoke(companionObj, expressions).asInstanceOf[Expression]
      } else {
        // Otherwise, find an apply method that matches the number of arguments, and use that.
        val params = Seq.fill(expressions.size)(classOf[Expression])
        val f = Try(objectClass.getDeclaredMethod("apply", params : _*)) match {
          case Success(e) =>
            e
          case Failure(e) =>
            throw new AnalysisException(s"Invalid number of arguments for function $name")
        }
        f.invoke(companionObj, expressions : _*).asInstanceOf[Expression]
      }
    }
    (name, builder)
  }
}
