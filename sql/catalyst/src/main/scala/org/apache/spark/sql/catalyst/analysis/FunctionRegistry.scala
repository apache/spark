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

import java.lang.reflect.Constructor

import scala.reflect.ClassTag

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.StringKeyHashMap


/** A catalog for looking up user defined functions, used by an [[Analyzer]]. */
trait FunctionRegistry {

  def registerFunction(name: String, builder: FunctionBuilder): Unit

  def lookupFunction(name: String, children: Seq[Expression]): Expression

  def conf: CatalystConf
}


object FunctionRegistry {

  type FunctionBuilder = Seq[Expression] => Expression

  val expressions: Map[String, FunctionBuilder] = Map(
    // Non aggregate functions
    expression[Abs]("abs"),
    expression[CreateArray]("array"),
    expression[Coalesce]("coalesce"),
    expression[Explode]("explode"),
    expression[Lower]("lower"),
    expression[Substring]("substr"),
    expression[Substring]("substring"),
    expression[Rand]("rand"),
    expression[Randn]("randn"),
    expression[CreateStruct]("struct"),
    expression[Sqrt]("sqrt"),
    expression[Upper]("upper"),

    // Math functions
    expression[Acos]("acos"),
    expression[Asin]("asin"),
    expression[Atan]("atan"),
    expression[Atan2]("atan2"),
    expression[Cbrt]("cbrt"),
    expression[Ceil]("ceil"),
    expression[Cos]("cos"),
    expression[Exp]("exp"),
    expression[Expm1]("expm1"),
    expression[Floor]("floor"),
    expression[Hypot]("hypot"),
    expression[Log]("log"),
    expression[Log10]("log10"),
    expression[Log1p]("log1p"),
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
    expression[Sum]("sum")
  )

  /** See usage above. */
  private def expression[T <: Expression](name: String)
      (implicit tag: ClassTag[T]): (String, FunctionBuilder) = {
    val constructors = tag.runtimeClass.getDeclaredConstructors.toSeq
    val builder = (expressions: Seq[Expression]) => {
      // First see if there is a constructor that accepts a Seq[Expression]
      val varargCtor: Option[Constructor[_]] = constructors.find { ctor =>
        val params = ctor.getParameterTypes
        params.length == 1 && params(0) == classOf[Seq[_]]
      }

      if (varargCtor.isDefined) {
        // If there is a constructor that accepts Seq[Expression], use that one.
        varargCtor.get.newInstance(expressions).asInstanceOf[Expression]
      } else {
        // Otherwise, find a constructor that matches the number of arguments, and use that.
        val ctor = constructors.find { ctor =>
          val params = ctor.getParameterTypes
          val valid = params.forall(classOf[Expression].isAssignableFrom)
          params.length == expressions.size && valid
        }.getOrElse(throw new AnalysisException(s"Invalid number of arguments for function $name"))

        ctor.newInstance(expressions : _*).asInstanceOf[Expression]
      }
    }
    (name, builder)
  }
}


trait OverrideFunctionRegistry extends FunctionRegistry {

  private val functionBuilders = StringKeyHashMap[FunctionBuilder](conf.caseSensitiveAnalysis)

  override def registerFunction(name: String, builder: FunctionBuilder): Unit = {
    functionBuilders.put(name, builder)
  }

  abstract override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    functionBuilders.get(name).map(_(children)).getOrElse(super.lookupFunction(name, children))
  }
}

class SimpleFunctionRegistry(val conf: CatalystConf) extends FunctionRegistry {

  private val functionBuilders = StringKeyHashMap[FunctionBuilder](conf.caseSensitiveAnalysis)

  override def registerFunction(name: String, builder: FunctionBuilder): Unit = {
    functionBuilders.put(name, builder)
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    functionBuilders(name)(children)
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

  override def conf: CatalystConf = throw new UnsupportedOperationException
}
