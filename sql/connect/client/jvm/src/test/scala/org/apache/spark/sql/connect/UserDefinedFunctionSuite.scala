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
package org.apache.spark.sql.connect

import scala.reflect.runtime.universe.typeTag

import org.apache.spark.{SparkException, SparkIllegalArgumentException}
import org.apache.spark.sql.{Column, Encoder}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.PrimitiveLongEncoder
import org.apache.spark.sql.connect.common.UdfPacket
import org.apache.spark.sql.connect.test.ConnectFunSuite
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedAggregator}
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.util.SparkSerDeUtils

class UserDefinedFunctionSuite extends ConnectFunSuite {

  test("udf and encoder serialization") {
    def func(x: Int): Int = x + 1

    val dummyCol = Column("dummy")
    val myUdf = udf(func _)
    val colWithUdf = myUdf(dummyCol)

    val udfExpr = toExpr(colWithUdf).getCommonInlineUserDefinedFunction
    assert(udfExpr.getDeterministic)
    assert(udfExpr.getArgumentsCount == 1)
    assert(udfExpr.getArguments(0) == toExpr(dummyCol))
    val udfObj = udfExpr.getScalarScalaUdf

    assert(!udfObj.getNullable)

    val deSer = SparkSerDeUtils.deserialize[UdfPacket](udfObj.getPayload.toByteArray)

    assert(deSer.function.asInstanceOf[Int => Int](5) == func(5))
    assert(deSer.outputEncoder == ScalaReflection.encoderFor(typeTag[Int]))
    assert(deSer.inputEncoders == Seq(ScalaReflection.encoderFor(typeTag[Int])))
  }

  private def testNonDeserializable(f: Int => Int): Unit = {
    val e = intercept[SparkException](toExpr(udf(f).apply(lit(1))))
    assert(
      e.getMessage.contains(
        "UDF cannot be executed on a Spark cluster: it cannot be deserialized."))
    assert(e.getMessage.contains("This is not supported by java serialization."))
  }

  test("non deserializable UDFs") {
    testNonDeserializable(Command2(Command1()).indirect)
    testNonDeserializable(MultipleLambdas().indirect)
    testNonDeserializable(SelfRef(22).method)
  }

  test("serializable UDFs") {
    val direct = (i: Int) => i + 1
    val indirect = (i: Int) => direct(i)
    udf(indirect)
    udf(Command1().direct)
    udf(MultipleLambdas().direct)
  }

  test("Connect UDF rowOrderIndependent chaining reset and UDAF rejection") {
    val f = udf((x: Int) => x + 1)
    assert(f.deterministic && f.rowOrderIndependent)

    val rowIndep = f.asRowOrderIndependentNondeterministic()
    assert(!rowIndep.deterministic && rowIndep.rowOrderIndependent)
    val rowIndepProto = toExpr(rowIndep(Column("a"))).getCommonInlineUserDefinedFunction
    assert(!rowIndepProto.getDeterministic)
    assert(rowIndepProto.hasRowOrderIndependent && rowIndepProto.getRowOrderIndependent)

    val reset = rowIndep.asNondeterministic()
    assert(!reset.deterministic && !reset.rowOrderIndependent)
    val resetProto = toExpr(reset(Column("a"))).getCommonInlineUserDefinedFunction
    assert(!resetProto.getDeterministic)
    assert(!resetProto.hasRowOrderIndependent)

    val agg = new Aggregator[Long, Long, Long] {
      override def zero: Long = 0L
      override def reduce(b: Long, a: Long): Long = a + b
      override def merge(b1: Long, b2: Long): Long = b1 + b2
      override def finish(reduction: Long): Long = reduction
      override def bufferEncoder: Encoder[Long] = PrimitiveLongEncoder
      override def outputEncoder: Encoder[Long] = PrimitiveLongEncoder
    }
    val uda = UserDefinedAggregator(agg, PrimitiveLongEncoder).withName("bad_uda")
    val ex = intercept[SparkIllegalArgumentException] {
      uda.asRowOrderIndependentNondeterministic()
    }
    assert(ex.getCondition === "UNSUPPORTED_FEATURE.ROW_ORDER_INDEPENDENT_AGGREGATOR")
    assert(ex.getMessageParameters.get("functionName") === "bad_uda")
  }
}

case class Command1() extends Serializable {
  val direct: Int => Int = (i: Int) => i + 1
}

case class Command2(prev: Command1) extends Serializable {
  val indirect: Int => Int = (i: Int) => prev.direct(i)
}

case class SelfRef(start: Int) extends Serializable {
  val method: Int => Int = (i: Int) => i + start
}

case class MultipleLambdas() extends Serializable {
  val direct: Int => Int = (i: Int) => i + 1
  val indirect: Int => Int = (i: Int) => direct(i)
}
