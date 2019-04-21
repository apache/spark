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

package org.apache.spark.sql.bytecode

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.execution.{DeserializeToObjectExec, FilterExec, ObjectConsumerExec, ObjectProducerExec, ProjectExec, SerializeFromObjectExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext


class BytecodeAnalysisSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  // TODO: the support for arithmetic operations must be revisited because of edge cases
  test("map transformations with basic arithmetic operations") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val byteDS = Seq(1.toByte, 2.toByte).toDS()
        .map(b => (b + 1).toByte)
        .map(b => (b - 6).toByte)
        .map(b => (b * 1).toByte)
        .map(b => (b / 2).toByte)
        .map(b => (b % 2).toByte)
      val bytePlan = byteDS.queryExecution.executedPlan
      // TODO: the field `value` is nullable as Divide is always nullable
      // we break the schema here
      // assert(bytePlan.schema == byteDS.schema)
      assertNoTypedOperations(bytePlan)
      assert(bytePlan.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(byteDS, 0.toByte, -1.toByte)

      val shortDS = Seq(1.toShort, 2.toShort).toDS()
        .map(s => (s + 1).toShort)
        .map(s => (s - 6).toShort)
        .map(s => (s * 1).toShort)
        .map(s => (s / 2).toShort)
        .map(s => (s % 2).toShort)
      val shortPlan = shortDS.queryExecution.executedPlan
      // TODO: the field `value` is nullable as Divide is always nullable
      // we break the schema here
      // assert(shortPlan.schema == shortDS.schema)
      assertNoTypedOperations(shortPlan)
      assert(shortPlan.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(shortDS, 0.toShort, -1.toShort)

      val intDS = Seq(1, 2).toDS()
        .map(i => i + 1)
        .map(i => i - 6)
        .map(i => i * 1)
        .map(i => i / 2)
        .map(i => i % 2)
      val intPlan = intDS.queryExecution.executedPlan
      // TODO: the field `value` is nullable as Divide is always nullable
      // we break the schema here
      // assert(intPlan.schema == intDS.schema)
      assertNoTypedOperations(intPlan)
      assert(intPlan.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(intDS, 0, -1)

      val longDS = spark.range(1, 3).as[Long]
        .map(l => l + 1)
        .map(l => l - 6)
        .map(l => l * 1)
        .map(l => l / 2)
        .map(l => l % 2)
      val longPlan = longDS.queryExecution.executedPlan
      // TODO: the field `value` is nullable as Divide is always nullable
      // we break the schema here
      // assert(longPlan.schema == longDS.schema)
      assertNoTypedOperations(longPlan)
      assert(longPlan.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(longDS, 0L, -1L)

      val floatDS = Seq(1.0F, 2.0F).toDS()
        .map(f => f + 1)
        .map(f => f - 6)
        .map(f => f * 1)
        .map(f => f / 2)
        .map(f => f % 2)
      val floatPlan = floatDS.queryExecution.executedPlan
      // TODO: the field `value` is nullable as Divide is always nullable
      // we break the schema here
      // assert(floatPlan.schema == floatDS.schema)
      assertNoTypedOperations(floatPlan)
      assert(floatPlan.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(floatDS, -0.0F, -1.5F)

      val doubleDS = Seq(1.0, 2.0).toDS()
        .map(d => d + 1)
        .map(d => d - 6)
        .map(d => d * 1)
        .map(d => d / 2)
        .map(d => d % 2)
      val doublePlan = doubleDS.queryExecution.executedPlan
      // TODO: the field `value` is nullable as Divide is always nullable
      // we break the schema here
      // assert(doublePlan.schema == doubleDS.schema)
      assertNoTypedOperations(doublePlan)
      assert(doublePlan.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(doubleDS, -0.0, -1.5)
    }

    // TODO: other types
  }

  test("map transformations with mixed types") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds1 = Seq(1, 2).toDS()
        .map(v => v + 1.5)
        .filter(v => v > 3)
      val plan1 = ds1.queryExecution.executedPlan
      assertNoTypedOperations(plan1)
      checkDataset(ds1, 3.5)

      // TODO: more cases
    }
  }

  test("filters with basic arithmetic operations") {
    // TODO
  }

  // TODO: this one is broken as edge cases are not handled
  ignore("edge cases for arithmetic operations") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {

      // TODO: 1 / 0 -> An arithmetic exception
      val byteDS = Seq(1.toByte, 2.toByte).toDS()
      byteDS.map(b => b / 0).show()

      // TODO: 1 / 0.0 -> Infinity
      val doubleDS = Seq(1.0, 2.0).toDS()
      doubleDS.map(d => d / 0.0).show()

      // TODO: other edge cases
    }
  }

  test("map transformations with java.lang.Long") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds1 = Seq(java.lang.Long.valueOf(1L), java.lang.Long.valueOf(3L)).toDS()
        .map { l => JavaLongWrapper(l) }
        .map { w =>
          val oldNumber: java.lang.Long = w.number
          val additionalValue = new java.lang.Long(3)
          val finalValue = w.add(oldNumber, additionalValue)
          JavaLongWrapper(finalValue)
        }
      val plan1 = ds1.queryExecution.executedPlan
      assertNoTypedOperations(plan1)
      checkDataset(ds1, JavaLongWrapper(5L), JavaLongWrapper(9L))

      val ds2 = Seq(java.lang.Long.valueOf(1L), java.lang.Long.valueOf(3L)).toDS()
        .map { l => JavaLongWrapper(l) }
        .map { w =>
          val oldNumber: java.lang.Long = w.number
          val primitiveValue: Long = 3L
          val additionalValue = w.number + primitiveValue
          val finalValue = w.add(oldNumber, additionalValue)
          JavaLongWrapper(finalValue)
        }
      val plan2 = ds2.queryExecution.executedPlan
      assertNoTypedOperations(plan2)
      checkDataset(ds2, JavaLongWrapper(6L), JavaLongWrapper(12L))

      // TODO: call methods on java.lang.Long
    }
  }

  test("map transformations with String") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds1 = Seq(1L, 2L).toDS()
        .map(l => l.toString)
        .map(s => s.concat("-XXX"))
      val plan1 = ds1.queryExecution.executedPlan
      assertNoTypedOperations(plan1)
      checkDataset(ds1, "1-XXX", "2-XXX")

      val ds2 = Seq(1L, 4L).toDS()
        .map { l =>
          val b = if (l > 2) "" else l.toString
          b.toString
        }
      val plan2 = ds2.queryExecution.executedPlan
      assertNoTypedOperations(plan2)
      checkDataset(ds2, "1", "")

      // TODO: more cases
    }
  }

  test("map transformations with case classes") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds1 = Seq(1L, 2L).toDS()
        .map(l => Event(l, l.toString))
        .map(e => Data(e.l))
      val plan1 = ds1.queryExecution.executedPlan
      assert(plan1.schema == ds1.schema)
      assertNoTypedOperations(plan1)
      assert(plan1.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(ds1, Data(1), Data(2))

      val ds2 = Seq(Event(1L, "1L")).toDS()
        .map(e => Data(e.l))
        .map(d => Event(d.l, d.l.toString))
      val plan2 = ds2.queryExecution.executedPlan
      // TODO: we derive a more precise schema in this case
      // assert(plan2.schema == ds2.schema)
      assertNoTypedOperations(plan2)
      assert(plan2.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(ds2, Event(1L, "1"))

      val ds3 = Seq(1, 2).toDS()
        .map { i =>
          val value = if (i == 1) Data(-1L) else Data(0L)
          value.l
        }
      val plan3 = ds3.queryExecution.executedPlan
      assert(plan3.schema == ds3.schema)
      assertNoTypedOperations(plan3)
      checkDataset(ds3, -1L, 0L)
    }
  }

  test("map transformations with nested data") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds1 = Seq(1L, 2L).toDS()
        .map { l => NestedData(l.toString, Data(l)) }
        .map { nd => nd.data.l }
      val plan1 = ds1.queryExecution.executedPlan
      assert(plan1.schema == ds1.schema)
      assertNoTypedOperations(plan1)
      checkDataset(ds1, 1L, 2L)

      val ds2 = Seq(1L, 2L).toDS()
        .map { l => NestedData(l.toString, Data(l)) }
        .map { nd => Event(nd.data.l, nd.name) }
      val plan2 = ds2.queryExecution.executedPlan
      // TODO: we derive a more precise schema in this case
      // assert(plan2.schema == q2.schema)
      assertNoTypedOperations(plan2)
      assert(plan2.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(ds2, Event(1, "1"), Event(2, "2"))

      val ds3 = Seq(1L, 2L).toDS()
        .map { l => NestedData(l.toString, Data(l)) }
        .map { nd => DeeplyNestedData(10, nd) }
        .map { dnd => dnd.nestedData.data.l }
      val plan3 = ds3.queryExecution.executedPlan
      assert(plan3.schema == ds3.schema)
      assertNoTypedOperations(plan3)
      checkDataset(ds3, 1L, 2L)
    }
  }

  test("filters with case classes") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds1 = Seq(1L, 2L).toDS()
        .map(l => Data(l))
        .filter(_.l > 1L)
      val plan1 = ds1.queryExecution.executedPlan
      assert(plan1.schema == ds1.schema)
      assertNoTypedOperations(plan1)
      checkDataset(ds1, Data(2L))

      // TODO: more cases
    }
  }

  // TODO: nulls are not properly handled
  ignore("null handling") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds1 = Seq(java.lang.Long.valueOf(1L)).toDS()
        .map { _ =>
          val longValue: java.lang.Long = null
          longValue.toString
        }
      ds1.collect()

      val ds2 = spark.range(1L, 10L).as[Long]
        .map(l => Event(l, null))
        .map(event => event.s.concat("suffix"))
      val plan2 = ds2.queryExecution.executedPlan
      assert(plan2.schema == ds2.schema)
      assertNoTypedOperations(plan2)
      val thrownException2 = intercept[SparkException] {
        ds2.collect()
      }
      assert(thrownException2.getCause.isInstanceOf[NullPointerException])

      val ds3 = spark.range(1L, 10L).as[Long]
        .map { l =>
          val e: Event = null
          e.doSmth()
          Event(l, l.toString)
        }
      // TODO: this throws an analysis exception without execution
      // val plan3 = ds3.queryExecution.executedPlan
      // assert(plan3.schema == ds3.schema)
      val thrownException3 = intercept[AnalysisException] {
        ds3.collect()
      }
      assert(thrownException3.message == "Calling 'doSmth' on a null reference")

      val ds4 = spark.range(1L, 10L).as[Long]
        .map { l =>
          val e: Event = EventHelper.nullEvent()
          e.copy(l = l + 1)
        }
      // TODO: this throws an analysis exception without execution
      // val plan4 = ds4.queryExecution.executedPlan
      // assert(plan4.schema == ds4.schema)
      val thrownException4 = intercept[AnalysisException] {
        ds4.collect()
      }
      assert(thrownException4.message == "Calling 'copy$default$2' on a null reference")

      val ds5 = spark.range(1L, 10L).as[Long]
        .map { l =>
          val e = Event(l, l.toString)
          val newE = e.generateNull()
          newE.copy(l = l + 1)
        }
      // TODO: this throws an analysis exception without execution
      // val plan5 = ds5.queryExecution.executedPlan
      // assert(plan5.schema == ds5.schema)
      val thrownException5 = intercept[AnalysisException] {
        ds5.collect()
      }
      assert(thrownException5.message == "Calling 'copy$default$2' on a null reference")

      val ds6 = spark.range(1L, 10L).as[Long]
        .map { l =>
          if (l % 2 == 0) NestedData(l.toString, Data(l)) else NestedData(l.toString, null)
        }
        .map { nd => Event(nd.data.l, nd.name) }
      val plan6 = ds6.queryExecution.executedPlan
      assert(plan6.schema == ds6.schema)
      assertNoTypedOperations(plan6)
      val thrownException6 = intercept[SparkException] {
        ds6.collect()
      }
      assert(thrownException6.getCause.isInstanceOf[NullPointerException])

      val ds7 = Seq(java.lang.Long.valueOf(1L), java.lang.Long.valueOf(2L)).toDS()
        .map { l => if (l == 1) l else null }
        .map { l => l.toString }
      ds7.explain(true)
      ds7.show()
    }
  }

  test("no additional serialization is introduced") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds1 = spark.range(1, 10).as[Long]
        .map { l => println("unsupported"); l + 1 } // scalastyle:ignore
        .map { l => l + 6 }
        .map { l => println("unsupported"); l + 5 } // scalastyle:ignore
      val plan1 = ds1.queryExecution.executedPlan
      assert(plan1.collect { case d: DeserializeToObjectExec => d }.size == 1)
      assert(plan1.collect { case s: SerializeFromObjectExec => s }.size == 1)
      assert(plan1.collect { case p: ProjectExec => p }.isEmpty)

      val ds2 = spark.range(1, 10).as[Long]
        .map { l => println("unsupported"); l + 1 } // scalastyle:ignore
        .filter(_ > 4)
        .map { l => println("unsupported"); l + 5 } // scalastyle:ignore
      val plan2 = ds2.queryExecution.executedPlan
      assert(plan2.collect { case d: DeserializeToObjectExec => d }.size == 1)
      assert(plan2.collect { case s: SerializeFromObjectExec => s }.size == 1)
      assert(plan2.collect { case f: FilterExec if f.condition.isInstanceOf[Invoke] => f }.nonEmpty)

      val ds3 = spark.range(1, 10).as[Long]
        .map { l => println("unsupported"); l + 1 } // scalastyle:ignore
        .filter(_ > 4)
      val plan3 = ds3.queryExecution.executedPlan
      assert(plan3.collect { case d: DeserializeToObjectExec => d }.size == 1)
      assert(plan3.collect { case s: SerializeFromObjectExec => s }.size == 1)
      assert(plan3.collect { case f: FilterExec if f.condition.isInstanceOf[Invoke] => f }.isEmpty)

      val ds4 = spark.range(1, 10).as[Long]
        .map { l => println("unsupported"); Event(l, l.toString) } // scalastyle:ignore
        .map { event => Event(event.l + 1, event.s) }
        .map { event => println("unsupported"); event.l } // scalastyle:ignore
      val plan4 = ds4.queryExecution.executedPlan
      assert(plan4.collect { case d: DeserializeToObjectExec => d }.size == 1)
      assert(plan4.collect { case s: SerializeFromObjectExec => s }.size == 1)
      assert(plan4.collect { case p: ProjectExec => p }.isEmpty)
    }
  }

  test("invokeinterface") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds1 = Seq(1L, 2L).toDS()
        .map { _ =>
          val obj: BaseTrait = ChildObject
          obj.number
        }
      val plan1 = ds1.queryExecution.executedPlan
      assert(plan1.schema == ds1.schema)
      assertNoTypedOperations(plan1)
      checkDataset(ds1, 2, 2)

      val func = (b: BaseTrait) => b.number
      val ds2 = Seq(ChildClass(1), ChildClass(0)).toDS()
        .map(func)
      val plan2 = ds2.queryExecution.executedPlan
      assert(plan2.schema == ds2.schema)
      assertNoTypedOperations(plan2)
      checkDataset(ds2, 1, 0)
    }
  }

  test("invokevirtual") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds1 = spark.range(1L, 3L)
        .map { v =>
          val obj: Object = JavaLongWrapper(java.lang.Long.valueOf(v))
          obj.toString
        }
      val plan1 = ds1.queryExecution.executedPlan
      // TODO: we derive a more precise schema in this case
      // assert(plan1.schema == ds1.schema)
      assertNoTypedOperations(plan1)
      checkDataset(ds1, "XXX", "XXX")
    }
  }

  test("outer variables") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      // TODO: decide what to do with outer variables
      val outerObj = ChildObject
      val ds1 = spark.range(1L, 3L)
        .map { l => l + outerObj.number }
      val plan1 = ds1.queryExecution.executedPlan
      assert(plan1.collect { case p: ProjectExec => p } == Nil)
    }
  }

  test("if statements") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds1 = Seq(1, 2, 3).toDS()
        .map { l => if (l == 2) -2 else if (l == 1) -1 else 0 }
      val plan1 = ds1.queryExecution.executedPlan
      assertNoTypedOperations(plan1)
      checkDataset(ds1, -1, -2, 0)

      val ds2 = Seq(1, 2, 3).toDS()
        .map { l =>
          val value = if (l == 2) -2 else if (l == 1) -1 else 0
          val expr = ChildObject.number
          Data(value.toLong + expr)
        }
        .map { data => data.l }
      val plan2 = ds2.queryExecution.executedPlan
      assertNoTypedOperations(plan2)
      checkDataset(ds2, 1L, 0L, 2L)

      val ds3 = Seq(1, 2, 3).toDS()
        .map { l =>
          if (l > 2) {
            val event = Event(100, "CASE_1")
            val newResult = event.multiply(2)
            Event(newResult, event.s)
          } else {
            val event = Event(-50, "CASE_2")
            val newResult = event.multiply(-3)
            Event(newResult, event.s)
          }
        }
      val plan3 = ds3.queryExecution.executedPlan
      assertNoTypedOperations(plan3)
      checkDataset(ds3, Event(150, "CASE_2"), Event(150, "CASE_2"), Event(200, "CASE_1"))
    }
  }

  // TODO: support tableswitch and lookupswitch in case of attributes
  ignore("pattern matching") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds1 = Seq(1, 2, 3).toDS()
        .map { l =>
          val strValue = l match {
            case 1 => "100"
            case 2 => "200"
            case 3 => "300"
            case _ => "XXX"
          }
          Event(l, strValue)
        }
      val plan1 = ds1.queryExecution.executedPlan
      assertNoTypedOperations(plan1)

      val ds2 = Seq(1, 2, 3).toDS()
        .map { l =>
          val number = ChildObject.number
          val strValue = l match {
            case 1 if number == 1 => "100"
            case 2 if number == 2 => "200"
            case 3 if number == 3 => "300"
            case _ => "XXX"
          }
          Event(l, strValue)
        }
      val plan2 = ds2.queryExecution.executedPlan
      assertNoTypedOperations(plan2)
    }
  }

  test("case classes with collections") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val a1 = Address(1, "Cody Way", "San Jose", "CA", "95112")
      val a2 = Address(2, "Parkwood", "New York", "NY", "95014")

      val h1 = Home(a1, Facts(140, 1400, 1982), List())
      val h2 = Home(a2, Facts(150, 1100, 1983), List())

      val ds1 = Seq(h1, h2).toDS()
        .map(_.address)
      val plan1 = ds1.queryExecution.executedPlan
      // TODO: houseNumber becomes nullable after the conversion
      // assert(plan1.schema == ds1.schema)
      assertNoTypedOperations(plan1)
      checkDataset(ds1, a1, a2)

      val ds2 = Seq(h1, h2).toDS()
        .map(_.address.city)
      val plan2 = ds2.queryExecution.executedPlan
      assert(plan2.schema == ds2.schema)
      assertNoTypedOperations(plan2)
      checkDataset(ds2, "San Jose", "New York")

      val ds3 = Seq(h1, h2).toDS()
        .filter(_.address.city == "San Jose")
        .map(_.address.state)
      val plan3 = ds3.queryExecution.executedPlan
      assert(plan3.schema == ds3.schema)
      assertNoTypedOperations(plan3)
      checkDataset(ds3, "CA")
    }
  }

  // TODO: more tests for method calls

  private def assertNoTypedOperations(plan: SparkPlan): Unit = {
    val typedOperations = plan.collect {
      case p: ObjectProducerExec => p
      case c: ObjectConsumerExec => c
      case f @ FilterExec(_: Invoke, _) => f
    }
    assert(typedOperations.isEmpty)
  }
}

case class Address(
    houseNumber: Int,
    streetAddress: String,
    city: String,
    state: String,
    zipCode: String)

case class Facts(price: Int, size: Int, yearBuilt: Int)

case class School(name: String)

case class Home(address: Address, facts: Facts, schools: List[School])

case class Data(l: Long)
case class NestedData(name: String, data: Data)
case class DeeplyNestedData(index: Int, nestedData: NestedData)

case class Event(l: Long, s: String) {
  def doSmth(): Long = l * 2
  def generateNull(): Event = null
  def voidFunc(): Unit = {}
  def multiply(times: Int): Long = {
    times * l
  }
}

object EventHelper {
  def nullEvent(): Event = null
}

trait BaseTrait {
  def number: Int = 4
}
object ChildObject extends BaseTrait {
  override def number: Int = 2
}
case class ChildClass(value: Int) extends BaseTrait {
  override def number: Int = value
}

case class JavaLongWrapper(number: java.lang.Long) {
  def add(firstNumber: java.lang.Long, secondNumber: java.lang.Long): java.lang.Long = {
    number + firstNumber + secondNumber
  }

  override def toString: String = "XXX"
}
