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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

/**
 * For lazy computing, be sure the generator.terminate() called in the very last
 * TODO reusing the CompletionIterator?
 */
private[execution] sealed case class LazyIterator(func: () => TraversableOnce[InternalRow])
  extends Iterator[InternalRow] {

  lazy val results = func().toIterator
  override def hasNext: Boolean = results.hasNext
  override def next(): InternalRow = results.next()
}

/**
 * Applies a [[Generator]] to a stream of input rows, combining the
 * output of each into a new stream of rows.  This operation is similar to a `flatMap` in functional
 * programming with one important additional feature, which allows the input rows to be joined with
 * their output.
 *
 * @param generator the generator expression
 * @param join  when true, each output row is implicitly joined with the input tuple that produced
 *              it.
 * @param outer when true, each input row will be output at least once, even if the output of the
 *              given `generator` is empty. `outer` has no effect when `join` is false.
 * @param output the output attributes of this node, which constructed in analysis phase,
 *               and we can not change it, as the parent node bound with it already.
 */
case class GenerateExec(
    generator: Generator,
    join: Boolean,
    outer: Boolean,
    output: Seq[Attribute],
    child: SparkPlan)
  extends UnaryExecNode with CodegenSupport {

  private[sql] override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def producedAttributes: AttributeSet = AttributeSet(output)

  val boundGenerator = BindReferences.bindReference(generator, child.output)

  protected override def doExecute(): RDD[InternalRow] = {
    // boundGenerator.terminate() should be triggered after all of the rows in the partition
    val rows = if (join) {
      child.execute().mapPartitionsInternal { iter =>
        val generatorNullRow = new GenericInternalRow(generator.elementSchema.length)
        val joinedRow = new JoinedRow

        iter.flatMap { row =>
          // we should always set the left (child output)
          joinedRow.withLeft(row)
          val outputRows = boundGenerator.eval(row)
          if (outer && outputRows.isEmpty) {
            joinedRow.withRight(generatorNullRow) :: Nil
          } else {
            outputRows.map(joinedRow.withRight)
          }
        } ++ LazyIterator(boundGenerator.terminate).map { row =>
          // we leave the left side as the last element of its child output
          // keep it the same as Hive does
          joinedRow.withRight(row)
        }
      }
    } else {
      child.execute().mapPartitionsInternal { iter =>
        iter.flatMap(boundGenerator.eval) ++ LazyIterator(boundGenerator.terminate)
      }
    }

    val numOutputRows = longMetric("numOutputRows")
    rows.mapPartitionsInternal { iter =>
      val proj = UnsafeProjection.create(output, output)
      iter.map { r =>
        numOutputRows += 1
        proj(r)
      }
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    // We need to add some code here for terminating generators.
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    boundGenerator match {
      case e: Explode => codegen(e.child, ctx, input, row)
      case e => codegen(e, ctx, input, row)
    }
  }

  private def codegen(
      e: Expression,
      ctx: CodegenContext,
      input: Seq[ExprCode],
      row: ExprCode): String = {
    ctx.currentVars = input
    ctx.copyResult = true

    // Generate the driving expression.
    val data = e.genCode(ctx)

    // Generate looping variables.
    val numOutput = metricTerm(ctx, "numOutputRows")
    val index = ctx.freshName("index")
    val numElements = ctx.freshName("numElements")

    // Generate accessor for MapData/Array element(s).
    val outerCheck = optionalCode(outer, s"$numElements == 0")
    def accessor(source: String, name: String, dt: DataType, nullable: Boolean): ExprCode = {
      val value = ctx.freshName(name)
      val javaType = ctx.javaType(dt)
      val getter = ctx.getValue(source, dt, index)
      val checks = outerCheck ++ optionalCode(nullable, s"$source.isNullAt($index)")
      if (checks.nonEmpty) {
        val isNull = ctx.freshName("isNull")
        val code =
          s"""
             |boolean $isNull = ${checks.mkString(" || ")};
             |$javaType $value = $isNull ? ${ctx.defaultValue(dt)} : $getter;
           """.stripMargin
        ExprCode(code, isNull, value)
      } else {
        ExprCode(s"$javaType $value = $getter;", "false", value)
      }
    }

    val (initArrayData, initValues, values) = e.dataType match {
        /*
      case ArrayType(st: StructType, nullable) if output.size > 1 =>
        val rowCode = accessor("", "col", st, nullable)
        st.fields.map { f =>

        }
        ("", rowCode.code, Seq.empty[ExprCode])
*/
      case ArrayType(dataType, nullable) =>
        ("", "", Seq(accessor(data.value, "col", dataType, nullable)))

      case MapType(keyType, valueType, valueContainsNull) =>
        // Materialize the key and the value array before we enter the loop.
        val keyArray = ctx.freshName("keyArray")
        val valueArray = ctx.freshName("valueArray")
        val initArrayData =
          s"""
             |ArrayData $keyArray = ${data.isNull} ? null : ${data.value}.keyArray();
             |ArrayData $valueArray = ${data.isNull} ? null : ${data.value}.valueArray();
           """.stripMargin
        val values = Seq(
          accessor(keyArray, "key", keyType, nullable = false),
          accessor(valueArray, "value", valueType, valueContainsNull))
        (initArrayData, "", values)
    }

    // Determine result vars.
    val outputValues = if (join) {
      input ++ values
    } else {
      values
    }

    // In case of outer we need to make sure the loop is executed at-least once when the array/map
    // contains no input. We do this by setting the looping index to -1 if there is no input,
    // evaluation of the array is prevented by a check in the accessor code.
    val init = if (outer) s"$numElements == 0 ? -1 : 0" else "0"
    s"""
       |${data.code}
       |$initArrayData
       |int $numElements = ${data.isNull} ? 0 : ${data.value}.numElements();
       |for (int $index = $init; $index < $numElements; $index++) {
       |  $numOutput.add(1);
       |  $initValues
       |  ${consume(ctx, outputValues)}
       |}
     """.stripMargin
  }

  private def optionalCode(condition: Boolean, code: => String): Seq[String] = {
    if (condition) Seq(code)
    else Seq.empty
  }
}
