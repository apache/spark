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

  override lazy val metrics = Map(
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
    ctx.currentVars = input
    ctx.copyResult = true

    // Add input rows to the values when we are joining
    val values = if (join) {
      input
    } else {
      Seq.empty
    }

    // Generate the driving expression.
    val data = boundGenerator.genCode(ctx)

    boundGenerator match {
      case e: CollectionGenerator => codeGenCollection(ctx, e, values, data, row)
      case g => codeGenTraversableOnce(ctx, g, values, data, row)
    }
  }

  /**
   * Generate code for [[CollectionGenerator]] expressions.
   */
  private def codeGenCollection(
      ctx: CodegenContext,
      e: CollectionGenerator,
      input: Seq[ExprCode],
      data: ExprCode,
      row: ExprCode): String = {

    // Generate looping variables.
    val index = ctx.freshName("index")

    // Add a check if the generate outer flag is true.
    val checks = optionalCode(outer, data.isNull)

    // Add position
    val position = if (e.position) {
      Seq(ExprCode("", "false", index))
    } else {
      Seq.empty
    }

    // Generate code for either ArrayData or MapData
    val (initMapData, updateRowData, values) = e.collectionSchema match {
      case ArrayType(st: StructType, nullable) if e.inline =>
        val row = codeGenAccessor(ctx, data.value, "col", index, st, nullable, checks)
        val fieldChecks = checks ++ optionalCode(nullable, row.isNull)
        val columns = st.fields.toSeq.zipWithIndex.map { case (f, i) =>
          codeGenAccessor(ctx, row.value, f.name, i.toString, f.dataType, f.nullable, fieldChecks)
        }
        ("", row.code, columns)

      case ArrayType(dataType, nullable) =>
        ("", "", Seq(codeGenAccessor(ctx, data.value, "col", index, dataType, nullable, checks)))

      case MapType(keyType, valueType, valueContainsNull) =>
        // Materialize the key and the value arrays before we enter the loop.
        val keyArray = ctx.freshName("keyArray")
        val valueArray = ctx.freshName("valueArray")
        val initArrayData =
          s"""
             |ArrayData $keyArray = ${data.isNull} ? null : ${data.value}.keyArray();
             |ArrayData $valueArray = ${data.isNull} ? null : ${data.value}.valueArray();
           """.stripMargin
        val values = Seq(
          codeGenAccessor(ctx, keyArray, "key", index, keyType, nullable = false, checks),
          codeGenAccessor(ctx, valueArray, "value", index, valueType, valueContainsNull, checks))
        (initArrayData, "", values)
    }

    // In case of outer=true we need to make sure the loop is executed at-least once when the
    // array/map contains no input. We do this by setting the looping index to -1 if there is no
    // input, evaluation of the array is prevented by a check in the accessor code.
    val numElements = ctx.freshName("numElements")
    val init = if (outer) s"$numElements == 0 ? -1 : 0" else "0"
    val numOutput = metricTerm(ctx, "numOutputRows")
    s"""
       |${data.code}
       |$initMapData
       |int $numElements = ${data.isNull} ? 0 : ${data.value}.numElements();
       |for (int $index = $init; $index < $numElements; $index++) {
       |  $numOutput.add(1);
       |  $updateRowData
       |  ${consume(ctx, input ++ position ++ values)}
       |}
     """.stripMargin
  }

  /**
   * Generate code for a regular [[TraversableOnce]] returning [[Generator]].
   */
  private def codeGenTraversableOnce(
      ctx: CodegenContext,
      e: Expression,
      input: Seq[ExprCode],
      data: ExprCode,
      row: ExprCode): String = {

    // Generate looping variables.
    val iterator = ctx.freshName("iterator")
    val hasNext = ctx.freshName("hasNext")
    val current = ctx.freshName("row")

    // Add a check if the generate outer flag is true.
    val checks = optionalCode(outer, s"!$hasNext")
    val values = e.dataType match {
      case ArrayType(st: StructType, nullable) =>
        st.fields.toSeq.zipWithIndex.map { case (f, i) =>
          codeGenAccessor(ctx, current, f.name, s"$i", f.dataType, f.nullable, checks)
        }
    }

    // In case of outer=true we need to make sure the loop is executed at-least-once when the
    // iterator contains no input. We do this by adding an 'outer' variable which guarantees
    // execution of the first iteration even if there is no input. Evaluation of the iterator is
    // prevented by checks in the next() and accessor code.
    val hasNextCode = s"$hasNext = $iterator.hasNext()"
    val outerVal = ctx.freshName("outer")
    def concatIfOuter(s1: String, s2: String): String = s1 + (if (outer) s2 else "")
    val init = concatIfOuter(s"boolean $hasNextCode", s", $outerVal = true")
    val check = concatIfOuter(hasNext, s"|| $outerVal")
    val update = concatIfOuter(hasNextCode, s", $outerVal = false")
    val next = if (outer) s"$hasNext ? $iterator.next() : null" else s"$iterator.next()"
    val numOutput = metricTerm(ctx, "numOutputRows")
    s"""
       |${data.code}
       |scala.collection.Iterator<InternalRow> $iterator = ${data.value}.toIterator();
       |for ($init; $check; $update) {
       |  $numOutput.add(1);
       |  InternalRow $current = (InternalRow)($next);
       |  ${consume(ctx, input ++ values)}
       |}
     """.stripMargin
  }

  /**
   * Generate accessor code for ArrayData and InternalRows.
   */
  private def codeGenAccessor(
      ctx: CodegenContext,
      source: String,
      name: String,
      index: String,
      dt: DataType,
      nullable: Boolean,
      initialChecks: Seq[String]): ExprCode = {
    val value = ctx.freshName(name)
    val javaType = ctx.javaType(dt)
    val getter = ctx.getValue(source, dt, index)
    val checks = initialChecks ++ optionalCode(nullable, s"$source.isNullAt($index)")
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

  private def optionalCode(condition: Boolean, code: => String): Seq[String] = {
    if (condition) Seq(code)
    else Seq.empty
  }
}
