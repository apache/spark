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
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types._

/**
 * For lazy computing, be sure the generator.terminate() called in the very last
 * TODO reusing the CompletionIterator?
 */
private[execution] sealed case class LazyIterator(func: () => TraversableOnce[InternalRow])
  extends Iterator[InternalRow] {

  lazy val results: Iterator[InternalRow] = func().toIterator
  override def hasNext: Boolean = results.hasNext
  override def next(): InternalRow = results.next()
}

/**
 * Applies a [[Generator]] to a stream of input rows, combining the
 * output of each into a new stream of rows.  This operation is similar to a `flatMap` in functional
 * programming with one important additional feature, which allows the input rows to be joined with
 * their output.
 *
 * This operator supports whole stage code generation for generators that do not implement
 * terminate().
 *
 * @param generator the generator expression
 * @param requiredChildOutput required attributes from child's output
 * @param outer when true, each input row will be output at least once, even if the output of the
 *              given `generator` is empty.
 * @param generatorOutput the qualified output attributes of the generator of this node, which
 *                        constructed in analysis phase, and we can not change it, as the
 *                        parent node bound with it already.
 */
case class GenerateExec(
    generator: Generator,
    requiredChildOutput: Seq[Attribute],
    outer: Boolean,
    generatorOutput: Seq[Attribute],
    child: SparkPlan)
  extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = requiredChildOutput ++ generatorOutput

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  lazy val boundGenerator: Generator = BindReferences.bindReference(generator, child.output)

  protected override def doExecute(): RDD[InternalRow] = {
    // boundGenerator.terminate() should be triggered after all of the rows in the partition
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      val generatorNullRow = new GenericInternalRow(generator.elementSchema.length)
      val rows = if (requiredChildOutput.nonEmpty) {

        val pruneChildForResult: InternalRow => InternalRow =
          if (child.outputSet == AttributeSet(requiredChildOutput)) {
            identity
          } else {
            UnsafeProjection.create(requiredChildOutput, child.output)
          }

        val joinedRow = new JoinedRow
        iter.flatMap { row =>
          // we should always set the left (required child output)
          joinedRow.withLeft(pruneChildForResult(row))
          val outputRows = boundGenerator.eval(row)
          if (outer && outputRows.isEmpty) {
            joinedRow.withRight(generatorNullRow) :: Nil
          } else {
            outputRows.map(joinedRow.withRight)
          }
        } ++ LazyIterator(() => boundGenerator.terminate()).map { row =>
          // we leave the left side as the last element of its child output
          // keep it the same as Hive does
          joinedRow.withRight(row)
        }
      } else {
        iter.flatMap { row =>
          val outputRows = boundGenerator.eval(row)
          if (outer && outputRows.isEmpty) {
            Seq(generatorNullRow)
          } else {
            outputRows
          }
        } ++ LazyIterator(() => boundGenerator.terminate())
      }

      // Convert the rows to unsafe rows.
      val proj = UnsafeProjection.create(output, output)
      proj.initialize(index)
      rows.map { r =>
        numOutputRows += 1
        proj(r)
      }
    }
  }

  override def supportCodegen: Boolean = false

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def needCopyResult: Boolean = true

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    // Add input rows to the values when we are joining
    val values = if (requiredChildOutput.nonEmpty) {
      input
    } else {
      Seq.empty
    }

    boundGenerator match {
      case e: CollectionGenerator => codeGenCollection(ctx, e, values, row)
      case g => codeGenTraversableOnce(ctx, g, values, row)
    }
  }

  /**
   * Generate code for [[CollectionGenerator]] expressions.
   */
  private def codeGenCollection(
      ctx: CodegenContext,
      e: CollectionGenerator,
      input: Seq[ExprCode],
      row: ExprCode): String = {

    // Generate code for the generator.
    val data = e.genCode(ctx)

    // Generate looping variables.
    val index = ctx.freshName("index")

    // Add a check if the generate outer flag is true.
    val checks = optionalCode(outer, s"($index == -1)")

    // Add position
    val position = if (e.position) {
      if (outer) {
        Seq(ExprCode(
          JavaCode.isNullExpression(s"$index == -1"),
          JavaCode.variable(index, IntegerType)))
      } else {
        Seq(ExprCode(FalseLiteral, JavaCode.variable(index, IntegerType)))
      }
    } else {
      Seq.empty
    }

    // Generate code for either ArrayData or MapData
    val (initMapData, updateRowData, values) = e.collectionType match {
      case ArrayType(st: StructType, nullable) if e.inline =>
        val row = codeGenAccessor(ctx, data.value, "col", index, st, nullable, checks)
        val fieldChecks = checks ++ optionalCode(nullable, row.isNull)
        val columns = st.fields.toSeq.zipWithIndex.map { case (f, i) =>
          codeGenAccessor(
            ctx,
            row.value,
            s"st_col${i}",
            i.toString,
            f.dataType,
            f.nullable,
            fieldChecks)
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
    val init = if (outer) {
      s"$numElements == 0 ? -1 : 0"
    } else {
      "0"
    }
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
      row: ExprCode): String = {

    // Generate the code for the generator
    val data = e.genCode(ctx)

    // Generate looping variables.
    val iterator = ctx.freshName("iterator")
    val hasNext = ctx.freshName("hasNext")
    val current = ctx.freshName("row")

    // Add a check if the generate outer flag is true.
    val checks = optionalCode(outer, s"!$hasNext")
    val values = e.dataType match {
      case ArrayType(st: StructType, nullable) =>
        st.fields.toSeq.zipWithIndex.map { case (f, i) =>
          codeGenAccessor(ctx, current, s"st_col${i}", s"$i", f.dataType, f.nullable, checks)
        }
    }

    // In case of outer=true we need to make sure the loop is executed at-least-once when the
    // iterator contains no input. We do this by adding an 'outer' variable which guarantees
    // execution of the first iteration even if there is no input. Evaluation of the iterator is
    // prevented by checks in the next() and accessor code.
    val numOutput = metricTerm(ctx, "numOutputRows")
    if (outer) {
      val outerVal = ctx.freshName("outer")
      s"""
         |${data.code}
         |scala.collection.Iterator<InternalRow> $iterator = ${data.value}.toIterator();
         |boolean $outerVal = true;
         |while ($iterator.hasNext() || $outerVal) {
         |  $numOutput.add(1);
         |  boolean $hasNext = $iterator.hasNext();
         |  InternalRow $current = (InternalRow)($hasNext? $iterator.next() : null);
         |  $outerVal = false;
         |  ${consume(ctx, input ++ values)}
         |}
      """.stripMargin
    } else {
      s"""
         |${data.code}
         |scala.collection.Iterator<InternalRow> $iterator = ${data.value}.toIterator();
         |while ($iterator.hasNext()) {
         |  $numOutput.add(1);
         |  InternalRow $current = (InternalRow)($iterator.next());
         |  ${consume(ctx, input ++ values)}
         |}
      """.stripMargin
    }
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
    val javaType = CodeGenerator.javaType(dt)
    val getter = CodeGenerator.getValue(source, dt, index)
    val checks = initialChecks ++ optionalCode(nullable, s"$source.isNullAt($index)")
    if (checks.nonEmpty) {
      val isNull = ctx.freshName("isNull")
      val code =
        code"""
           |boolean $isNull = ${checks.mkString(" || ")};
           |$javaType $value = $isNull ? ${CodeGenerator.defaultValue(dt)} : $getter;
         """.stripMargin
      ExprCode(code, JavaCode.isNullVariable(isNull), JavaCode.variable(value, dt))
    } else {
      ExprCode(code"$javaType $value = $getter;", FalseLiteral, JavaCode.variable(value, dt))
    }
  }

  private def optionalCode(condition: Boolean, code: => String): Seq[String] = {
    if (condition) Seq(code)
    else Seq.empty
  }
}
