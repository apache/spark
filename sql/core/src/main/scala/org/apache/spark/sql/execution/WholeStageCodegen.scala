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
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen._


/**
  * FakeInput is used to hide a SparkPlan from a subtree that support codegen.
  */
case class FakeInput(child: SparkPlan) extends LeafNode {

  override def output: Seq[Attribute] = child.output

  override def supportCodegen: Boolean = true

  override def doProduce(ctx: CodegenContext): (RDD[InternalRow], String) = {
    val exprs = output.zipWithIndex.map(x => new BoundReference(x._2, x._1.dataType, true))
    val row = ctx.freshName("row")
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    val columns = exprs.map(_.gen(ctx))
    val code = s"""
       |  while (input.hasNext()) {
       |   InternalRow $row = (InternalRow) input.next();
       |   ${columns.map(_.code).mkString("\n")}
       |   ${consume(ctx, columns)}
       | }
     """.stripMargin
    (child.execute(), code)
  }

  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException
  }

  override def simpleString: String = "INPUT"
}

/**
  * WholeStageCodegen compile a subtree of plans that support codegen together into single Java
  * function.
  *
  * Here is the call graph of to generate Java source (plan A support codegen, but plan B does not):
  *
  *   WholeStageCodegen       Plan A               FakeInput        Plan B
  * =========================================================================
  *
  * -> execute()
  *     |
  *  doExecute() -------->   produce()
  *                             |
  *                          doProduce()  -------> produce()
  *                                                   |
  *                                                doProduce() ---> execute()
  *                                                   |
  *                                                consume()
  *                          doConsume()  ------------|
  *                             |
  *  doConsume()  <-----    consume()
  *
  * SparkPlan A should override doProduce() and doConsume().
  *
  * doCodeGen() will create a CodeGenContext, which will hold a list of variables for input,
  * used to generated code for BoundReference.
  */
case class WholeStageCodegen(plan: SparkPlan, children: Seq[SparkPlan]) extends SparkPlan {

  override def output: Seq[Attribute] = plan.output

  override def doExecute(): RDD[InternalRow] = {
    val ctx = new CodegenContext
    val (rdd, code) = plan.produce(ctx, this)
    val exprType: String = classOf[Expression].getName
    val references = ctx.references.toArray
    val source = s"""
      public Object generate($exprType[] exprs) {
       return new GeneratedIterator(exprs);
      }

      class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {

       private $exprType[] expressions;
       ${ctx.declareMutableStates()}

       public GeneratedIterator($exprType[] exprs) {
         expressions = exprs;
         ${ctx.initMutableStates()}
       }

       protected void processNext() {
         $code
       }
      }
     """
    // try to compile, helpful for debug
    // println(s"${CodeFormatter.format(source)}")
    CodeGenerator.compile(source)

    rdd.mapPartitions { iter =>
      val clazz = CodeGenerator.compile(source)
      val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
      buffer.process(iter)
      new Iterator[InternalRow] {
        override def hasNext: Boolean = buffer.hasNext
        override def next: InternalRow = buffer.next()
      }
    }
  }

  override def doConsume(ctx: CodegenContext, child: SparkPlan, input: Seq[ExprCode]): String = {
    if (input.nonEmpty) {
      val colExprs = output.zipWithIndex.map { case (attr, i) =>
        BoundReference(i, attr.dataType, attr.nullable)
      }
      // generate the code to create a UnsafeRow
      ctx.currentVars = input
      val code = GenerateUnsafeProjection.createCode(ctx, colExprs, false)
      s"""
         | ${code.code.trim}
         | currentRow = ${code.value};
         | return;
     """.stripMargin
    } else {
      // There is no columns
      s"""
         | currentRow = unsafeRow;
         | return;
       """.stripMargin
    }
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      builder: StringBuilder): StringBuilder = {
    if (depth > 0) {
      lastChildren.init.foreach { isLast =>
        val prefixFragment = if (isLast) "   " else ":  "
        builder.append(prefixFragment)
      }

      val branch = if (lastChildren.last) "+- " else ":- "
      builder.append(branch)
    }

    builder.append(simpleString)
    builder.append("\n")

    plan.generateTreeString(depth + 1, lastChildren :+children.isEmpty :+ true, builder)
    if (children.nonEmpty) {
      children.init.foreach(_.generateTreeString(depth + 1, lastChildren :+ false, builder))
      children.last.generateTreeString(depth + 1, lastChildren :+ true, builder)
    }

    builder
  }

  override def simpleString: String = "WholeStageCodegen"
}
