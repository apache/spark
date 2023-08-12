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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.expressions.{Expression, LeafExpression, Nondeterministic}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._

/**
 * A trait that can be used to provide a fallback mode for expression code generation.
 */
trait CodegenFallback extends Expression {

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // LeafNode does not need `input`
    val input = if (this.isInstanceOf[LeafExpression]) "null" else ctx.INPUT_ROW
    val idx = ctx.references.length
    ctx.references += this
    var childIndex = idx
    this.foreach {
      case n: Nondeterministic =>
        // This might add the current expression twice, but it won't hurt.
        ctx.references += n
        childIndex += 1
        ctx.addPartitionInitializationStatement(
          s"""
             |((Nondeterministic) references[$childIndex])
             |  .initialize(partitionIndex);
          """.stripMargin)
      case _ =>
    }
    val objectTerm = ctx.freshName("obj")
    val placeHolder = ctx.registerComment(this.toString)
    val javaType = CodeGenerator.javaType(this.dataType)
    val childrenGen = children.map(_.genCode(ctx))
    val childrenCode = childrenGen.map(_.code).mkString("\n")
    val childrenParameter = childrenGen.map(_.value).mkString(", ")
    val clazz = this.getClass.getName
    if (supportWholeStageCodegen) {
      if (nullable) {
        ev.copy(code =
          code"""
          $placeHolder
          ${childrenCode}
          Object $objectTerm = null;
          if(${childrenGen.map(_.isNull).mkString(" || ")}) {
            $objectTerm = null;
          } else {
            $objectTerm = (($clazz) references[$idx]).nullSafeEval(${childrenParameter});
          }
          boolean ${ev.isNull} = $objectTerm == null;
          $javaType ${ev.value} = ${CodeGenerator.defaultValue(this.dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = (${CodeGenerator.boxedType(this.dataType)}) $objectTerm;
          }""")
      } else {
        ev.copy(code =
          code"""
          $placeHolder
          ${childrenCode}
          Object $objectTerm = (($clazz) references[$idx]).nullSafeEval(${childrenParameter});
          $javaType ${ev.value} = (${CodeGenerator.boxedType(this.dataType)}) $objectTerm;
          """, isNull = FalseLiteral)
      }
    } else {
      if (nullable) {
        ev.copy(code = code"""
          $placeHolder
          Object $objectTerm = ((Expression) references[$idx]).eval($input);
          boolean ${ev.isNull} = $objectTerm == null;
          $javaType ${ev.value} = ${CodeGenerator.defaultValue(this.dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = (${CodeGenerator.boxedType(this.dataType)}) $objectTerm;
          }""")
      } else {
        ev.copy(code = code"""
          $placeHolder
          Object $objectTerm = ((Expression) references[$idx]).eval($input);
          $javaType ${ev.value} = (${CodeGenerator.boxedType(this.dataType)}) $objectTerm;
          """, isNull = FalseLiteral)
      }
    }
  }

}
