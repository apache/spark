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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions._

/**
 * :: DeveloperApi ::
 * Applies a [[catalyst.expressions.Generator Generator]] to a stream of input rows, combining the
 * output of each into a new stream of rows.  This operation is similar to a `flatMap` in functional
 * programming with one important additional feature, which allows the input rows to be joined with
 * their output.
 * @param join  when true, each output row is implicitly joined with the input tuple that produced
 *              it.
 * @param outer when true, each input row will be output at least once, even if the output of the
 *              given `generator` is empty. `outer` has no effect when `join` is false.
 */
@DeveloperApi
case class Generate(
    generator: Generator,
    join: Boolean,
    outer: Boolean,
    child: SparkPlan)
  extends UnaryNode {

  // This must be a val since the generator output expr ids are not preserved by serialization.
  protected val generatorOutput: Seq[Attribute] = {
    if (join && outer) {
      generator.output.map(_.withNullability(true))
    } else {
      generator.output
    }
  }

  // This must be a val since the generator output expr ids are not preserved by serialization.
  override val output =
    if (join) child.output ++ generatorOutput else generatorOutput

  val boundGenerator = BindReferences.bindReference(generator, child.output)

  override def execute() = {
    if (join) {
      child.execute().mapPartitions { iter =>
        val nullValues = Seq.fill(generator.output.size)(Literal(null))
        // Used to produce rows with no matches when outer = true.
        val outerProjection =
          newProjection(child.output ++ nullValues, child.output)

        val joinProjection =
          newProjection(child.output ++ generatorOutput, child.output ++ generatorOutput)
        val joinedRow = new JoinedRow

        iter.flatMap {row =>
          val outputRows = boundGenerator.eval(row)
          if (outer && outputRows.isEmpty) {
            outerProjection(row) :: Nil
          } else {
            outputRows.map(or => joinProjection(joinedRow(row, or)))
          }
        }
      }
    } else {
      child.execute().mapPartitions(iter => iter.flatMap(row => boundGenerator.eval(row)))
    }
  }
}
