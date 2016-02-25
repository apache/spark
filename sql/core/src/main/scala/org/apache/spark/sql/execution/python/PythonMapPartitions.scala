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

package org.apache.spark.sql.execution.python

import scala.collection.JavaConverters._

import net.razorvine.pickle.{Pickler, Unpickler}

import org.apache.spark.TaskContext
import org.apache.spark.api.python.{PythonFunction, PythonRunner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericMutableRow, UnsafeProjection}
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}

/**
 * Launches a Python runner, send all rows to it, then apply the given function to each row in the
 * launched Python runner, and send the results back.
 *
 * Note that if the schema of this plan equals to [[EvaluatePython.schemaOfPickled]], it means the
 * result is not row, and we can't understand the data without the real schema, so we will just wrap
 * the pickled binary as a single field row.
 * If the schema of child plan equals to [[EvaluatePython.schemaOfPickled]], it means the input data
 * is a single field row with pickled binary data, so we will just get the binary and send to Python
 * runner, without serializing it.
 */
case class PythonMapPartitions(
    func: PythonFunction,
    output: Seq[Attribute],
    child: SparkPlan) extends UnaryNode {

  override def expressions: Seq[Expression] = Nil

  override protected def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute().map(_.copy())
    val bufferSize = inputRDD.conf.getInt("spark.buffer.size", 65536)
    val reuseWorker = inputRDD.conf.getBoolean("spark.python.worker.reuse", defaultValue = true)
    val isChildPickled = EvaluatePython.schemaOfPickled == child.schema
    val isOutputPickled = EvaluatePython.schemaOfPickled == schema

    inputRDD.mapPartitions { iter =>
      val inputIterator = if (isChildPickled) {
        iter.map(_.getBinary(0))
      } else {
        EvaluatePython.registerPicklers()  // register pickler for Row
        val pickle = new Pickler

        // Input iterator to Python: input rows are grouped so we send them in batches to Python.
        iter.grouped(100).map { inputRows =>
          val toBePickled = inputRows.map { row =>
            EvaluatePython.toJava(row, child.schema)
          }.toArray
          pickle.dumps(toBePickled)
        }
      }

      val context = TaskContext.get()

      // Output iterator for results from Python.
      val outputIterator =
        new PythonRunner(
          func,
          bufferSize,
          reuseWorker
        ).compute(inputIterator, context.partitionId(), context)

      val toUnsafe = UnsafeProjection.create(output, output)

      if (isOutputPickled) {
        val row = new GenericMutableRow(1)
        outputIterator.map { bytes =>
          row(0) = bytes
          toUnsafe(row)
        }
      } else {
        val unpickle = new Unpickler
        outputIterator.flatMap { pickedResult =>
          val unpickledBatch = unpickle.loads(pickedResult)
          unpickledBatch.asInstanceOf[java.util.ArrayList[Any]].asScala
        }.map { result =>
          toUnsafe(EvaluatePython.fromJava(result, schema).asInstanceOf[InternalRow])
        }
      }
    }
  }
}
