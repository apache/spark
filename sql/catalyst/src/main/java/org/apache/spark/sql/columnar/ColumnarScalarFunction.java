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

package org.apache.spark.sql.columnar;

import java.io.Serializable;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A scalar function that Spark evaluates one batch at a time, in the JVM. It is the
 * implementation of a columnar user-defined function that Spark itself can run; an engine that
 * replaces Spark's operators can run another implementation of the same function instead, see
 * {@link ColumnarUDFImplementation}.
 * <p>
 * Spark serializes the function to the executors. There, each operator that evaluates calls of
 * the function gets its own instance for each task, and calls it from one thread at a time.
 * The batches of a task can be evaluated by different threads, as below a script
 * transformation, so an implementation must not rely on state that is bound to a thread. The
 * calls that one operator evaluates share that instance. Two calls in one projection are
 * evaluated by one operator, unless the argument of one of them is another call: such a call is
 * evaluated by an operator of its own, with its own instance.
 * <p>
 * A function that holds resources that have to be released, such as native memory, can
 * implement {@link AutoCloseable}. Spark then closes each instance once, when the operator has
 * read all its input or when the task ends, after it has closed the results that the instance
 * returned. Whatever a returned vector uses must stay valid until Spark closes the vector, so
 * a function does not release it from a task completion listener of its own.
 * <p>
 * An exception that the function throws fails the query with the error condition
 * {@code FAILED_EXECUTE_UDF}, with the exception as its cause. So does a {@code LinkageError},
 * such as the one of a native library that cannot be loaded.
 * <p>
 * Unless the function is declared not deterministic, its result must not depend on how the
 * rows are split into batches, nor on the order of the rows or of the batches. Spark evaluates
 * the function for every row that reaches it, also for rows whose result the query does not
 * use, such as in a branch of a CASE expression that is not taken.
 *
 * @since 4.5.0
 */
@Evolving
public interface ColumnarScalarFunction extends Serializable {

  /**
   * Evaluates the function for every row of a batch.
   *
   * @param args the arguments, one column per argument in the declared order, with the declared
   *             types. The function must neither close the batch or its columns, nor use them
   *             after it returns.
   * @return a vector of the declared result type that holds at least {@code args.numRows()}
   *         values. For a CHAR or VARCHAR result type, the vector can also be of the STRING
   *         type, and the function makes sure that the values have the declared length. It
   *         must be a vector of the function's own, which shares no memory with
   *         the argument columns: Spark closes it once it is done with the batch. A function
   *         that returns an argument as it is, or a vector inside one (a field of a struct, the
   *         elements of an array, the keys or the values of a map), copies it into a new vector.
   *         Each call returns a new vector, which shares no memory with a vector that the
   *         function returned before, and which the function does not use after it returns
   *         it. The calls that share an instance each need a vector of their own for one
   *         batch, so an instance does not keep one vector for its results.
   */
  ColumnVector eval(ColumnarBatch args);
}
