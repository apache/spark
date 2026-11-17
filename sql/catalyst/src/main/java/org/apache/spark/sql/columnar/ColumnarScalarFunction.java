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
 * Spark serializes the function to the executors, where each task gets its own instance and
 * calls it from one thread.
 * <p>
 * The result must not depend on how the rows are split into batches, nor on the order of the
 * rows or of the batches. Spark evaluates the function for every row that reaches it, also for
 * rows whose result the query does not use, such as in a branch of a CASE expression that is
 * not taken.
 *
 * @since 4.4.0
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
   *         values. It must be a vector of the function's own, and not one of the argument
   *         columns: Spark closes it once it is done with the batch. A function that returns
   *         an argument as it is copies it into a new vector.
   */
  ColumnVector eval(ColumnarBatch args);
}
