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
package org.apache.spark.sql.execution.vectorized;

import org.apache.arrow.vector.ValueVector;

import org.apache.spark.sql.vectorized.ArrowColumnVector;

/**
 * An {@link ArrowColumnVector} whose producer may release or overwrite it after returning it, for
 * example by loading every batch into the same Arrow vectors. A consumer must not hold it past the
 * next batch.
 *
 * <p>Like the vectors that {@link WritableColumnVector} and {@link ConstantColumnVector} reuse
 * across batches, the producer releases it: {@link #closeIfFreeable()}, which a consumer calls
 * once it is done with a batch, is a no-op, and the producer frees the vector with
 * {@link #close()}.
 */
public class ReusedArrowColumnVector extends ArrowColumnVector {

  public ReusedArrowColumnVector(ValueVector vector) {
    super(vector);
  }

  @Override
  public void closeIfFreeable() {
    // no-op: the producer owns the vector and frees it in `close`.
  }
}
