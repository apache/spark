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

/**
 * A marker for a {@link org.apache.spark.sql.vectorized.ColumnVector} that its producer may
 * release or overwrite after returning it in a batch: for example, a reader that refills the same
 * vectors for every batch, or frees the vectors of a batch once the next one is read or once it is
 * closed. A consumer may only read such a vector until it asks the producer for the next batch,
 * and must copy what it needs past that point.
 *
 * <p>There is no other agreement between the producer and the consumer of a batch, so the type of
 * its vectors is the protocol, as for {@code ColumnVector.closeIfFreeable}: the producer owns the
 * lifetime of a vector carrying this marker, and its {@code closeIfFreeable} is a no-op. A vector
 * without it may be held by the consumer past the next batch without a copy, and the consumer
 * releases it with {@code closeIfFreeable} once done with it.
 *
 * <p>The marker covers the whole vector, so it is only checked on the top-level columns of a
 * batch. The child vectors of a struct, array or map column belong to that column and share its
 * lifetime.
 */
public interface ReusedColumnVector {
}
