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

package org.apache.spark.sql.connector.expressions.filter;

import java.io.Serializable;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.StorageFilterRequest;
import org.apache.spark.sql.connector.read.SupportsStorageFilters;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A row-level predicate a scan may evaluate inside its readers, to skip reading the other columns
 * of the rows it rejects. The reader reads the columns of {@link #references()} first, evaluates
 * the filter, and reads the remaining columns only for the rows it keeps.
 * <p>
 * The filter is a predicate on the rows the scan returns. Spark offers none to a scan that took a
 * pushed operation that decides which rows it returns, such as a limit, an offset, a top-N, a
 * sample, an aggregate or a join.
 * <p>
 * Spark implements the predicate, and the reader only supplies the values. For example, a scan
 * does not have to reproduce the Spark sketch and hash of a runtime bloom filter. The values must
 * be the ones the scan returns for those columns, in the types of {@link Scan#readSchema()}, nulls
 * included.
 * <p>
 * Applying it is optional, see {@link SupportsStorageFilters}.
 * <p>
 * A row's result depends only on its own values. So with one reference, a reader may evaluate each
 * distinct value once, such as each entry of a dictionary, and reuse the result for every row
 * holding that value. A dictionary entry is evaluated as the value the scan returns for it, decoded
 * and converted.
 * <p>
 * Spark hands the filter to each task in a {@link StorageFilterRequest}. The readers of a task
 * share one instance, which is not thread-safe. A reader must not copy or serialize it, since an
 * evaluated instance may have released what a copy would be built from.
 *
 * @since 5.0.0
 */
@Evolving
public abstract class StorageFilter implements Serializable {

  /**
   * The columns the predicate reads. Their order is the layout of the row
   * {@link #eval(InternalRow)} and the batch {@link #select(ColumnarBatch, int[])} expect. Each
   * names one of the scan's {@link SupportsStorageFilters#storageFilterAttributes()}, as
   * {@link Scan#readSchema()} spells it.
   */
  public abstract NamedReference[] references();

  /**
   * Evaluates the filter on one row holding the values of {@link #references()}, in that order.
   *
   * @return false when the row is rejected, so a reader may drop it and skip any read only that
   *         row needed. A row the reader keeps anyway is not marked checked, see
   *         {@link StorageFilterRequest#appendsCheckedColumn()}.
   * @throws StorageFilterNotAppliedException when the filter cannot be applied to this row. The
   *         reader then reads the rest of the unit without the filter. Any other exception, such
   *         as a task kill, must propagate.
   */
  public abstract boolean eval(InternalRow row) throws StorageFilterNotAppliedException;

  /**
   * Evaluates the filter on every row of {@code batch}, whose columns hold the values of
   * {@link #references()}, in that order. Writes the positions of the rows it keeps into
   * {@code selected}, in ascending order. {@code selected} must hold at least
   * {@code batch.numRows()} entries.
   * <p>
   * A reader can use the positions to pick the kept rows out of its other columns. The default
   * evaluates the rows one by one through {@link #eval(InternalRow)}, and a vectorized
   * implementation can replace it with no change to the reader.
   *
   * @return the number of positions written to {@code selected}
   * @throws StorageFilterNotAppliedException as {@link #eval(InternalRow)} does, which leaves
   *         {@code selected} undefined
   */
  public int select(ColumnarBatch batch, int[] selected)
      throws StorageFilterNotAppliedException {
    int numRows = batch.numRows();
    int count = 0;
    for (int i = 0; i < numRows; i++) {
      if (eval(batch.getRow(i))) {
        selected[count++] = i;
      }
    }
    return count;
  }
}
