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

package org.apache.spark.sql.execution;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterIterator;
import scala.collection.AbstractIterator;

import java.io.Closeable;
import java.io.IOException;

public abstract class UnsafeExternalRowIterator extends AbstractIterator<UnsafeRow> implements Closeable {

    private final UnsafeSorterIterator sortedIterator;
    private UnsafeRow row;

    UnsafeExternalRowIterator(StructType schema, UnsafeSorterIterator iterator) {
        row = new UnsafeRow(schema.length());
        sortedIterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return sortedIterator.hasNext();
    }

    @Override
    public UnsafeRow next() {
        try {
            sortedIterator.loadNext();
            row.pointTo(
                    sortedIterator.getBaseObject(),
                    sortedIterator.getBaseOffset(),
                    sortedIterator.getRecordLength());
            if (!hasNext()) {
                UnsafeRow copy = row.copy(); // so that we don't have dangling pointers to freed page
                row = null; // so that we don't keep references to the base object
                close();
                return copy;
            } else {
                return row;
            }
        } catch (IOException e) {
            close();
            // Scala iterators don't declare any checked exceptions, so we need to use this hack
            // to re-throw the exception:
            Platform.throwException(e);
        }
        throw new RuntimeException("Exception should have been re-thrown in next()");
    }

    /**
     * Implementation should clean up resources used by this iterator, to prevent memory leaks
     */
    @Override
    public abstract void close();
}
