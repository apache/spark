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

package org.apache.spark.sql.catalyst.expressions;

import java.util.Arrays;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.memory.Memory;

import org.apache.spark.sql.catalyst.util.CollationFactory;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * SerDe for {@link CollatedString} items used by {@code approx_top_k} over non-binary collated
 * strings (SPARK-58069).
 * <p>
 * Only the {@code original} value is written to the wire, reusing the plain-string format of
 * {@link ArrayOfStringsSerDe}; the collation key is recomputed on read from {@code collationId}.
 * As a result the serialized bytes are exactly a string array (the extra per-item key is derived,
 * not persisted), and the on-wire layout stays identical to the plain-string sketch.
 */
public class ArrayOfCollatedStringsSerDe extends ArrayOfItemsSerDe<CollatedString> {

    private final int collationId;
    private final ArrayOfStringsSerDe stringSerDe = new ArrayOfStringsSerDe();

    public ArrayOfCollatedStringsSerDe(int collationId) {
        this.collationId = collationId;
    }

    private CollatedString wrap(String original) {
        String key = CollationFactory.getCollationKey(
            UTF8String.fromString(original), collationId).toString();
        return new CollatedString(key, original);
    }

    @Override
    public byte[] serializeToByteArray(CollatedString item) {
        return stringSerDe.serializeToByteArray(item.original());
    }

    @Override
    public byte[] serializeToByteArray(CollatedString[] items) {
        String[] originals = new String[items.length];
        for (int i = 0; i < items.length; i++) {
            originals[i] = items[i].original();
        }
        return stringSerDe.serializeToByteArray(originals);
    }

    @Override
    public CollatedString[] deserializeFromMemory(Memory mem, long offsetBytes, int numItems) {
        String[] originals = stringSerDe.deserializeFromMemory(mem, offsetBytes, numItems);
        return Arrays.stream(originals).map(this::wrap).toArray(CollatedString[]::new);
    }

    @Override
    public int sizeOf(CollatedString item) {
        return stringSerDe.sizeOf(item.original());
    }

    @Override
    public int sizeOf(Memory mem, long offsetBytes, int numItems) {
        return stringSerDe.sizeOf(mem, offsetBytes, numItems);
    }

    @Override
    public String toString(CollatedString item) {
        return item.original();
    }

    @Override
    public Class<CollatedString> getClassOfT() {
        return CollatedString.class;
    }
}
