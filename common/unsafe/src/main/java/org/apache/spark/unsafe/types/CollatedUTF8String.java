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

package org.apache.spark.unsafe.types;

import static org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET;
import org.apache.spark.unsafe.array.ByteArrayMethods;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.text.Collator;
import java.util.Arrays;

public final class CollatedUTF8String extends UTF8String {
    transient private Collator collator;

    private CollatedUTF8String(Object base, long offset, int numBytes, Collator collator) {
        super(base, offset, numBytes);
        this.collator = collator;
    }

    /**
     * Creates an CollatedUTF8String from byte array, which should be encoded in UTF-8.
     *
     * Note: `bytes` will be hold by returned UTF8String.
     */
    public static CollatedUTF8String fromBytes(byte[] bytes, Collator collator) {
        if (bytes != null) {
            return new CollatedUTF8String(bytes, BYTE_ARRAY_OFFSET, bytes.length, collator);
        } else {
            return null;
        }
    }

    /**
     * Creates an UTF8String from byte array, which should be encoded in UTF-8.
     *
     * Note: `bytes` will be hold by returned UTF8String.
     */
    public static CollatedUTF8String fromBytes(
            byte[] bytes, int offset, int numBytes, Collator collator) {
        if (bytes != null) {
            return new CollatedUTF8String(bytes, BYTE_ARRAY_OFFSET + offset, numBytes, collator);
        } else {
            return null;
        }
    }

    /**
     * Creates an UTF8String from given address (base and offset) and length.
     */
    public static CollatedUTF8String fromAddress(Object base, long offset, int numBytes, Collator collator) {
        return new CollatedUTF8String(base, offset, numBytes, collator);
    }

    /**
     * Creates an UTF8String from String.
     */
    public static CollatedUTF8String fromString(String str, Collator collator) {
        return str == null ? null : fromBytes(str.getBytes(StandardCharsets.UTF_8), collator);
    }

    /**
     * Creates an UTF8String that contains `length` spaces.
     */
    public static CollatedUTF8String blankString(int length, Collator collator) {
        byte[] spaces = new byte[length];
        Arrays.fill(spaces, (byte) ' ');
        return fromBytes(spaces, collator);
    }

    public int compareTo(@Nonnull final CollatedUTF8String other) {
        // TODO: At this point this requires us to convert the UTF8String to a String.
        // This is due t o Collator implementation in Java.
        // With ICU we should aim to stay in UTF8 format and do comparison without additional
        // allocations.
        return this.collator.compare(this.toString(), other.toString());
    }

    public int compare(final CollatedUTF8String other) {
        return compareTo(other);
    }

    public boolean equals(final Object other) {
        if (other instanceof CollatedUTF8String o) {
            // TODO: Can we do number of bytes comparison for collated strings?
            // TODO: We can also check whether collation is deterministic and do byte-wise comparison if so.
            // if (numBytes != o.numBytes) {
            //     return false;
            // }
            return this.compare(o) == 0;
        } else {
            return false;
        }
    }
}