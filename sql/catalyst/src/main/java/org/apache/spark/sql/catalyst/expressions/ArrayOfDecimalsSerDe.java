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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.common.ArrayOfNumbersSerDe;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.memory.Memory;

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;

import static org.apache.datasketches.common.ByteArrayUtil.copyBytes;
import static org.apache.datasketches.common.ByteArrayUtil.putIntLE;

public class ArrayOfDecimalsSerDe extends ArrayOfItemsSerDe<Decimal> {

    private final int precision;
    private final int scale;
    private final DecimalType decimalType;
    private final ArrayOfItemsSerDe<?> delegate;

    public ArrayOfDecimalsSerDe(DecimalType decimalType) {
        this.decimalType = decimalType;
        this.precision = decimalType.precision();
        this.scale = decimalType.scale();

        if (DecimalType.is32BitDecimalType(decimalType)) {
            this.delegate = new ArrayOfNumbersSerDe();
        } else if (DecimalType.is64BitDecimalType(decimalType)) {
            this.delegate = new ArrayOfLongsSerDe();
        } else {
            this.delegate = new ArrayOfDecimalByteArrSerDe(decimalType);
        }
    }

    @Override
    public byte[] serializeToByteArray(Decimal item) {
        Objects.requireNonNull(item, "Item must not be null");
        if (DecimalType.is32BitDecimalType(decimalType)) {
            return ((ArrayOfNumbersSerDe) delegate).serializeToByteArray(decimalToInt(item));
        } else if (DecimalType.is64BitDecimalType(decimalType)) {
            return ((ArrayOfLongsSerDe) delegate).serializeToByteArray(item.toUnscaledLong());
        } else {
            return ((ArrayOfDecimalByteArrSerDe) delegate).serializeToByteArray(item);
        }
    }

    @Override
    public byte[] serializeToByteArray(Decimal[] items) {
        Objects.requireNonNull(items, "Item must not be null");
        if (DecimalType.is32BitDecimalType(decimalType)) {
            Number[] intItems = new Number[items.length];
            for (int i = 0; i < items.length; i++) {
                intItems[i] = decimalToInt(items[i]);
            }
            return ((ArrayOfNumbersSerDe) delegate).serializeToByteArray(intItems);
        } else if (DecimalType.is64BitDecimalType(decimalType)) {
            Long[] longItems = new Long[items.length];
            for (int i = 0; i < items.length; i++) {
                longItems[i] = items[i].toUnscaledLong();
            }
            return ((ArrayOfLongsSerDe) delegate).serializeToByteArray(longItems);
        } else {
            return ((ArrayOfDecimalByteArrSerDe) delegate).serializeToByteArray(items);
        }
    }

    @Override
    public Decimal[] deserializeFromMemory(Memory mem, long offsetBytes, int numItems) {
        Objects.requireNonNull(mem, "Memory must not be null");
        if (DecimalType.is32BitDecimalType(decimalType)) {
            Number[] intArray = ((ArrayOfNumbersSerDe) delegate)
                    .deserializeFromMemory(mem, offsetBytes, numItems);
            Decimal[] result = new Decimal[intArray.length];
            for (int i = 0; i < intArray.length; i++) {
                result[i] = Decimal.createUnsafe((int) intArray[i], precision, scale);
            }
            return result;
        } else if (DecimalType.is64BitDecimalType(decimalType)) {
            Long[] longArray = ((ArrayOfLongsSerDe) delegate)
                    .deserializeFromMemory(mem, offsetBytes, numItems);
            Decimal[] result = new Decimal[longArray.length];
            for (int i = 0; i < longArray.length; i++) {
                result[i] = Decimal.createUnsafe(longArray[i], precision, scale);
            }
            return result;
        } else {
            return ((ArrayOfDecimalByteArrSerDe) delegate)
                    .deserializeFromMemory(mem, offsetBytes, numItems);
        }
    }

    @Override
    public int sizeOf(Decimal item) {
        Objects.requireNonNull(item, "Item must not be null");
        if (DecimalType.is32BitDecimalType(decimalType)) {
            return ((ArrayOfNumbersSerDe) delegate).sizeOf(decimalToInt(item));
        } else if (DecimalType.is64BitDecimalType(decimalType)) {
            return ((ArrayOfLongsSerDe) delegate).sizeOf(item.toUnscaledLong());
        } else {
            return ((ArrayOfDecimalByteArrSerDe) delegate).sizeOf(item);
        }
    }

    @Override
    public int sizeOf(Memory mem, long offsetBytes, int numItems) {
        Objects.requireNonNull(mem, "Memory must not be null");
        return delegate.sizeOf(mem, offsetBytes, numItems);
    }

    @Override
    public String toString(Decimal item) {
        if (item == null) {
            return "null";
        }
        return item.toString();
    }

    @Override
    public Class<Decimal> getClassOfT() {
        return Decimal.class;
    }

    private int decimalToInt(Decimal item) {
        return ((int) item.toUnscaledLong());
    }


    /**
     * Serialize and deserialize Decimal as byte array.
     */
    private static class ArrayOfDecimalByteArrSerDe extends ArrayOfItemsSerDe<Decimal> {
        private final int precision;
        private final int scale;

        ArrayOfDecimalByteArrSerDe(DecimalType decimalType) {
            assert DecimalType.isByteArrayDecimalType(decimalType);
            this.precision = decimalType.precision();
            this.scale = decimalType.scale();
        }

        @Override
        public byte[] serializeToByteArray(Decimal item) {
            Objects.requireNonNull(item, "Item must not be null");
            final byte[] decimalByteArr = item.toJavaBigDecimal().unscaledValue().toByteArray();
            final int numBytes = decimalByteArr.length;
            final byte[] out = new byte[numBytes + Integer.BYTES];
            copyBytes(decimalByteArr, 0, out, 4, numBytes);
            putIntLE(out, 0, numBytes);
            return out;
        }

        @Override
        public byte[] serializeToByteArray(Decimal[] items) {
            Objects.requireNonNull(items, "Items must not be null");
            if (items.length == 0) {
                return new byte[0];
            }
            int totalBytes = 0;
            final int numItems = items.length;
            final byte[][] serialized2DArray = new byte[numItems][];
            for (int i = 0; i < numItems; i++) {
                serialized2DArray[i] = items[i].toJavaBigDecimal().unscaledValue().toByteArray();
                totalBytes += serialized2DArray[i].length + Integer.BYTES;
            }
            final byte[] bytesOut = new byte[totalBytes];
            int offset = 0;
            for (int i = 0; i < numItems; i++) {
                final int decimalLen = serialized2DArray[i].length;
                putIntLE(bytesOut, offset, decimalLen);
                offset += Integer.BYTES;
                copyBytes(serialized2DArray[i], 0, bytesOut, offset, decimalLen);
                offset += decimalLen;
            }
            return bytesOut;
        }

        @Override
        public Decimal[] deserializeFromMemory(Memory mem, long offsetBytes, int numItems) {
            Objects.requireNonNull(mem, "Memory must not be null");
            if (numItems <= 0) {
                return new Decimal[0];
            }
            final Decimal[] array = new Decimal[numItems];
            long offset = offsetBytes;
            for (int i = 0; i < numItems; i++) {
                Util.checkBounds(offset, Integer.BYTES, mem.getCapacity());
                final int decimalLength = mem.getInt(offset);
                offset += Integer.BYTES;
                final byte[] decimalBytes = new byte[decimalLength];
                Util.checkBounds(offset, decimalLength, mem.getCapacity());
                mem.getByteArray(offset, decimalBytes, 0, decimalLength);
                offset += decimalLength;
                BigInteger bigInteger = new BigInteger(decimalBytes);
                BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
                array[i] = Decimal.apply(javaDecimal, precision, scale);
            }
            return array;
        }

        @Override
        public int sizeOf(Decimal item) {
            Objects.requireNonNull(item, "Item must not be null");
            return item.toJavaBigDecimal().unscaledValue().toByteArray().length + Integer.BYTES;
        }

        @Override
        public int sizeOf(Memory mem, long offsetBytes, int numItems) {
            Objects.requireNonNull(mem, "Memory must not be null");
            if (numItems <= 0) {
                return 0;
            }
            long offset = offsetBytes;
            final long memCap = mem.getCapacity();
            for (int i = 0; i < numItems; i++) {
                Util.checkBounds(offset, Integer.BYTES, memCap);
                final int itemLenBytes = mem.getInt(offset);
                offset += Integer.BYTES;
                Util.checkBounds(offset, itemLenBytes, memCap);
                offset += itemLenBytes;
            }
            return (int) (offset - offsetBytes);
        }

        @Override
        public String toString(Decimal item) {
            if (item == null) {
                return "null";
            }
            return item.toString();
        }

        @Override
        public Class<Decimal> getClassOfT() {
            return Decimal.class;
        }
    }
}
