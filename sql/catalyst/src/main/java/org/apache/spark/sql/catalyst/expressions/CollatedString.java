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

/**
 * A DataSketches ItemsSketch item for non-binary collated strings (SPARK-58069).
 * <p>
 * Equality and hashing are driven solely by the collation {@code key}, so that collation-equal
 * strings (e.g. {@code 'HELLO'} and {@code 'hello'} under {@code UTF8_LCASE}) are counted as a
 * single item. The {@code original} field retains an actual input value to return in the result,
 * mirroring how {@code mode()} returns a real value rather than the normalized collation key.
 */
public class CollatedString {
    private final String key;
    private final String original;

    public CollatedString(String key, String original) {
        this.key = key;
        this.original = original;
    }

    public String key() {
        return key;
    }

    public String original() {
        return original;
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CollatedString)) {
            return false;
        }
        return key.equals(((CollatedString) obj).key);
    }
}
