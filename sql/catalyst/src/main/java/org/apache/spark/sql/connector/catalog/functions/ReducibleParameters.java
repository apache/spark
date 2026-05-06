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
package org.apache.spark.sql.connector.catalog.functions;

import java.util.Collections;
import java.util.List;

import org.apache.spark.annotation.Evolving;

/**
 * Container for parameters of a {@link ReducibleFunction}.
 * <p>
 * Provides type-safe access to function parameters for generic reducer comparisons,
 * enabling SPJ support for any parameterized transform (not just bucket).
 * <p>
 * Examples:
 * <ul>
 *   <li>bucket(4, x) → ReducibleParameters([4])</li>
 *   <li>truncate(x, 3) → ReducibleParameters([3])</li>
 *   <li>bucket(16, x) → ReducibleParameters([16])</li>
 * </ul>
 *
 * @since 4.1.0
 */
@Evolving
public class ReducibleParameters {
    private final List<Object> values;

    public ReducibleParameters(List<Object> values) {
        this.values = Collections.unmodifiableList(values);
    }

    /** Number of parameters. */
    public int count() {
        return values.size();
    }

    /** Get raw parameter value at index. */
    public Object get(int index) {
        return values.get(index);
    }

    /** Get parameter as int. Throws ClassCastException if not numeric. */
    public int getInt(int index) {
        return ((Number) values.get(index)).intValue();
    }

    /** Get parameter as long. Throws ClassCastException if not numeric. */
    public long getLong(int index) {
        return ((Number) values.get(index)).longValue();
    }

    /** Get parameter as String. */
    public String getString(int index) {
        return (String) values.get(index);
    }

    /** Get parameter as double. Throws ClassCastException if not numeric. */
    public double getDouble(int index) {
        return ((Number) values.get(index)).doubleValue();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof ReducibleParameters)) return false;
        return values.equals(((ReducibleParameters) other).values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public String toString() {
        return "ReducibleParameters(" + values + ")";
    }
}
