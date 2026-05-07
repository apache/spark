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

import org.apache.spark.annotation.Evolving;
import java.util.Arrays;
import java.util.List;

/**
 * Container for reducible function literal parameters.
 * Provides type-safe access to parameters of various types.
 *
 * Examples:
 * <ul>
 *   <li>bucket(4, col) → ReducibleParameters([4])</li>
 *   <li>truncate(col, 3) → ReducibleParameters([3])</li>
 *   <li>range_bucket(col, 0L, 100L, 10) → ReducibleParameters([0L, 100L, 10])</li>
 *   <li>custom_transform(col, "param") → ReducibleParameters(["param"])</li>
 * </ul>
 *
 * @since 4.0.0
 */
@Evolving
public class ReducibleParameters {
    private final List<Object> values;

    public ReducibleParameters(List<Object> values) {
        this.values = values;
    }

    public ReducibleParameters(Object... values) {
        this.values = Arrays.asList(values);
    }

    /**
     * Get the number of parameters.
     */
    public int count() {
        return values.size();
    }

    /**
     * Check if this container has parameters.
     */
    public boolean isEmpty() {
        return values.isEmpty();
    }

    /**
     * Get parameter at index as Integer.
     * @throws ClassCastException if parameter is not an Integer
     * @throws IndexOutOfBoundsException if index is invalid
     */
    public int getInt(int index) {
        return (Integer) values.get(index);
    }

    /**
     * Get parameter at index as Long.
     * @throws ClassCastException if parameter is not a Long
     * @throws IndexOutOfBoundsException if index is invalid
     */
    public long getLong(int index) {
        return (Long) values.get(index);
    }

    /**
     * Get parameter at index as String.
     * @throws ClassCastException if parameter is not a String
     * @throws IndexOutOfBoundsException if index is invalid
     */
    public String getString(int index) {
        return (String) values.get(index);
    }

    /**
     * Get parameter at index as Double.
     * @throws ClassCastException if parameter is not a Double
     * @throws IndexOutOfBoundsException if index is invalid
     */
    public double getDouble(int index) {
        return (Double) values.get(index);
    }

    /**
     * Get parameter at index as Float.
     * @throws ClassCastException if parameter is not a Float
     * @throws IndexOutOfBoundsException if index is invalid
     */
    public float getFloat(int index) {
        return (Float) values.get(index);
    }

    /**
     * Get raw parameter value at index.
     */
    public Object get(int index) {
        return values.get(index);
    }

    /**
     * Get all parameter values as a list.
     */
    public List<Object> getAll() {
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReducibleParameters that = (ReducibleParameters) o;
        return values.equals(that.values);
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
