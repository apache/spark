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

package org.apache.spark.sql.connector.catalog;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.types.DataType;

/**
 * Describes a custom predicate function that a data source supports for pushdown.
 *
 * <p>The canonical name must be dot-qualified to avoid collisions with Spark's built-in
 * predicate names (e.g. "com.mycompany.INDEXQUERY", not just "INDEXQUERY"). This follows
 * the same convention as
 * {@link org.apache.spark.sql.connector.catalog.functions.BoundFunction#canonicalName()}.
 *
 * <p>Parameter types are advisory: the analyzer uses them for implicit cast hints but
 * does not reject queries when types don't match exactly. The data source can reject
 * incompatible predicates at {@code pushPredicates()} time, and Spark will fall back
 * to post-scan evaluation.
 *
 * @since 4.1.0
 */
@Evolving
public class CustomPredicateDescriptor {
    private final String canonicalName;
    private final String sqlName;
    private final DataType[] parameterTypes;
    private final boolean isDeterministic;

    /**
     * Creates a new custom predicate descriptor.
     *
     * @param canonicalName  Dot-qualified canonical name (e.g. "com.mycompany.INDEXQUERY").
     *                       Must contain at least one '.' to enforce namespace qualification.
     *                       This is the name used in the V2 Predicate wire format.
     * @param sqlName        The short name used in SQL syntax (e.g. "indexquery"). This is what
     *                       users write in SQL queries. Case-insensitive.
     * @param parameterTypes Expected parameter types for implicit cast hints.
     *                       Null entries mean "any type" for that position.
     *                       An empty array means any number of arguments of any type.
     * @param isDeterministic Whether the function is deterministic (affects optimizer decisions)
     */
    public CustomPredicateDescriptor(
            String canonicalName,
            String sqlName,
            DataType[] parameterTypes,
            boolean isDeterministic) {
        if (canonicalName == null) {
            throw new IllegalArgumentException("canonicalName must not be null");
        }
        if (sqlName == null) {
            throw new IllegalArgumentException("sqlName must not be null");
        }
        if (!canonicalName.contains(".")) {
            throw new IllegalArgumentException(
                "Canonical name must be dot-qualified (e.g. 'com.mycompany.FUNC'), got: "
                  + canonicalName);
        }
        this.canonicalName = canonicalName.toUpperCase(Locale.ROOT);
        this.sqlName = sqlName.toUpperCase(Locale.ROOT);
        this.parameterTypes = parameterTypes != null ? parameterTypes.clone() : null;
        this.isDeterministic = isDeterministic;
    }

    /**
     * Convenience constructor using the last segment of canonicalName as the sql name.
     */
    public CustomPredicateDescriptor(
            String canonicalName,
            DataType[] parameterTypes,
            boolean isDeterministic) {
        this(canonicalName,
            canonicalName != null
                ? canonicalName.substring(canonicalName.lastIndexOf('.') + 1)
                : null,
            parameterTypes, isDeterministic);
    }

    public String canonicalName() { return canonicalName; }
    public String sqlName() { return sqlName; }
    public DataType[] parameterTypes() {
        return parameterTypes != null ? parameterTypes.clone() : null;
    }
    public boolean isDeterministic() { return isDeterministic; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CustomPredicateDescriptor)) return false;
        CustomPredicateDescriptor that = (CustomPredicateDescriptor) o;
        return isDeterministic == that.isDeterministic
            && Objects.equals(canonicalName, that.canonicalName)
            && Objects.equals(sqlName, that.sqlName)
            && Arrays.equals(parameterTypes, that.parameterTypes);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(canonicalName, sqlName, isDeterministic);
        result = 31 * result + Arrays.hashCode(parameterTypes);
        return result;
    }

    @Override
    public String toString() {
        return "CustomPredicateDescriptor{"
            + "canonicalName='" + canonicalName + '\''
            + ", sqlName='" + sqlName + '\''
            + ", parameterTypes=" + Arrays.toString(parameterTypes)
            + ", isDeterministic=" + isDeterministic
            + '}';
    }
}
