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

package org.apache.spark.sql.connector.expressions.aggregate;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.NamedReference;

/**
 * An aggregate function that returns the sample covariance of a set of number pairs.
 *
 * @since 3.3.0
 */
@Evolving
public final class CovarSamp implements AggregateFunc {
    private final NamedReference left;
    private final NamedReference right;

    public CovarSamp(NamedReference left, NamedReference right) {
        this.left = left;
        this.right = right;
    }

    public NamedReference left() { return left; }

    public NamedReference right() { return right; }

    @Override
    public String toString() {
        return "COVAR_SAMP(" + left.describe() + "," + right.describe() + ")";
    }

    @Override
    public String describe() { return this.toString(); }
}
