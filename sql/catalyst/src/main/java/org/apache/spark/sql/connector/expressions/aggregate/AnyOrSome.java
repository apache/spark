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
 * An aggregate function that returns true if at least one value of `column` is true.
 *
 * @since 3.3.0
 */
@Evolving
public final class AnyOrSome implements AggregateFunc {
    public static final String ANY = "ANY";
    public static final String SOME = "SOME";

    private final NamedReference column;
    private final String realName;

    public AnyOrSome(NamedReference column, String realName) {
        this.column = column;
        this.realName = realName;
    }

    public NamedReference column() { return column; }

    @Override
    public String toString() { return realName + "(" + column.describe() + ")"; }

    @Override
    public String describe() { return this.toString(); }
}
