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

package org.apache.spark.sql.api.java;

import com.google.common.collect.ImmutableMap;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

/**
 * This test doesn't actually run anything. It is here to check the API compatibility for Java.
 */
public class JavaDsl {

  public static void testDataFrame(final DataFrame df) {
    DataFrame df1 = df.select("colA");
    df1 = df.select("colA", "colB");

    df1 = df.select(col("colA"), col("colB"), lit("literal value").$plus(1));

    df1 = df.filter(col("colA"));

    java.util.Map<String, String> aggExprs = ImmutableMap.<String, String>builder()
      .put("colA", "sum")
      .put("colB", "avg")
      .build();

    df1 = df.agg(aggExprs);

    df1 = df.groupBy("groupCol").agg(aggExprs);

    df1 = df.join(df1, col("key1").$eq$eq$eq(col("key2")), "outer");

    df.orderBy("colA");
    df.orderBy("colA", "colB", "colC");
    df.orderBy(col("colA").desc());
    df.orderBy(col("colA").desc(), col("colB").asc());

    df.sort("colA");
    df.sort("colA", "colB", "colC");
    df.sort(col("colA").desc());
    df.sort(col("colA").desc(), col("colB").asc());

    df.as("b");

    df.limit(5);

    df.unionAll(df1);
    df.intersect(df1);
    df.except(df1);

    df.sample(true, 0.1, 234);

    df.head();
    df.head(5);
    df.first();
    df.count();
  }

  public static void testColumn(final Column c) {
    c.asc();
    c.desc();

    c.endsWith("abcd");
    c.startsWith("afgasdf");

    c.like("asdf%");
    c.rlike("wef%asdf");

    c.as("newcol");

    c.cast("int");
    c.cast(DataTypes.IntegerType);
  }

  public static void testDsl() {
    // Creating a column.
    Column c = col("abcd");
    Column c1 = column("abcd");

    // Literals
    Column l1 = lit(1);
    Column l2 = lit(1.0);
    Column l3 = lit("abcd");

    // Functions
    Column a = upper(c);
    a = lower(c);
    a = sqrt(c);
    a = abs(c);

    // Aggregates
    a = min(c);
    a = max(c);
    a = sum(c);
    a = sumDistinct(c);
    a = countDistinct(c, a);
    a = avg(c);
    a = first(c);
    a = last(c);
  }
}
