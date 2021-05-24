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

package test.org.apache.spark.sql;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import static java.util.stream.Collectors.toList;

import static scala.collection.JavaConverters.mapAsScalaMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.test.TestSparkSession;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.types.DataTypes.*;

public class JavaHigherOrderFunctionsSuite {
    private transient TestSparkSession spark;
    private Dataset<Row> arrDf;
    private Dataset<Row> mapDf;

    private void checkAnswer(Dataset<Row> actualDS, List<Row> expected) throws Exception {
        List<Row> actual = actualDS.collectAsList();
        Assert.assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            Row expectedRow = expected.get(i);
            Row actualRow = actual.get(i);
            Assert.assertEquals(expectedRow.size(), actualRow.size());
            for (int j = 0; j < expectedRow.size(); j++) {
                Object expectedValue = expectedRow.get(j);
                Object actualValue = actualRow.get(j);
                if (expectedValue != null && expectedValue.getClass().isArray()) {
                    actualValue = actualValue.getClass().getMethod("array").invoke(actualValue);
                    Assert.assertArrayEquals((Object[]) expectedValue, (Object[]) actualValue);
                } else {
                    Assert.assertEquals(expectedValue, actualValue);
                }
            }
        }
    }

    @SafeVarargs
    private static <T> List<Row> toRows(T... objs) {
        return Arrays.stream(objs)
            .map(RowFactory::create)
            .collect(toList());
    }

    @SafeVarargs
    private static <T> T[] makeArray(T... ts) {
        return ts;
    }

    private void setUpArrDf() {
        List<Row> data = toRows(
            makeArray(1, 9, 8, 7),
            makeArray(5, 8, 9, 7, 2),
            JavaHigherOrderFunctionsSuite.<Integer>makeArray(),
            null
        );
        StructType schema =  new StructType()
            .add("x", new ArrayType(IntegerType, true), true);
        arrDf = spark.createDataFrame(data, schema);
    }

    private void setUpMapDf() {
        List<Row> data = toRows(
            new HashMap<Integer, Integer>() {{
                put(1, 1);
                put(2, 2);
            }},
            null
        );
        StructType schema = new StructType()
            .add("x", new MapType(IntegerType, IntegerType, true));
        mapDf = spark.createDataFrame(data, schema);
    }

    @Before
    public void setUp() {
        spark = new TestSparkSession();
        setUpArrDf();
        setUpMapDf();
    }

    @After
    public void tearDown() {
        spark.stop();
        spark = null;
    }

    @Test
    public void testTransform() throws Exception {
        checkAnswer(
            arrDf.select(transform(col("x"), x -> x.plus(1))),
            toRows(
                makeArray(2, 10, 9, 8),
                makeArray(6, 9, 10, 8, 3),
                JavaHigherOrderFunctionsSuite.<Integer>makeArray(),
                null
            )
        );
        checkAnswer(
            arrDf.select(transform(col("x"), (x, i) -> x.plus(i))),
            toRows(
                makeArray(1, 10, 10, 10),
                makeArray(5, 9, 11, 10, 6),
                JavaHigherOrderFunctionsSuite.<Integer>makeArray(),
                null
            )
        );
    }

    @Test
    public void testFilter() throws Exception {
        checkAnswer(
            arrDf.select(filter(col("x"), x -> x.plus(1).equalTo(10))),
            toRows(
                makeArray(9),
                makeArray(9),
                JavaHigherOrderFunctionsSuite.<Integer>makeArray(),
                null
            )
        );
        checkAnswer(
            arrDf.select(filter(col("x"), (x, i) -> x.plus(i).equalTo(10))),
            toRows(
                makeArray(9, 8, 7),
                makeArray(7),
                JavaHigherOrderFunctionsSuite.<Integer>makeArray(),
                null
            )
        );
    }

    @Test
    public void testExists() throws Exception {
        checkAnswer(
            arrDf.select(exists(col("x"), x -> x.plus(1).equalTo(10))),
            toRows(
                true,
                true,
                false,
                null
            )
        );
    }

    @Test
    public void testForall() throws Exception {
        checkAnswer(
            arrDf.select(forall(col("x"), x -> x.plus(1).equalTo(10))),
            toRows(
                false,
                false,
                true,
                null
            )
        );
    }

    @Test
    public void testAggregate() throws Exception {
        checkAnswer(
            arrDf.select(aggregate(col("x"), lit(0), (acc, x) -> acc.plus(x))),
            toRows(
                25,
                31,
                0,
                null
            )
        );
        checkAnswer(
            arrDf.select(aggregate(col("x"), lit(0), (acc, x) -> acc.plus(x), x -> x)),
            toRows(
                25,
                31,
                0,
                null
            )
        );
    }

    @Test
    public void testZipWith() throws Exception {
        checkAnswer(
            arrDf.select(zip_with(col("x"), col("x"), (a, b) -> lit(42))),
            toRows(
                makeArray(42, 42, 42, 42),
                makeArray(42, 42, 42, 42, 42),
                JavaHigherOrderFunctionsSuite.<Integer>makeArray(),
                null
            )
        );
    }

    @Test
    public void testTransformKeys() throws Exception {
        checkAnswer(
            mapDf.select(transform_keys(col("x"), (k, v) -> k.plus(v))),
            toRows(
                mapAsScalaMap(new HashMap<Integer, Integer>() {{
                    put(2, 1);
                    put(4, 2);
                }}),
                null
            )
        );
    }

    @Test
    public void testTransformValues() throws Exception {
        checkAnswer(
            mapDf.select(transform_values(col("x"), (k, v) -> k.plus(v))),
            toRows(
                mapAsScalaMap(new HashMap<Integer, Integer>() {{
                    put(1, 2);
                    put(2, 4);
                }}),
                null
            )
        );
    }

    @Test
    public void testMapFilter() throws Exception {
        checkAnswer(
            mapDf.select(map_filter(col("x"), (k, v) -> lit(false))),
            toRows(
                mapAsScalaMap(new HashMap<Integer, Integer>()),
                null
            )
        );
    }

    @Test
    public void testMapZipWith() throws Exception {
        checkAnswer(
            mapDf.select(map_zip_with(col("x"), col("x"), (k, v1, v2) -> lit(false))),
            toRows(
                mapAsScalaMap(new HashMap<Integer, Boolean>() {{
                    put(1, false);
                    put(2, false);
                }}),
                null
            )
        );
    }
}
