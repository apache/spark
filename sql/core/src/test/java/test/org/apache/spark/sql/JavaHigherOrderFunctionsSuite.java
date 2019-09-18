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

import java.util.List;

import scala.collection.Seq;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.test.TestSparkSession;
import static test.org.apache.spark.sql.JavaTestUtils.*;
import test.org.apache.spark.sql.JavaTestUtils;

public class JavaHigherOrderFunctionsSuite {
    private transient TestSparkSession spark;
    private Dataset<Row> df;

    @Before
    public void setUp() {
        spark = new TestSparkSession();
        List<Row> data = toRows(
            makeArray(1, 9, 8, 7),
            makeArray(5, 8, 9, 7, 2),
            JavaTestUtils.<Integer>makeArray(),
            null
        );
        StructType schema =  new StructType()
            .add("x", new ArrayType(IntegerType, true), true);
        df = spark.createDataFrame(data, schema);
    }

    @After
    public void tearDown() {
        spark.stop();
        spark = null;
    }

    @Test
    public void testTransform() {
        checkAnswer(
            df.select(transform(col("x"), x -> x.plus(1))),
            toRows(
                makeArray(2, 10, 9, 8),
                makeArray(6, 9, 10, 8, 3),
                JavaTestUtils.<Integer>makeArray(),
                null
            ));
        checkAnswer(
            df.select(transform(col("x"), (x, i) -> x.plus(i))),
            toRows(
                makeArray(1, 10, 10, 10),
                makeArray(5, 9, 11, 10, 6),
                JavaTestUtils.<Integer>makeArray(),
                null
            ));
    }

    @Test
    public void testFilter() {
        checkAnswer(
            df.select(filter(col("x"), x -> x.plus(1).equalTo(10))),
            toRows(
                makeArray(9),
                makeArray(9),
                JavaTestUtils.<Integer>makeArray(),
                null
            ));
    }

    @Test
    public void testExists() {
        checkAnswer(
            df.select(exists(col("x"), x -> x.plus(1).equalTo(10))),
            toRows(
                true,
                true,
                false,
                null
            ));
    }
}
