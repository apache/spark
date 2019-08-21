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

    @Before
    public void setUp() {
        spark = new TestSparkSession();
    }

    @After
    public void tearDown() {
        spark.stop();
        spark = null;
    }

    @Test
    public void testTransformArrayPrimitiveNotContainingNull() {
        List<Row> data = toRows(
            makeArray(1, 9, 8, 7),
            makeArray(5, 8, 9, 7, 2),
            JavaTestUtils.<Integer>makeArray(),
            null
        );
        StructType schema =  new StructType()
            .add("i", new ArrayType(IntegerType, true), true);
        Dataset<Row> df = spark.createDataFrame(data, schema);

        Runnable f = () -> {
            checkAnswer(
                df.select(transform(col("i"), x -> x.plus(1))),
                toRows(
                    makeArray(2, 10, 9, 8),
                    makeArray(6, 9, 10, 8, 3),
                    JavaTestUtils.<Integer>makeArray(),
                    null
                ));
            checkAnswer(
                df.select(transform(col("i"), (x, i) -> x.plus(i))),
                toRows(
                    makeArray(1, 10, 10, 10),
                    makeArray(5, 9, 11, 10, 6),
                    JavaTestUtils.<Integer>makeArray(),
                    null
                ));
        };

        // Test with local relation, the Project will be evaluated without codegen
        f.run();
        // Test with cached relation, the Project will be evaluated with codegen
        df.cache();
        f.run();
      }

    @Test
    public void testTransformArrayPrimitiveContainingNull() {
        List<Row> data = toRows(
            makeArray(1, 9, 8, null, 7),
            makeArray(5, null, 8, 9, 7, 2),
            JavaTestUtils.<Integer>makeArray(),
            null
        );
        StructType schema =  new StructType()
            .add("i", new ArrayType(IntegerType, true), true);
        Dataset<Row> df = spark.createDataFrame(data, schema);

        Runnable f = () -> {
            checkAnswer(
                df.select(transform(col("i"), x -> x.plus(1))),
                toRows(
                    makeArray(2, 10, 9, null, 8),
                    makeArray(6, null, 9, 10, 8, 3),
                    JavaTestUtils.<Integer>makeArray(),
                    null
                ));
            checkAnswer(
                df.select(transform(col("i"), (x, i) -> x.plus(i))),
                toRows(
                    makeArray(1, 10, 10, null, 11),
                    makeArray(5, null, 10, 12, 11, 7),
                    JavaTestUtils.<Integer>makeArray(),
                    null
                ));
        };

        // Test with local relation, the Project will be evaluated without codegen
        f.run();
        df.cache();
        // Test with cached relation, the Project will be evaluated with codegen
        f.run();
    }

    @Test
    public void testTransformArrayNonPrimitive() {
        List<Row> data = toRows(
            makeArray("c", "a", "b"),
            makeArray("b", null, "c", null),
            JavaTestUtils.<String>makeArray(),
            null
        );
        StructType schema =  new StructType()
            .add("s", new ArrayType(StringType, true), true);
        Dataset<Row> df = spark.createDataFrame(data, schema);

        Runnable f = () -> {
            checkAnswer(df.select(transform(col("s"), x -> concat(x, x))),
                toRows(
                    makeArray("cc", "aa", "bb"),
                    makeArray("bb", null, "cc", null),
                    JavaTestUtils.<String>makeArray(),
                    null
                ));
          checkAnswer(df.select(transform(col("s"), (x, i) -> concat(x, i))),
                toRows(
                    makeArray("c0", "a1", "b2"),
                    makeArray("b0", null, "c2", null),
                    JavaTestUtils.<String>makeArray(),
                    null
                ));
        };

        // Test with local relation, the Project will be evaluated without codegen
        f.run();
        // Test with cached relation, the Project will be evaluated with codegen
        df.cache();
        f.run();
    }

    @Test
    public void testTransformSpecialCases() {
        List<Row> data = toRows(
            makeArray("c", "a", "b"),
            makeArray("b", null, "c", null),
            JavaTestUtils.<String>makeArray(),
            null
        );
        StructType schema =  new StructType()
            .add("s", new ArrayType(StringType, true), true);
        Dataset<Row> df = spark.createDataFrame(data, schema);

        Runnable f = () -> {
            checkAnswer(df.select(transform(col("arg"), arg -> arg)),
                toRows(
                    makeArray("c", "a", "b"),
                    makeArray("b", null, "c", null),
                    JavaTestUtils.<String>makeArray(),
                    null));
            checkAnswer(df.select(transform(col("arg"), x -> col("arg"))),
                toRows(
                    makeArray(
                        makeArray("c", "a", "b"),
                        makeArray("c", "a", "b"),
                        makeArray("c", "a", "b")
                    ),
                    makeArray(
                        makeArray("b", null, "c", null),
                        makeArray("b", null, "c", null),
                        makeArray("b", null, "c", null),
                        makeArray("b", null, "c", null)
                    ),
                    JavaTestUtils.<String>makeArray(),
                    null));
            checkAnswer(df.select(transform(col("arg"), x -> concat(col("arg"), array(x)))),
                toRows(
                    makeArray(
                        makeArray("c", "a", "b", "c"),
                        makeArray("c", "a", "b", "c"),
                        makeArray("c", "a", "b", "c")
                    ),
                    makeArray(
                        makeArray("b", null, "c", null, "b"),
                        makeArray("b", null, "c", null, null),
                        makeArray("b", null, "c", null, "b"),
                        makeArray("b", null, "c", null, null)
                    ),
                    JavaTestUtils.<String>makeArray(),
                    null));
        };
    }
}
