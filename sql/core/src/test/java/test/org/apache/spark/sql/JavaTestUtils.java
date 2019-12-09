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
import java.util.List;
import static java.util.stream.Collectors.toList;

import scala.collection.mutable.WrappedArray;

import static org.junit.Assert.assertEquals;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class JavaTestUtils {
    public static <T> void checkAnswer(Dataset<T> actual, List<Row> expected) {
        assertEquals(expected, actual.collectAsList());
    }

    public static List<Row> toRows(Object... objs) {
        return Arrays.asList(objs)
            .stream()
            .map(RowFactory::create)
            .collect(toList());
    }

    public static <T> WrappedArray<T> makeArray(T... ts) {
        return WrappedArray.make(ts);
    }
}
