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

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;

<<<<<<< HEAD:dev/audit-release/maven_app_core/src/main/java/SimpleApp.java
public class SimpleApp {
  public static void main(String[] args) {
    String logFile = "input.txt";
    JavaSparkContext sc = new JavaSparkContext("local", "Simple App");
    JavaRDD<String> logData = sc.textFile(logFile).cache();
=======
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types.{IntegerType, ObjectType}
>>>>>>> e0b20f9... [SPARK-17061][SPARK-17093][SQL] MapObjects` should make copies of unsafe-backed data:sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/ObjectExpressionsSuite.scala

    long numAs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("a"); }
    }).count();

    long numBs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("b"); }
    }).count();

   if (numAs != 2 || numBs != 2) {
     System.out.println("Failed to parse log files with Spark");
     System.exit(-1);
   }
   System.out.println("Test succeeded");
   sc.stop();
  }

  test("MapObjects should make copies of unsafe-backed data") {
    // test UnsafeRow-backed data
    val structEncoder = ExpressionEncoder[Array[Tuple2[java.lang.Integer, java.lang.Integer]]]
    val structInputRow = InternalRow.fromSeq(Seq(Array((1, 2), (3, 4))))
    val structExpected = new GenericArrayData(
      Array(InternalRow.fromSeq(Seq(1, 2)), InternalRow.fromSeq(Seq(3, 4))))
    checkEvalutionWithUnsafeProjection(
      structEncoder.serializer.head, structExpected, structInputRow)

    // test UnsafeArray-backed data
    val arrayEncoder = ExpressionEncoder[Array[Array[Int]]]
    val arrayInputRow = InternalRow.fromSeq(Seq(Array(Array(1, 2), Array(3, 4))))
    val arrayExpected = new GenericArrayData(
      Array(new GenericArrayData(Array(1, 2)), new GenericArrayData(Array(3, 4))))
    checkEvalutionWithUnsafeProjection(
      arrayEncoder.serializer.head, arrayExpected, arrayInputRow)

    // test UnsafeMap-backed data
    val mapEncoder = ExpressionEncoder[Array[Map[Int, Int]]]
    val mapInputRow = InternalRow.fromSeq(Seq(Array(
      Map(1 -> 100, 2 -> 200), Map(3 -> 300, 4 -> 400))))
    val mapExpected = new GenericArrayData(Seq(
      new ArrayBasedMapData(
        new GenericArrayData(Array(1, 2)),
        new GenericArrayData(Array(100, 200))),
      new ArrayBasedMapData(
        new GenericArrayData(Array(3, 4)),
        new GenericArrayData(Array(300, 400)))))
    checkEvalutionWithUnsafeProjection(
      mapEncoder.serializer.head, mapExpected, mapInputRow)
  }
}
