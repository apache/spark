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

package org.apache.spark.sql.execution

import org.apache.spark.sql.SQLContext
import org.apache.spark.util.Benchmark
import org.apache.spark.{SparkFunSuite, SparkConf, SparkContext}

/**
  * Benchmark to measure whole stage codegen performance.
  * To run this:
  *  build/sbt "sql/test-only *BenchmarkWholeStageCodegen"
  */
class BenchmarkWholeStageCodegen extends SparkFunSuite {
  val conf = new SparkConf()
  val sc = new SparkContext("local[1]", "test-sql-context", conf)
  val sqlContext = new SQLContext(sc)

  def testWholeStage(values: Int): Unit = {
    val benchmark = new Benchmark("Single Int Column Scan", values)

    benchmark.addCase("Without whole stage codegen") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "false")
      sqlContext.range(values).filter("(id & 1) = 1").count()
    }

    benchmark.addCase("With whole stage codegen") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "true")
      sqlContext.range(values).filter("(id & 1) = 1").count()
    }

    /**
      *  The code generated for this query (some comments and empty lines are removed):
      *
    /* 001 */
    /* 002 */ public Object generate(org.apache.spark.sql.catalyst.expressions.Expression[] exprs) {
    /* 003 */   return new GeneratedIterator(exprs);
    /* 004 */ }
    /* 005 */
    /* 006 */ class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {
    /* 007 */
    /* 008 */   private org.apache.spark.sql.catalyst.expressions.Expression[] expressions;
    /* 009 */   private boolean initRange0;
    /* 010 */   private long partitionEnd1;
    /* 011 */   private long number2;
    /* 012 */   private boolean overflow3;
    /* 013 */   private UnsafeRow unsafeRow = new UnsafeRow(0);
    /* 014 */
    /* 015 */   public GeneratedIterator(org.apache.spark.sql.catalyst.expressions.Expression[] exprs) {
    /* 016 */     expressions = exprs;
    /* 017 */     initRange0 = false;
    /* 018 */     partitionEnd1 = 0L;
    /* 019 */     number2 = 0L;
    /* 020 */     overflow3 = false;
    /* 021 */   }
    /* 022 */
    /* 023 */   protected void processNext() {
    /* 024 */
    /* 025 */     if (!initRange0) {
    /* 026 */       if (input.hasNext()) {
    /* 027 */         java.math.BigInteger index = java.math.BigInteger.valueOf(((InternalRow) input.next()).getInt(0));
    /* 028 */         java.math.BigInteger numSlice = java.math.BigInteger.valueOf(1L);
    /* 029 */         java.math.BigInteger numElement = java.math.BigInteger.valueOf(209715200L);
    /* 030 */         java.math.BigInteger step = java.math.BigInteger.valueOf(1L);
    /* 031 */         java.math.BigInteger start = java.math.BigInteger.valueOf(0L);
    /* 032 */
    /* 033 */         java.math.BigInteger st = index.multiply(numElement).divide(numSlice).multiply(step).add(start);
    /* 034 */         if (st.compareTo(java.math.BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
    /* 035 */           number2 = Long.MAX_VALUE;
    /* 036 */         } else if (st.compareTo(java.math.BigInteger.valueOf(Long.MIN_VALUE)) < 0) {
    /* 037 */           number2 = Long.MIN_VALUE;
    /* 038 */         } else {
    /* 039 */           number2 = st.longValue();
    /* 040 */         }
    /* 041 */
    /* 042 */         java.math.BigInteger end = index.add(java.math.BigInteger.ONE).multiply(numElement).divide(numSlice)
    /* 043 */         .multiply(step).add(start);
    /* 044 */         if (end.compareTo(java.math.BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
    /* 045 */           partitionEnd1 = Long.MAX_VALUE;
    /* 046 */         } else if (end.compareTo(java.math.BigInteger.valueOf(Long.MIN_VALUE)) < 0) {
    /* 047 */           partitionEnd1 = Long.MIN_VALUE;
    /* 048 */         } else {
    /* 049 */           partitionEnd1 = end.longValue();
    /* 050 */         }
    /* 051 */       } else {
    /* 052 */         return;
    /* 053 */       }
    /* 054 */       initRange0 = true;
    /* 055 */     }
    /* 056 */
    /* 057 */     while (!overflow3 && number2 < partitionEnd1) {
    /* 058 */       long value4 = number2;
    /* 059 */       number2 += 1L;
    /* 060 */       if (number2 < value4 ^ 1L < 0) {
    /* 061 */         overflow3 = true;
    /* 062 */       }
    /* 063 */
    /* 070 */       long primitive8 = -1L;
    /* 071 */       primitive8 = value4 & 1L;
    /* 074 */       boolean primitive6 = false;
    /* 075 */       primitive6 = primitive8 == 1L;
    /* 076 */       if (!false && primitive6) {
    /* 081 */         currentRow = unsafeRow;
    /* 082 */         return;
    /* 085 */       }
    /* 087 */     }
    /* 089 */   }
    /* 090 */ }
    /* 091 */
      */
    /*
      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      Single Int Column Scan:      Avg Time(ms)    Avg Rate(M/s)  Relative Rate
      -------------------------------------------------------------------------
      Without whole stage codegen       6725.52            31.18         1.00 X
      With whole stage codegen          2233.05            93.91         3.01 X
    */
    benchmark.run()
  }

  test("benchmark") {
    testWholeStage(1024 * 1024 * 200)
  }
}
