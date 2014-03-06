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

package org.apache.spark.examples.mllib
      
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.PCA
import org.apache.spark.mllib.linalg.MatrixEntry
import org.apache.spark.mllib.linalg.SparseMatrix
import org.apache.spark.mllib.util._


/**
 * Compute PCA of an example matrix.
 * Input file should be comma separated, 1 indexed of the form
 * i,j,value
 * Where i is the column, j the row, and value is the matrix entry
 * 
 * For example input file, see:
 * mllib/data/als/test.data  (example is 4 x 4)
 */
object SparkPCA {
  def main(args: Array[String]) {
   if (args.length != 4) {
      System.err.println("Usage: SparkPCA <master> <file> m n")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "SVD",
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    // Load and parse the data file
    val data = sc.textFile(args(1)).map { line =>
      val parts = line.split(',')
      MatrixEntry(parts(0).toInt - 1, parts(1).toInt - 1, parts(2).toDouble)
    }
    val m = args(2).toInt
    val n = args(3).toInt

    // recover top principal component
    val coeffs = new PCA().computePCA(LAUtils.spToDense(SparseMatrix(data, m, n)), 1)

    println("top principal component = " + coeffs.rows.toArray.mkString(", "))
  }
}
