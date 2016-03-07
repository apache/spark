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

package org.apache.spark.examples.mllib;

// $example on$
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
// $example off$

public class JavaLocalVectorExample {
  public static void main(String[] args) {
    // $example on$
    // Create a dense vector
    Vector dv = Vectors.dense(1.0, 0.0, 3.0);

    // Create a sparse vector by specifying its indices
    // and values corresponding to nonzero entries.
    Vector sv = Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0});
    // $example off$
  }
}
