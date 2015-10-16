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

package org.apache.spark.mllib.linalg;

import java.io.Serializable;
import java.util.Arrays;

import scala.Tuple2;

import org.junit.Test;
import static org.junit.Assert.*;

public class JavaVectorsSuite implements Serializable {

  @Test
  public void denseArrayConstruction() {
    Vector v = Vectors.dense(1.0, 2.0, 3.0);
    assertArrayEquals(new double[]{1.0, 2.0, 3.0}, v.toArray(), 0.0);
  }

  @Test
  public void sparseArrayConstruction() {
    @SuppressWarnings("unchecked")
    Vector v = Vectors.sparse(3, Arrays.asList(
        new Tuple2<Integer, Double>(0, 2.0),
        new Tuple2<Integer, Double>(2, 3.0)));
    assertArrayEquals(new double[]{2.0, 0.0, 3.0}, v.toArray(), 0.0);
  }
}
