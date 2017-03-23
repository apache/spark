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

package org.apache.spark.mllib.feature;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.sql.SQLContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;

public class JavaRandomProjection {
  private transient JavaSparkContext jsc;
  private transient SQLContext jsql;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaRandomProjection");
    jsql = new SQLContext(jsc);
  }

  @After
  public void tearDown() {
    jsc.stop();
    jsc = null;
  }

  @Test
  public void javaRandomProjection() {
    List<MatrixEntry> list = new ArrayList<MatrixEntry>();
    list.add(new MatrixEntry(0, 0, 1));
    list.add(new MatrixEntry(0, 1, 3));
    list.add(new MatrixEntry(0, 2, 5));
    list.add(new MatrixEntry(1, 0, 2));
    list.add(new MatrixEntry(1, 1, 6));
    list.add(new MatrixEntry(1, 2, 9));

    JavaRDD<MatrixEntry> data = jsc.parallelize(list);

    CoordinateMatrix coordMat = new CoordinateMatrix(data.rdd());
    BlockMatrix matA = coordMat.toBlockMatrix().cache();

    int newDimension = 2;
    RandomProjection rp = new RandomProjection(newDimension);
    int origDimension = 3;
    BlockMatrix bm = rp.computeRPMatrix(jsc.sc(), origDimension);

    BlockMatrix reduced = bm.transpose().multiply(matA.transpose()).transpose();

    Boolean a = (int)reduced.numRows() == 2;
    Assert.assertTrue(a);
    Boolean b = reduced.numCols() == newDimension;
    Assert.assertTrue(b);
  }
}
