package org.apache.spark.mllib.linalg.distributed;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.linalg.DenseVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JavaRowMatrixSuite {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaRowMatrixSuite");
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  @Test
  public void rowMatrixConstruction() {
    List<DenseVector> rows = Arrays.asList(
      new DenseVector(new double[]{1.0, 0.0, 0.0, 0.0}),
      new DenseVector(new double[]{0.0, 2.0, 1.0, 0.0}),
      new DenseVector(new double[]{3.0, 1.0, 1.0, 0.0}),
      new DenseVector(new double[]{0.0, 1.0, 2.0, 1.0}),
      new DenseVector(new double[]{0.0, 0.0, 1.0, 5.0}));
    RowMatrix rowMatrix = RowMatrix.from(sc.parallelize(rows));
    final DenseMatrix expectedMatrix = new DenseMatrix(5, 4, new double[]{
      1.0, 0.0, 0.0, 0.0,
      0.0, 2.0, 1.0, 0.0,
      3.0, 1.0, 1.0, 0.0,
      0.0, 1.0, 2.0, 1.0,
      0.0, 0.0, 1.0, 5.0}, true);
    assertEquals(5L, rowMatrix.numRows());
    assertEquals(4L, rowMatrix.numCols());
    assertEquals(expectedMatrix.toBreeze(), rowMatrix.toBreeze());
  }
}
