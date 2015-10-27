package org.apache.spark.mllib.linalg.distributed;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.DenseMatrix;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JavaBlockMatrixSuite {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaBlockMatrixSuite");
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  @Test
  public void blockMatrixConstruction() {
    List<Tuple2<Tuple2<Integer, Integer>, DenseMatrix>> blocks = Arrays.asList(
      new Tuple2<Tuple2<Integer, Integer>, DenseMatrix>(
        new Tuple2<Integer, Integer>(0, 0), new DenseMatrix(2, 2, new double[]{1.0, 0.0, 0.0, 2.0})),
      new Tuple2<Tuple2<Integer, Integer>, DenseMatrix>(
        new Tuple2<Integer, Integer>(0, 1), new DenseMatrix(2, 2, new double[]{0.0, 1.0, 0.0, 0.0})),
      new Tuple2<Tuple2<Integer, Integer>, DenseMatrix>(
        new Tuple2<Integer, Integer>(1, 0), new DenseMatrix(2, 2, new double[]{3.0, 0.0, 1.0, 1.0})),
      new Tuple2<Tuple2<Integer, Integer>, DenseMatrix>(
        new Tuple2<Integer, Integer>(1, 1), new DenseMatrix(2, 2, new double[]{1.0, 2.0, 0.0, 1.0})),
      new Tuple2<Tuple2<Integer, Integer>, DenseMatrix>(
        new Tuple2<Integer, Integer>(2, 1), new DenseMatrix(1, 2, new double[]{1.0, 5.0})));
    BlockMatrix blockMatrix = BlockMatrix.from(sc.parallelize(blocks), 2, 2);
    final DenseMatrix expectedMatrix = new DenseMatrix(5, 4, new double[]{
      1.0, 0.0, 0.0, 0.0,
      0.0, 2.0, 1.0, 0.0,
      3.0, 1.0, 1.0, 0.0,
      0.0, 1.0, 2.0, 1.0,
      0.0, 0.0, 1.0, 5.0}, true);
    assertEquals(5L, blockMatrix.numRows());
    assertEquals(4L, blockMatrix.numCols());
    assertEquals(expectedMatrix, blockMatrix.toLocalMatrix());
  }
}
