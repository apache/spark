package org.apache.spark.mllib.classification;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class JavaNaiveBayesSuite implements Serializable {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaNaiveBayesSuite");
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
    System.clearProperty("spark.driver.port");
  }

  private static final List<LabeledPoint> POINTS = Arrays.asList(
    new LabeledPoint(0, new double[] {1.0, 0.0, 0.0}),
    new LabeledPoint(0, new double[] {2.0, 0.0, 0.0}),
    new LabeledPoint(1, new double[] {0.0, 1.0, 0.0}),
    new LabeledPoint(1, new double[] {0.0, 2.0, 0.0}),
    new LabeledPoint(2, new double[] {0.0, 0.0, 1.0}),
    new LabeledPoint(2, new double[] {0.0, 0.0, 2.0})
  );

  private int validatePrediction(List<LabeledPoint> points, NaiveBayesModel model) {
    int correct = 0;
    for (LabeledPoint p: points) {
      if (model.predict(p.features()) == p.label()) {
        correct += 1;
      }
    }
    return correct;
  }

  @Test
  public void runUsingConstructor() {
    JavaRDD<LabeledPoint> testRDD = sc.parallelize(POINTS, 2).cache();

    NaiveBayes nb = new NaiveBayes().setLambda(1.0);
    NaiveBayesModel model = nb.run(testRDD.rdd());

    int numAccurate = validatePrediction(POINTS, model);
    Assert.assertEquals(POINTS.size(), numAccurate);
  }

  @Test
  public void runUsingStaticMethods() {
    JavaRDD<LabeledPoint> testRDD = sc.parallelize(POINTS, 2).cache();

    NaiveBayesModel model1 = NaiveBayes.train(testRDD.rdd());
    int numAccurate1 = validatePrediction(POINTS, model1);
    Assert.assertEquals(POINTS.size(), numAccurate1);

    NaiveBayesModel model2 = NaiveBayes.train(testRDD.rdd(), 0.5);
    int numAccurate2 = validatePrediction(POINTS, model2);
    Assert.assertEquals(POINTS.size(), numAccurate2);
  }
}
