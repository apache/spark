package org.apache.spark.mllib.feature;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import com.google.common.collect.Lists;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;

public class JavaTfIdfSuite implements Serializable {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaTfIdfSuite");
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  @Test
  public void tfIdf() {
    // The tests are to check Java compatibility.
    HashingTF tf = new HashingTF();
    JavaRDD<ArrayList<String>> documents = sc.parallelize(Lists.newArrayList(
      Lists.newArrayList("this is a sentence".split(" ")),
      Lists.newArrayList("this is another sentence".split(" ")),
      Lists.newArrayList("this is still a sentence".split(" "))), 2);
    JavaRDD<Vector> termFreqs = tf.transform(documents);
    termFreqs.collect();
    IDF idf = new IDF();
    JavaRDD<Vector> tfIdfs = idf.fit(termFreqs).transform(termFreqs);
    List<Vector> localTfIdfs = tfIdfs.collect();
    int indexOfThis = tf.indexOf("this");
    for (Vector v: localTfIdfs) {
      Assert.assertEquals(0.0, v.apply(indexOfThis), 1e-15);
    }
  }
}
