package org.apache.spark.ml.feature;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;

public class JavaWord2VecSuite {
  private transient JavaSparkContext jsc;
  private transient SQLContext sqlContext;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaWord2VecSuite");
    sqlContext = new SQLContext(jsc);
  }

  @After
  public void tearDown() {
    jsc.stop();
    jsc = null;
  }

  @Test
  public void testJavaWord2Vec() {
    JavaRDD<Row> jrdd = jsc.parallelize(Lists.newArrayList(
      RowFactory.create(Lists.newArrayList("Hi I heard about Spark".split(" ")),
        Vectors.dense(0.017877750098705292, -0.018388677015900613, -0.01183266043663025)),
      RowFactory.create(Lists.newArrayList("I wish Java could use case classes".split(" ")),
        Vectors.dense(0.0038498884865215844, -0.07299017374004636, 0.010990704176947474)),
      RowFactory.create(Lists.newArrayList("Logistic regression models are neat".split(" ")),
        Vectors.dense(0.017819208838045598, -0.006920230574905872, 0.022744188457727434))
    ));
    StructType schema = new StructType(new StructField[]{
      new StructField("text", new ArrayType(StringType$.MODULE$, true), false, Metadata.empty()),
      new StructField("expected", new VectorUDT(), false, Metadata.empty())
    });
    DataFrame documentDF = sqlContext.createDataFrame(jrdd, schema);

    Word2Vec word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0);
    Word2VecModel model = word2Vec.fit(documentDF);
    DataFrame result = model.transform(documentDF);

    for (Row r: result.select("result", "expected").collect()) {
      double[] polyFeatures = ((Vector)r.get(0)).toArray();
      double[] expected = ((Vector)r.get(1)).toArray();
      Assert.assertArrayEquals(polyFeatures, expected, 1e-1);
    }
  }
}
