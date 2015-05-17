package org.apache.spark.ml.feature;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
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
      RowFactory.create(Lists.newArrayList("Hi I heard about Spark".split(" "))),
      RowFactory.create(Lists.newArrayList("I wish Java could use case classes".split(" "))),
      RowFactory.create(Lists.newArrayList("Logistic regression models are neat".split(" ")))
    ));
    StructType schema = new StructType(new StructField[]{
      new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
    });
    DataFrame documentDF = sqlContext.createDataFrame(jrdd, schema);

    Word2Vec word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0);
    Word2VecModel model = word2Vec.fit(documentDF);
    DataFrame result = model.transform(documentDF);

    for (Row r: result.select("result").collect()) {
      double[] polyFeatures = ((Vector)r.get(0)).toArray();
      Assert.assertEquals(polyFeatures.length, 3);
    }
  }
}
