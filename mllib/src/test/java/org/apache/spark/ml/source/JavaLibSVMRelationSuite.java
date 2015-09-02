package org.apache.spark.ml.source;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.util.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Test LibSVMRelation in Java.
 */
public class JavaLibSVMRelationSuite {
  private transient JavaSparkContext jsc;
  private transient SQLContext jsql;
  private transient DataFrame dataset;

  private File path;

  @Before
  public void setUp() throws IOException {
    jsc = new JavaSparkContext("local", "JavaLibSVMRelationSuite");
    jsql = new SQLContext(jsc);

    path = Utils.createTempDir(System.getProperty("java.io.tmpdir"),
      "datasource").getCanonicalFile();
    if (path.exists()) {
      path.delete();
    }

    String s = "1 1:1.0 3:2.0 5:3.0\n0\n0 2:4.0 4:5.0 6:6.0";
    Files.write(s, path, Charsets.US_ASCII);
  }

  @After
  public void tearDown() {
    jsc.stop();
    jsc = null;
  }

  @Test
  public void verifyLibSvmDF() {
    dataset = jsql.read().format("libsvm").load();
    Assert.assertEquals(dataset.columns()[0], "label");
    Assert.assertEquals(dataset.columns()[1], "features");
    Row r = dataset.first();
    Assert.assertTrue(r.getDouble(0) == 1.0);
    Assert.assertEquals(r.getAs(1), Vectors.dense(1.0, 0.0, 2.0, 0.0, 3.0, 0.0));
  }
}
