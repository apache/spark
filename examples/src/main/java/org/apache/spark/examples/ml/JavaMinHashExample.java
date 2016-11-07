import org.apache.spark.sql.SparkSession;

// $example on$
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.MinHash;
import org.apache.spark.ml.feature.MinHashModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaMinHashExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaMinHashExample")
      .getOrCreate();

    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(0, Vectors.sparse(6, new int[]{0, 1, 2}, new double[]{1.0, 1.0, 1.0})),
      RowFactory.create(1, Vectors.sparse(6, new int[]{2, 3, 4}, new double[]{1.0, 1.0, 1.0})),
      RowFactory.create(2, Vectors.sparse(6, new int[]{0, 2, 4}, new double[]{1.0, 1.0, 1.0}))
    );

    StructType schema = new StructType(new StructField[]{
      new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("keys", new VectorUDT(), false, Metadata.empty())
    });
    Dataset<Row> dataFrame = spark.createDataFrame(data, schema);

    MinHash mh = new MinHash()
      .setOutputDim(1)
      .setInputCol("keys")
      .setOutputCol("values");

    MinHashModel model = mh.fit(dataFrame);
    model.transform(dataFrame).show();
    // $example off$

    spark.stop();
  }
}
