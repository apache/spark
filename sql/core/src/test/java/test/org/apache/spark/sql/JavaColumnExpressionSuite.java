package test.org.apache.spark.sql;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.test.TestSparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.apache.spark.sql.types.DataTypes.*;

public class JavaColumnExpressionSuite {

    private transient TestSparkSession spark;

    @Before
    public void setUp() {
        spark = new TestSparkSession();
    }

    @After
    public void tearDown() {
        spark.stop();
        spark = null;
    }

    @Test
    public void isInCollectionWorksCorrectlyOnJava() {
        List<Row> rows = Arrays.asList(
                RowFactory.create(1, "x"),
                RowFactory.create(2, "y"),
                RowFactory.create(3, "z")
        );
        StructType schema = createStructType(Arrays.asList(
                createStructField("a", IntegerType, false),
                createStructField("b", StringType, false)
        ));
        Dataset<Row> df = spark.createDataFrame(rows, schema);
        // Test with different types of collections
        Assert.assertTrue(Arrays.equals(
                (Row[]) df.filter(df.col("a").isInCollection(Arrays.asList(1, 2))).collect(),
                (Row[]) df.filter((FilterFunction<Row>) r -> r.getInt(0) == 1 || r.getInt(0) == 2).collect()
        ));
        Assert.assertTrue(Arrays.equals(
                (Row[]) df.filter(df.col("a").isInCollection(new HashSet<>(Arrays.asList(1, 2)))).collect(),
                (Row[]) df.filter((FilterFunction<Row>) r -> r.getInt(0) == 1 || r.getInt(0) == 2).collect()
        ));
        Assert.assertTrue(Arrays.equals(
                (Row[]) df.filter(df.col("a").isInCollection(new ArrayList<>(Arrays.asList(3, 1)))).collect(),
                (Row[]) df.filter((FilterFunction<Row>) r -> r.getInt(0) == 3 || r.getInt(0) == 1).collect()
        ));
    }

    @Test
    public void isInCollectionThrowsExceptionWithCorrectMessageOnJava() {
        List<Row> rows = Arrays.asList(
                RowFactory.create(1, Arrays.asList(1)),
                RowFactory.create(2, Arrays.asList(2)),
                RowFactory.create(3, Arrays.asList(3))
        );
        StructType schema = createStructType(Arrays.asList(
                createStructField("a", IntegerType, false),
                createStructField("b", createArrayType(IntegerType, false), false)
        ));
        Dataset<Row> df = spark.createDataFrame(rows, schema);
        try {
            df.filter(df.col("a").isInCollection(Arrays.asList(new Column("b"))));
        } catch (Exception e) {
            Arrays.asList("cannot resolve", "due to data type mismatch: Arguments must be same type but were")
                    .forEach(s -> Assert.assertTrue(e.getMessage().toLowerCase(Locale.ROOT).contains(s.toLowerCase(Locale.ROOT))));
        }
    }
}
