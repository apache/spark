package org.apache.spark.sql.execution

import java.sql.{Timestamp, Date}

import org.apache.spark.serializer.Serializer
import org.apache.spark.{SparkConf, ShuffleDependency, SparkContext}
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.scalatest.{FunSuite, BeforeAndAfterAll}

import org.apache.spark.sql.{MyDenseVectorUDT, SQLContext, QueryTest}

class SparkSqlSerializer2DataTypeSuite extends FunSuite {
  // Make sure that we will not use serializer2 for unsupported data types.
  def checkSupported(dataType: DataType, isSupported: Boolean): Unit = {
    val testName =
      s"${if (dataType == null) null else dataType.toString} is " +
        s"${if (isSupported) "supported" else "unsupported"}"

    test(testName) {
      assert(SparkSqlSerializer2.support(Array(dataType)) === isSupported)
    }
  }

  checkSupported(null, isSupported = true)
  checkSupported(NullType, isSupported = true)
  checkSupported(BooleanType, isSupported = true)
  checkSupported(ByteType, isSupported = true)
  checkSupported(ShortType, isSupported = true)
  checkSupported(IntegerType, isSupported = true)
  checkSupported(LongType, isSupported = true)
  checkSupported(FloatType, isSupported = true)
  checkSupported(DoubleType, isSupported = true)
  checkSupported(DateType, isSupported = true)
  checkSupported(TimestampType, isSupported = true)
  checkSupported(StringType, isSupported = true)
  checkSupported(BinaryType, isSupported = true)

  // Because at the runtime we accepts three kinds of Decimals
  // (Java BigDecimal, Scala BigDecimal, and Spark SQL's Decimal), we do support DecimalType
  // right now. We will support it once we fixed the internal type.
  checkSupported(DecimalType(10, 5), isSupported = false)
  checkSupported(DecimalType.Unlimited, isSupported = false)
  // For now, ArrayType, MapType, and StructType are not supported.
  checkSupported(ArrayType(DoubleType, true), isSupported = false)
  checkSupported(ArrayType(StringType, false), isSupported = false)
  checkSupported(MapType(IntegerType, StringType, true), isSupported = false)
  checkSupported(MapType(IntegerType, ArrayType(DoubleType), false), isSupported = false)
  checkSupported(StructType(StructField("a", IntegerType, true) :: Nil), isSupported = false)
  // UDTs are not supported right now.
  checkSupported(new MyDenseVectorUDT, isSupported = false)
}

abstract class SparkSqlSerializer2Suite extends QueryTest with BeforeAndAfterAll {

  @transient var sparkContext: SparkContext = _
  @transient var sqlContext: SQLContext = _
  var allColumns: String = _
  val serializerClass: Class[Serializer] =
    classOf[SparkSqlSerializer2].asInstanceOf[Class[Serializer]]

  override def beforeAll(): Unit = {
    sqlContext.sql("set spark.sql.shuffle.partitions=5")
    sqlContext.sql("set spark.sql.useSerializer2=true")

    val supportedTypes =
      Seq(StringType, BinaryType, NullType, BooleanType,
        ByteType, ShortType, IntegerType, LongType,
        FloatType, DoubleType, DateType, TimestampType)

    val fields = supportedTypes.zipWithIndex.map { case (dataType, index) =>
      StructField(s"col$index", dataType, true)
    }
    allColumns = fields.map(_.name).mkString(",")
    val schema = StructType(fields)

    // Create a RDD with all data types supported by SparkSqlSerializer2.
    val rdd =
      sparkContext.parallelize((1 to 1000), 10).map { i =>
        Row(
          s"str${i}: test serializer2.",
          s"binary${i}: test serializer2.".getBytes("UTF-8"),
          null,
          i % 2 == 0,
          i.toByte,
          i.toShort,
          i,
          i.toLong,
          (i + 0.25).toFloat,
          (i + 0.75),
          new Date(i),
          new Timestamp(i))
      }

    sqlContext.createDataFrame(rdd, schema).registerTempTable("shuffle")

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    sqlContext.dropTempTable("shuffle")
    sparkContext.stop()
    super.afterAll()
  }

  def checkSerializer[T <: Serializer](
      executedPlan: SparkPlan,
      expectedSerializerClass: Class[T]): Unit = {
    executedPlan.foreach {
      case exchange: Exchange =>
        val shuffledRDD = exchange.execute().firstParent.asInstanceOf[ShuffledRDD[_, _, _]]
        val dependency = shuffledRDD.getDependencies.head.asInstanceOf[ShuffleDependency[_, _, _]]
        val serializerNotSetMessage =
          s"Expected $expectedSerializerClass as the serializer of Exchange. " +
          s"However, the serializer was not set."
        val serializer = dependency.serializer.getOrElse(fail(serializerNotSetMessage))
        assert(serializer.getClass === expectedSerializerClass)
      case _ => // Ignore other nodes.
    }
  }

  test("key schema and value schema are not nulls") {
    val df = sqlContext.sql(s"SELECT DISTINCT ${allColumns} FROM shuffle")
    checkSerializer(df.queryExecution.executedPlan, serializerClass)
    checkAnswer(
      df,
      sqlContext.table("shuffle").collect())
  }

  test("value schema is null") {
    val df = sqlContext.sql(s"SELECT col0 FROM shuffle ORDER BY col0")
    checkSerializer(df.queryExecution.executedPlan, serializerClass)
    assert(
      df.map(r => r.getString(0)).collect().toSeq ===
      sqlContext.table("shuffle").select("col0").map(r => r.getString(0)).collect().sorted.toSeq)
  }

  test("key schema is null") {
    val aggregations = allColumns.split(",").map(c => s"COUNT($c)").mkString(",")
    val df = sqlContext.sql(s"SELECT $aggregations FROM shuffle")
    checkSerializer(df.queryExecution.executedPlan, serializerClass)
    checkAnswer(
      df,
      Row(1000, 1000, 0, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000))
  }
}

/** Tests SparkSqlSerializer2 with hash based shuffle. */
class SparkSqlSerializer2HashShuffleSuite extends SparkSqlSerializer2Suite {
  override def beforeAll(): Unit = {
    val sparkConf =
      new SparkConf()
        .set("spark.sql.testkey", "true")
        .set("spark.shuffle.manager", "hash")

    sparkContext = new SparkContext("local[2]", "Serializer2SQLContext", sparkConf)
    sqlContext = new SQLContext(sparkContext)
    super.beforeAll()
  }
}

/** Tests SparkSqlSerializer2 with sort based shuffle without sort merge. */
class SparkSqlSerializer2SortShuffleSuite extends SparkSqlSerializer2Suite {
  override def beforeAll(): Unit = {
    // Since spark.sql.shuffle.partition is 5, we will not do sort merge when
    // spark.shuffle.sort.bypassMergeThreshold is also 5.
    val sparkConf =
      new SparkConf()
        .set("spark.sql.testkey", "true")
        .set("spark.shuffle.manager", "sort")
        .set("spark.shuffle.sort.bypassMergeThreshold", "5")

    sparkContext = new SparkContext("local[2]", "Serializer2SQLContext", sparkConf)
    sqlContext = new SQLContext(sparkContext)
    super.beforeAll()
  }
}

/** For now, we will use SparkSqlSerializer for sort based shuffle with sort merge. */
class SparkSqlSerializer2SortMergeShuffleSuite extends SparkSqlSerializer2Suite {

  // We are expecting SparkSqlSerializer.
  override val serializerClass: Class[Serializer] =
    classOf[SparkSqlSerializer].asInstanceOf[Class[Serializer]]

  override def beforeAll(): Unit = {
    val sparkConf =
      new SparkConf()
        .set("spark.sql.testkey", "true")
        .set("spark.shuffle.manager", "sort")
        .set("spark.shuffle.sort.bypassMergeThreshold", "0") // Always do sort merge.

    sparkContext = new SparkContext("local[2]", "Serializer2SQLContext", sparkConf)
    sqlContext = new SQLContext(sparkContext)
    super.beforeAll()
  }
}
