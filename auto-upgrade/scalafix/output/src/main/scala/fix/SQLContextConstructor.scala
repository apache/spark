import org.apache.spark._
import org.apache.spark.sql._

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalacheck.{Arbitrary, Gen}
import org.apache.spark.sql.SparkSession

object BadSessionBuilder {
  def getSQLContext(sc: SparkContext): SQLContext = {
    val ctx = SparkSession.builder.getOrCreate().sqlContext
    ctx
  }

  def getOrCreateSQL(sc: SparkContext): SQLContext = {
    val ctx = SparkSession.builder.getOrCreate().sqlContext
    val boop = SQLContext.clearActive() // We shouldn't rewrite this
    ctx
  }

  // This function is unrelated but early tests had arbLong rewrite for some reason.
  private def getGenerator(
    dataType: DataType, generators: Seq[_] = Seq()): Gen[Any] = {
    dataType match {
      case ByteType => Arbitrary.arbitrary[Byte]
      case ShortType => Arbitrary.arbitrary[Short]
      case IntegerType => Arbitrary.arbitrary[Int]
      case LongType => Arbitrary.arbitrary[Long]
      case FloatType => Arbitrary.arbitrary[Float]
      case DoubleType => Arbitrary.arbitrary[Double]
      case StringType => Arbitrary.arbitrary[String]
      case BinaryType => Arbitrary.arbitrary[Array[Byte]]
      case BooleanType => Arbitrary.arbitrary[Boolean]
      case TimestampType => Arbitrary.arbLong.arbitrary.map(new Timestamp(_))
      case DateType => Arbitrary.arbLong.arbitrary.map(new Date(_))
      case arr: ArrayType => {
        val elementGenerator = getGenerator(arr.elementType)
        Gen.listOf(elementGenerator)
      }
      case map: MapType => {
        val keyGenerator = getGenerator(map.keyType)
        val valueGenerator = getGenerator(map.valueType)
        val keyValueGenerator: Gen[(Any, Any)] = for {
          key <- keyGenerator
          value <- valueGenerator
        } yield (key, value)

        Gen.mapOf(keyValueGenerator)
      }
      case row: StructType => None
      case _ => throw new UnsupportedOperationException(
        s"Type: $dataType not supported")
    }
  }

}
