package org.apache.spark.sql.columnar

import scala.util.Random

import org.apache.spark.sql.catalyst.expressions.GenericMutableRow

// TODO Enrich test data
object ColumnarTestData {
  object GenericMutableRow {
    def apply(values: Any*) = {
      val row = new GenericMutableRow(values.length)
      row.indices.foreach { i =>
        row(i) = values(i)
      }
      row
    }
  }

  def randomBytes(length: Int) = {
    val bytes = new Array[Byte](length)
    Random.nextBytes(bytes)
    bytes
  }

  val nonNullRandomRow = GenericMutableRow(
    Random.nextInt(),
    Random.nextLong(),
    Random.nextFloat(),
    Random.nextDouble(),
    Random.nextBoolean(),
    Random.nextInt(Byte.MaxValue).asInstanceOf[Byte],
    Random.nextInt(Short.MaxValue).asInstanceOf[Short],
    Random.nextString(Random.nextInt(64)),
    randomBytes(Random.nextInt(64)),
    Map(Random.nextInt() -> Random.nextString(4)))

  val nullRow = GenericMutableRow(Seq.fill(10)(null): _*)
}
