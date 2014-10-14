package org.apache.spark.sql.catalyst.util

import org.json4s.jackson.JsonMethods._
import org.scalatest.FunSuite

class MetadataSuite extends FunSuite {

  val baseMetadata = new MetadataBuilder()
      .putString("purpose", "ml")
      .build()

  val summary = new MetadataBuilder()
      .putInt("numFeatures", 10)
      .build()

  val age = new MetadataBuilder()
      .putString("name", "age")
      .putInt("index", 1)
      .putBoolean("categorical", false)
      .putDouble("average", 45.0)
      .build()

  val gender = new MetadataBuilder()
      .putString("name", "gender")
      .putInt("index", 5)
      .putBoolean("categorical", true)
      .putStringArray("categories", Seq("male", "female"))
      .build()

  val metadata = new MetadataBuilder()
      .withMetadata(baseMetadata)
      .putMetadata("summary", summary)
      .putIntArray("int[]", Seq(0, 1))
      .putDoubleArray("double[]", Seq(3.0, 4.0))
      .putBooleanArray("boolean[]", Seq(true, false))
      .putMetadataArray("features", Seq(age, gender))
      .build()

  test("metadata builder and getters") {
    assert(age.getInt("index") === 1)
    assert(age.getDouble("average") === 45.0)
    assert(age.getBoolean("categorical") === false)
    assert(age.getString("name") === "age")
    assert(metadata.getString("purpose") === "ml")
    assert(metadata.getMetadata("summary") === summary)
    assert(metadata.getIntArray("int[]").toSeq === Seq(0, 1))
    assert(metadata.getDoubleArray("double[]").toSeq === Seq(3.0, 4.0))
    assert(metadata.getBooleanArray("boolean[]").toSeq === Seq(true, false))
    assert(gender.getStringArray("categories").toSeq === Seq("male", "female"))
    assert(metadata.getMetadataArray("features").toSeq === Seq(age, gender))
  }

  test("metadata json conversion") {
    val json = metadata.toJson
    withClue("toJson must produce a valid JSON string") {
      parse(json)
    }
    assert(Metadata.fromJson(json) === metadata)
  }
}
