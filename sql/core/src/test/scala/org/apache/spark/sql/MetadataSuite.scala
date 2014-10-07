package org.apache.spark.sql

import org.apache.spark.sql.test.TestSQLContext
import org.scalatest.FunSuite

case class Person(id: Int, name: String, age: Int)

case class Score(personId: Int, score: Double)

class MetadataSuite extends FunSuite {

  test("metadata") {
    val sqlContext = TestSQLContext
    import sqlContext._
    val person = sqlContext.sparkContext.makeRDD(Seq(
      Person(0, "mike", 10),
      Person(1, "jim", 20))).toSchemaRDD
    val score = sqlContext.sparkContext.makeRDD(Seq(
      Score(0, 4.0),
      Score(1, 5.0))).toSchemaRDD
    val personSchema: StructType = person.schema
    println("schema: " + personSchema)
    val ageField = personSchema("age").copy(metadata = Map("doc" -> "age (must be nonnegative)"))
    val newPersonSchema = personSchema.copy(Seq(personSchema("id"), personSchema("name"), ageField))
    val newPerson = sqlContext.applySchema(person, newPersonSchema)
    newPerson.registerTempTable("person")
    score.registerTempTable("score")
    val selectByExprAgeField = newPerson.select('age).schema("age")
    assert(selectByExprAgeField.metadata.contains("doc"))
    val selectByNameAttrAgeField = newPerson.select("age".attr).schema("age")
    assert(selectByNameAttrAgeField.metadata.contains("doc"))
    val selectAgeBySQL = sql("SELECT age FROM person").schema("age")
    println(selectAgeBySQL)
    assert(selectAgeBySQL.metadata.contains("doc"))
    val selectStarBySQL = sql("SELECT * FROM person").schema("age")
    println(selectStarBySQL)
    assert(selectStarBySQL.metadata.contains("doc"))
    val selectStarJoinBySQL = sql("SELECT * FROM person JOIN score ON id = personId").schema("age")
    println(selectStarJoinBySQL)
    assert(selectStarJoinBySQL.metadata.contains("doc"))
    val selectAgeJoinBySQL = sql("SELECT age, score FROM person JOIN score ON id = personId").schema("age")
    println(selectAgeJoinBySQL)
    assert(selectAgeJoinBySQL.metadata.contains("doc"))
  }
}
