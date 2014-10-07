package org.apache.spark.sql

import org.apache.spark.sql.test.TestSQLContext
import org.scalatest.FunSuite

case class Person(name: String, age: Int)

class MetadataSuite extends FunSuite {

  test("metadata") {
    val sqlContext = TestSQLContext
    import sqlContext._
    val members = sqlContext.sparkContext.makeRDD(Seq(
      Person("mike", 10),
      Person("jim", 20)))
    val person: SchemaRDD = sqlContext.createSchemaRDD(members)
    val schema: StructType = person.schema
    println("schema: " + schema)
    val ageField = schema("age").copy(metadata = Map("doc" -> "age (must be nonnegative)"))
    val newSchema = schema.copy(Seq(schema("name"), ageField))
    val newTable = sqlContext.applySchema(person, newSchema)
    newTable.registerTempTable("person")
    val selectByExprAgeField = newTable.select('age).schema("age")
    assert(selectByExprAgeField.metadata.contains("doc"))
    val selectByNameAttrAgeField = newTable.select("age".attr).schema("age")
    assert(selectByNameAttrAgeField.metadata.contains("doc"))
    val selectAgeBySQL = sql("SELECT age FROM person").schema("age")
    println(selectAgeBySQL)
    assert(selectAgeBySQL.metadata.contains("doc"))
    val selectStarBySQL = sql("SELECT * FROM person").schema("age")
    println(selectStarBySQL)
    assert(selectStarBySQL.metadata.contains("doc"))
  }
}
