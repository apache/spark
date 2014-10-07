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
    val table: SchemaRDD = sqlContext.createSchemaRDD(members)
    val schema: StructType = table.schema
    println("schema: " + schema)
    val ageField = schema("age").copy(metadata = Map("desc" -> "age (must be nonnegative)"))
    val newSchema = schema.copy(Seq(schema("name"), ageField))
    val newTable = sqlContext.applySchema(table, newSchema)
    val selectByExprAgeField = newTable.select('age).schema("age")
    assert(selectByExprAgeField.metadata.nonEmpty)
    val selectByNameAttrAgeField = newTable.select("age".attr).schema("age")
    assert(selectByNameAttrAgeField.metadata.nonEmpty)
  }
}
