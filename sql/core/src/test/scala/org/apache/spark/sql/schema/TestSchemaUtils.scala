package org.apache.spark.sql.schema

import org.apache.spark.sql.types._
import org.scalatest.{Matchers, FlatSpec}

class TestSchemaUtils extends FlatSpec with Matchers with Serializable{

"Test-1: getStructType" should "return testSchema2" in {
    val testSchema1 = StructType(Array(StructField("value", ArrayType(StructType(Array(StructField("seqId",IntegerType,true), StructField("value",NullType,true))),false),true)))
    val testSchema2 = StructType(Array(StructField("value", ArrayType(StructType(Array(StructField("seqId",IntegerType,true), StructField("value",StringType,true))),false),true)))
    SchemaUtils.getStructType(testSchema1) should be (testSchema2)
}

"Test-2: getStructType" should "return testSchema2" in {
    val testSchema1 = StructType(Array(StructField("additionalStrap",StructType(Array(StructField("seqId",IntegerType,true), StructField("isGlobal",BooleanType,true), StructField("label",NullType,true), StructField("name",StringType,true))),true)))
    val testSchema2 = StructType(Array(StructField("additionalStrap",StructType(Array(StructField("seqId",IntegerType,true), StructField("isGlobal",BooleanType,true), StructField("label",StringType,true), StructField("name",StringType,true))),true)))
    SchemaUtils.getStructType(testSchema1) should be (testSchema2)
}

}
