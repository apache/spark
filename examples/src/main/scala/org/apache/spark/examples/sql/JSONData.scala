/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row;

// Importing the required data types from the package org.apache.spark.sql.types.
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType};

object JSONData {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("JSONData")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext.implicits._

    // Creating the schema with the desired column names and types, represented by a StructType.
    val schema =
        StructType(
            StructField("Id", StringType, true) ::
            StructField("Age", IntegerType, true) :: Nil)

    // Creating the RDD of Rows.
    val rowRDD =sc.parallelize((1 to 50).map(i => Row(s"id_$i", i)))

    // Applying the schema to the RDD.
    val peopleDF = sqlContext.createDataFrame(rowRDD, schema)

    // Converting the SchemaRDD into a MappedRDD of JSON documents, using 'toJSON' and saving it.
    peopleDF.toJSON.saveAsTextFile("ageGroups.json")

    // JSON dataset is pointed to by a path.
    // The path can be either a single text file or a directory storing text files.
    val path = "ageGroups.json"
    // Creating a SchemaRDD from the file(s) pointed to by path.
    val ageGroup = sqlContext.jsonFile("ageGroups.json")

    // The inferred schema can be visualized using the printSchema() method.
    ageGroup.printSchema()

    // Register the SchemaRDD as a table.
    ageGroup.registerTempTable("people")

    // Once tables have been registered, SQL statements can be run by using the sql methods 
    // provided by sqlContext.
    val kids = sqlContext.sql("SELECT Id FROM people WHERE Age >= 3 AND Age <= 12")

    println("Result of SELECT Id :")
    kids.collect().foreach(println)

    // Alternatively, we can create a SchemaRDD for a JSON dataset represented by
    // an RDD[String], with one JSON object per string.
    val employeesRDD = sc.parallelize(
        """{"Name":"Anna", "Profile":{"experieance":"5yrs","department":"HR"}}"""::
        """{"Name":"John", "Profile":{"experieance":"7yrs","department":"Marketing"}}""":: Nil)
    val employees = sqlContext.jsonRDD(employeesRDD)
    employees.registerTempTable("employees")
    println("Result of Table.show() :")
    employees.show()

    sc.stop()
  }
}


