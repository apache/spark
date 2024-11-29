#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import shutil
import tempfile
import uuid
import os

from pyspark.sql import Row
from pyspark.sql.datasource import InputPartition, DataSource
from pyspark.sql.types import IntegerType, StructField, StructType, LongType, StringType
from pyspark.errors import PySparkNotImplementedError
from pyspark.testing.sqlutils import ReusedSQLTestCase


class DataSourcesTestsMixin:
    def test_linesep_text(self):
        df = self.spark.read.text("python/test_support/sql/ages_newlines.csv", lineSep=",")
        expected = [
            Row(value="Joe"),
            Row(value="20"),
            Row(value='"Hi'),
            Row(value='\nI am Jeo"\nTom'),
            Row(value="30"),
            Row(value='"My name is Tom"\nHyukjin'),
            Row(value="25"),
            Row(value='"I am Hyukjin\n\nI love Spark!"\n'),
        ]
        self.assertEqual(df.collect(), expected)

        tpath = tempfile.mkdtemp()
        shutil.rmtree(tpath)
        try:
            df.write.text(tpath, lineSep="!")
            expected = [
                Row(value='Joe!20!"Hi!'),
                Row(value='I am Jeo"'),
                Row(value='Tom!30!"My name is Tom"'),
                Row(value='Hyukjin!25!"I am Hyukjin'),
                Row(value=""),
                Row(value='I love Spark!"'),
                Row(value="!"),
            ]
            readback = self.spark.read.text(tpath)
            self.assertEqual(readback.collect(), expected)
        finally:
            shutil.rmtree(tpath)

    def test_multiline_json(self):
        people1 = self.spark.read.json("python/test_support/sql/people.json")
        people_array = self.spark.read.json(
            "python/test_support/sql/people_array.json", multiLine=True
        )
        self.assertEqual(people1.collect(), people_array.collect())

    def test_encoding_json(self):
        people_array = self.spark.read.json(
            "python/test_support/sql/people_array_utf16le.json", multiLine=True, encoding="UTF-16LE"
        )
        expected = [Row(age=30, name="Andy"), Row(age=19, name="Justin")]
        self.assertEqual(people_array.collect(), expected)

    def test_linesep_json(self):
        df = self.spark.read.json("python/test_support/sql/people.json", lineSep=",")
        expected = [
            Row(_corrupt_record=None, name="Michael"),
            Row(_corrupt_record=' "age":30}\n{"name":"Justin"', name=None),
            Row(_corrupt_record=' "age":19}\n', name=None),
        ]
        self.assertEqual(df.collect(), expected)

        tpath = tempfile.mkdtemp()
        shutil.rmtree(tpath)
        try:
            df = self.spark.read.json("python/test_support/sql/people.json")
            df.write.json(tpath, lineSep="!!")
            readback = self.spark.read.json(tpath, lineSep="!!")
            self.assertEqual(readback.collect(), df.collect())
        finally:
            shutil.rmtree(tpath)

    def test_multiline_csv(self):
        ages_newlines = self.spark.read.csv(
            "python/test_support/sql/ages_newlines.csv", multiLine=True
        )
        expected = [
            Row(_c0="Joe", _c1="20", _c2="Hi,\nI am Jeo"),
            Row(_c0="Tom", _c1="30", _c2="My name is Tom"),
            Row(_c0="Hyukjin", _c1="25", _c2="I am Hyukjin\n\nI love Spark!"),
        ]
        self.assertEqual(ages_newlines.collect(), expected)

    def test_ignorewhitespace_csv(self):
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        self.spark.createDataFrame([[" a", "b  ", " c "]]).write.csv(
            tmpPath, ignoreLeadingWhiteSpace=False, ignoreTrailingWhiteSpace=False
        )

        expected = [Row(value=" a,b  , c ")]
        readback = self.spark.read.text(tmpPath)
        self.assertEqual(readback.collect(), expected)
        shutil.rmtree(tmpPath)

    def test_xml(self):
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        xsdPath = tempfile.mkdtemp()
        xsdString = """<?xml version="1.0" encoding="UTF-8" ?>
          <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
            <xs:element name="person">
              <xs:complexType>
                <xs:sequence>
                  <xs:element name="name" type="xs:string" />
                  <xs:element name="age" type="xs:long" />
                </xs:sequence>
              </xs:complexType>
            </xs:element>
          </xs:schema>"""
        try:
            with open(os.path.join(xsdPath, "people.xsd"), "w") as f:
                _ = f.write(xsdString)
            df = self.spark.createDataFrame([("Hyukjin", 100), ("Aria", 101), ("Arin", 102)]).toDF(
                "name", "age"
            )
            df.write.xml(tmpPath, rootTag="people", rowTag="person")
            people = self.spark.read.xml(
                tmpPath, rowTag="person", rowValidationXSDPath=os.path.join(xsdPath, "people.xsd")
            )
            expected = [
                Row(age=100, name="Hyukjin"),
                Row(age=101, name="Aria"),
                Row(age=102, name="Arin"),
            ]
            self.assertEqual(people.sort("age").collect(), expected)
            self.assertEqual(
                people.schema,
                StructType(
                    [StructField("age", LongType(), True), StructField("name", StringType(), True)]
                ),
            )
        finally:
            shutil.rmtree(tmpPath)
            shutil.rmtree(xsdPath)

    def test_xml_sampling_ratio(self):
        rdd = self.spark.sparkContext.range(0, 100, 1, 1).map(
            lambda x: "<p><a>0.1</a></p>" if x == 1 else "<p><a>%s</a></p>" % str(x)
        )
        schema = self.spark.read.option("samplingRatio", 0.5).xml(rdd).schema
        self.assertEqual(schema, StructType([StructField("a", LongType(), True)]))

    def test_read_multiple_orc_file(self):
        df = self.spark.read.orc(
            [
                "python/test_support/sql/orc_partitioned/b=0/c=0",
                "python/test_support/sql/orc_partitioned/b=1/c=1",
            ]
        )
        self.assertEqual(2, df.count())

    def test_read_text_file_list(self):
        df = self.spark.read.text(
            ["python/test_support/sql/text-test.txt", "python/test_support/sql/text-test.txt"]
        )
        count = df.count()
        self.assertEqual(count, 4)

    def test_json_sampling_ratio(self):
        rdd = self.spark.sparkContext.range(0, 100, 1, 1).map(
            lambda x: '{"a":0.1}' if x == 1 else '{"a":%s}' % str(x)
        )
        schema = (
            self.spark.read.option("inferSchema", True)
            .option("samplingRatio", 0.5)
            .json(rdd)
            .schema
        )
        self.assertEqual(schema, StructType([StructField("a", LongType(), True)]))

    def test_csv_sampling_ratio(self):
        rdd = self.spark.sparkContext.range(0, 100, 1, 1).map(lambda x: "0.1" if x == 1 else str(x))
        schema = self.spark.read.option("inferSchema", True).csv(rdd, samplingRatio=0.5).schema
        self.assertEqual(schema, StructType([StructField("_c0", IntegerType(), True)]))

    def test_checking_csv_header(self):
        path = tempfile.mkdtemp()
        shutil.rmtree(path)
        try:
            self.spark.createDataFrame([[1, 1000], [2000, 2]]).toDF("f1", "f2").write.option(
                "header", "true"
            ).csv(path)
            schema = StructType(
                [
                    StructField("f2", IntegerType(), nullable=True),
                    StructField("f1", IntegerType(), nullable=True),
                ]
            )
            df = (
                self.spark.read.option("header", "true")
                .option("quote", None)
                .schema(schema)
                .csv(path, enforceSchema=False)
            )
            self.assertRaisesRegex(
                Exception, "CSV header does not conform to the schema", lambda: df.collect()
            )
        finally:
            shutil.rmtree(path)

    def test_ignore_column_of_all_nulls(self):
        path = tempfile.mkdtemp()
        shutil.rmtree(path)
        try:
            df = self.spark.createDataFrame(
                [
                    ["""{"a":null, "b":1, "c":3.0}"""],
                    ["""{"a":null, "b":null, "c":"string"}"""],
                    ["""{"a":null, "b":null, "c":null}"""],
                ]
            )
            df.write.text(path)
            schema = StructType(
                [
                    StructField("b", LongType(), nullable=True),
                    StructField("c", StringType(), nullable=True),
                ]
            )
            readback = self.spark.read.json(path, dropFieldIfAllNull=True)
            self.assertEqual(readback.schema, schema)
        finally:
            shutil.rmtree(path)

    def test_jdbc(self):
        db = f"memory:{uuid.uuid4()}"
        url = f"jdbc:derby:{db}"
        dbtable = "test_table"

        try:
            df = self.spark.range(10)
            df.write.jdbc(url=f"{url};create=true", table=dbtable)

            readback = self.spark.read.jdbc(url=url, table=dbtable)
            self.assertEqual(sorted(df.collect()), sorted(readback.collect()))

            additional_arguments = dict(column="id", lowerBound=3, upperBound=8, numPartitions=10)
            readback = self.spark.read.jdbc(url=url, table=dbtable, **additional_arguments)
            self.assertEqual(sorted(df.collect()), sorted(readback.collect()))

            additional_arguments = dict(predicates=['"id" < 5'])
            readback = self.spark.read.jdbc(url=url, table=dbtable, **additional_arguments)
            self.assertEqual(sorted(df.filter("id < 5").collect()), sorted(readback.collect()))
        finally:
            # Clean up.
            with self.assertRaisesRegex(Exception, f"Database '{db}' dropped."):
                self.spark.read.jdbc(url=f"{url};drop=true", table=dbtable).collect()

    def test_jdbc_format(self):
        db = f"memory:{uuid.uuid4()}"
        url = f"jdbc:derby:{db}"
        dbtable = "test_table"

        try:
            df = self.spark.range(10)
            df.write.format("jdbc").options(url=f"{url};create=true", dbtable=dbtable).save()
            readback = self.spark.read.format("jdbc").options(url=url, dbtable=dbtable).load()
            self.assertEqual(sorted(df.collect()), sorted(readback.collect()))
        finally:
            # Clean up.
            with self.assertRaisesRegex(Exception, f"Database '{db}' dropped."):
                self.spark.read.format("jdbc").options(
                    url=f"{url};drop=true", dbtable=dbtable
                ).load().collect()

    def test_custom_data_source(self):
        class MyCustomDataSource(DataSource):
            pass

        custom_data_source = MyCustomDataSource(options={"path": "/path/to/custom/data"})

        with self.assertRaises(PySparkNotImplementedError) as pe:
            custom_data_source.schema()

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "schema"},
        )

        with self.assertRaises(PySparkNotImplementedError) as pe:
            custom_data_source.reader(schema=None)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "reader"},
        )

        with self.assertRaises(PySparkNotImplementedError) as pe:
            custom_data_source.writer(schema=None, overwrite=False)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "writer"},
        )

    def test_input_partition(self):
        partition = InputPartition(1)
        expected_repr = "InputPartition(value=1)"
        actual_repr = repr(partition)
        self.assertEqual(expected_repr, actual_repr)

        class RangeInputPartition(InputPartition):
            def __init__(self, start, end):
                super().__init__((start, end))

        partition = RangeInputPartition(1, 3)
        expected_repr = "RangeInputPartition(value=(1, 3))"
        actual_repr = repr(partition)
        self.assertEqual(expected_repr, actual_repr)


class DataSourcesTests(DataSourcesTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_datasources import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
