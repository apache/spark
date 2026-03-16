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

import os
import shutil
import tempfile
import time

from pyspark.errors import PySparkTypeError, PySparkValueError
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    IntegerType,
    ArrayType,
    MapType,
    Row,
)
from pyspark.testing.objects import (
    PythonOnlyUDT,
    ExamplePoint,
    PythonOnlyPoint,
)

from pyspark.testing.connectutils import should_test_connect
from pyspark.sql.tests.connect.test_connect_basic import SparkConnectSQLTestCase

if should_test_connect:
    from pyspark.sql.connect.readwriter import DataFrameWriterV2


class SparkConnectReadWriterTests(SparkConnectSQLTestCase):
    def test_simple_read(self):
        df = self.connect.read.table(self.tbl_name)
        data = df.limit(10).toPandas()
        # Check that the limit is applied
        self.assertEqual(len(data.index), 10)

    def test_json(self):
        with tempfile.TemporaryDirectory(prefix="test_json") as d:
            # Write a DataFrame into a JSON file
            self.spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}]).write.mode(
                "overwrite"
            ).format("json").save(d)
            # Read the JSON file as a DataFrame.
            self.assert_eq(self.connect.read.json(d).toPandas(), self.spark.read.json(d).toPandas())

            for schema in [
                "age INT, name STRING",
                StructType(
                    [
                        StructField("age", IntegerType()),
                        StructField("name", StringType()),
                    ]
                ),
            ]:
                self.assert_eq(
                    self.connect.read.json(path=d, schema=schema).toPandas(),
                    self.spark.read.json(path=d, schema=schema).toPandas(),
                )

            self.assert_eq(
                self.connect.read.json(path=d, primitivesAsString=True).toPandas(),
                self.spark.read.json(path=d, primitivesAsString=True).toPandas(),
            )

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
            xsdFile = os.path.join(xsdPath, "people.xsd")
            with open(xsdFile, "w") as f:
                _ = f.write(xsdString)
            df = self.spark.createDataFrame([("Hyukjin", 100), ("Aria", 101), ("Arin", 102)]).toDF(
                "name", "age"
            )
            df.write.xml(tmpPath, rootTag="people", rowTag="person")
            people = self.spark.read.xml(tmpPath, rowTag="person", rowValidationXSDPath=xsdFile)
            peopleConnect = self.connect.read.xml(
                tmpPath, rowTag="person", rowValidationXSDPath=xsdFile
            )
            self.assert_eq(people.toPandas(), peopleConnect.toPandas())
            expected = [
                Row(age=100, name="Hyukjin"),
                Row(age=101, name="Aria"),
                Row(age=102, name="Arin"),
            ]
            expectedSchema = StructType(
                [StructField("age", LongType(), True), StructField("name", StringType(), True)]
            )

            self.assertEqual(people.sort("age").collect(), expected)
            self.assertEqual(people.schema, expectedSchema)

            for schema in [
                "age INT, name STRING",
                expectedSchema,
            ]:
                people = self.spark.read.xml(
                    tmpPath, rowTag="person", rowValidationXSDPath=xsdFile, schema=schema
                )
                peopleConnect = self.connect.read.xml(
                    tmpPath, rowTag="person", rowValidationXSDPath=xsdFile, schema=schema
                )
                self.assert_eq(people.toPandas(), peopleConnect.toPandas())

        finally:
            shutil.rmtree(tmpPath)
            shutil.rmtree(xsdPath)

    def test_parquet(self):
        # SPARK-41445: Implement DataFrameReader.parquet
        with tempfile.TemporaryDirectory(prefix="test_parquet") as d:
            # Write a DataFrame into a JSON file
            self.spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}]).write.mode(
                "overwrite"
            ).format("parquet").save(d)
            # Read the Parquet file as a DataFrame.
            self.assert_eq(
                self.connect.read.parquet(d).toPandas(), self.spark.read.parquet(d).toPandas()
            )

    def test_parquet_compression_option(self):
        # SPARK-50537: Fix compression option being overwritten in df.write.parquet
        with tempfile.TemporaryDirectory(prefix="test_parquet") as d:
            self.connect.range(10).write.mode("overwrite").option("compression", "gzip").parquet(d)
            self.assertTrue(any(file.endswith(".gz.parquet") for file in os.listdir(d)))
            # Read the Parquet file as a DataFrame.
            self.assert_eq(
                self.connect.read.parquet(d).toPandas(), self.spark.read.parquet(d).toPandas()
            )

    def test_text(self):
        # SPARK-41849: Implement DataFrameReader.text
        with tempfile.TemporaryDirectory(prefix="test_text") as d:
            # Write a DataFrame into a text file
            self.spark.createDataFrame(
                [{"name": "Sandeep Singh"}, {"name": "Hyukjin Kwon"}]
            ).write.mode("overwrite").format("text").save(d)
            # Read the text file as a DataFrame.
            self.assert_eq(self.connect.read.text(d).toPandas(), self.spark.read.text(d).toPandas())

    def test_csv(self):
        # SPARK-42011: Implement DataFrameReader.csv
        with tempfile.TemporaryDirectory(prefix="test_csv") as d:
            # Write a DataFrame into a text file
            self.spark.createDataFrame(
                [{"name": "Sandeep Singh"}, {"name": "Hyukjin Kwon"}]
            ).write.mode("overwrite").format("csv").save(d)
            # Read the text file as a DataFrame.
            self.assert_eq(self.connect.read.csv(d).toPandas(), self.spark.read.csv(d).toPandas())

    def test_multi_paths(self):
        # SPARK-42041: DataFrameReader should support list of paths

        with tempfile.TemporaryDirectory(prefix="test_multi_paths1") as d:
            text_files = []
            for i in range(0, 3):
                text_file = f"{d}/text-{i}.text"
                shutil.copyfile("python/test_support/sql/text-test.txt", text_file)
                text_files.append(text_file)

            self.assertEqual(
                self.connect.read.text(text_files).collect(),
                self.spark.read.text(text_files).collect(),
            )

        with tempfile.TemporaryDirectory(prefix="test_multi_paths2") as d:
            json_files = []
            for i in range(0, 5):
                json_file = f"{d}/json-{i}.json"
                shutil.copyfile("python/test_support/sql/people.json", json_file)
                json_files.append(json_file)

            self.assertEqual(
                self.connect.read.json(json_files).collect(),
                self.spark.read.json(json_files).collect(),
            )

    def test_orc(self):
        # SPARK-42012: Implement DataFrameReader.orc
        with tempfile.TemporaryDirectory(prefix="test_orc") as d:
            # Write a DataFrame into a text file
            self.spark.createDataFrame(
                [{"name": "Sandeep Singh"}, {"name": "Hyukjin Kwon"}]
            ).write.mode("overwrite").format("orc").save(d)
            # Read the text file as a DataFrame.
            self.assert_eq(self.connect.read.orc(d).toPandas(), self.spark.read.orc(d).toPandas())

    def test_simple_datasource_read(self) -> None:
        writeDf = self.df_text
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        writeDf.write.text(tmpPath)

        for schema in [
            "id STRING",
            StructType([StructField("id", StringType())]),
        ]:
            readDf = self.connect.read.format("text").schema(schema).load(path=tmpPath)
            expectResult = writeDf.collect()
            pandasResult = readDf.toPandas()
            if pandasResult is None:
                self.assertTrue(False, "Empty pandas dataframe")
            else:
                actualResult = pandasResult.values.tolist()
                self.assertEqual(len(expectResult), len(actualResult))

    def test_simple_read_without_schema(self) -> None:
        """SPARK-41300: Schema not set when reading CSV."""
        writeDf = self.df_text
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        writeDf.write.csv(tmpPath, header=True)

        readDf = self.connect.read.format("csv").option("header", True).load(path=tmpPath)
        expectResult = set(writeDf.collect())
        pandasResult = set(readDf.collect())
        self.assertEqual(expectResult, pandasResult)

    def test_write_operations(self):
        with tempfile.TemporaryDirectory(prefix="test_write_operations") as d:
            df = self.connect.range(50)
            df.write.mode("overwrite").format("csv").save(d)

            ndf = self.connect.read.schema("id int").load(d, format="csv")
            self.assertEqual(50, len(ndf.collect()))
            cd = ndf.collect()
            self.assertEqual(set(df.collect()), set(cd))

        with tempfile.TemporaryDirectory(prefix="test_write_operations") as d:
            df = self.connect.range(50)
            df.write.mode("overwrite").csv(d, lineSep="|")

            ndf = self.connect.read.schema("id int").load(d, format="csv", lineSep="|")
            self.assertEqual(set(df.collect()), set(ndf.collect()))

        df = self.connect.range(50)
        df.write.format("parquet").saveAsTable("parquet_test")

        ndf = self.connect.read.table("parquet_test")
        self.assertEqual(set(df.collect()), set(ndf.collect()))

    def test_writeTo_operations(self):
        # SPARK-42002: Implement DataFrameWriterV2
        import datetime
        from pyspark.sql.connect.functions import col, years, months, days, hours, bucket

        df = self.connect.createDataFrame(
            [(1, datetime.datetime(2000, 1, 1), "foo")], ("id", "ts", "value")
        )
        writer = df.writeTo("table1")
        self.assertIsInstance(writer.option("property", "value"), DataFrameWriterV2)
        self.assertIsInstance(writer.options(property="value"), DataFrameWriterV2)
        self.assertIsInstance(writer.using("source"), DataFrameWriterV2)
        self.assertIsInstance(writer.partitionedBy(col("id")), DataFrameWriterV2)
        self.assertIsInstance(writer.tableProperty("foo", "bar"), DataFrameWriterV2)
        self.assertIsInstance(writer.partitionedBy(years("ts")), DataFrameWriterV2)
        self.assertIsInstance(writer.partitionedBy(months("ts")), DataFrameWriterV2)
        self.assertIsInstance(writer.partitionedBy(days("ts")), DataFrameWriterV2)
        self.assertIsInstance(writer.partitionedBy(hours("ts")), DataFrameWriterV2)
        self.assertIsInstance(writer.partitionedBy(bucket(11, "id")), DataFrameWriterV2)
        self.assertIsInstance(writer.partitionedBy(bucket(3, "id"), hours("ts")), DataFrameWriterV2)

    def test_simple_udt_from_read(self):
        from pyspark.ml.linalg import Matrices, Vectors

        with tempfile.TemporaryDirectory(prefix="test_simple_udt_from_read") as d:
            path1 = f"{d}/df1.parquet"
            self.spark.createDataFrame(
                [(i % 3, PythonOnlyPoint(float(i), float(i))) for i in range(10)],
                schema=StructType().add("key", LongType()).add("val", PythonOnlyUDT()),
            ).write.parquet(path1)

            path2 = f"{d}/df2.parquet"
            self.spark.createDataFrame(
                [(i % 3, [PythonOnlyPoint(float(i), float(i))]) for i in range(10)],
                schema=StructType().add("key", LongType()).add("val", ArrayType(PythonOnlyUDT())),
            ).write.parquet(path2)

            path3 = f"{d}/df3.parquet"
            self.spark.createDataFrame(
                [(i % 3, {i % 3: PythonOnlyPoint(float(i + 1), float(i + 1))}) for i in range(10)],
                schema=StructType()
                .add("key", LongType())
                .add("val", MapType(LongType(), PythonOnlyUDT())),
            ).write.parquet(path3)

            path4 = f"{d}/df4.parquet"
            self.spark.createDataFrame(
                [(i % 3, PythonOnlyPoint(float(i), float(i))) for i in range(10)],
                schema=StructType().add("key", LongType()).add("val", PythonOnlyUDT()),
            ).write.parquet(path4)

            path5 = f"{d}/df5.parquet"
            self.spark.createDataFrame(
                [Row(label=1.0, point=ExamplePoint(1.0, 2.0))]
            ).write.parquet(path5)

            path6 = f"{d}/df6.parquet"
            self.spark.createDataFrame(
                [(Vectors.dense(1.0, 2.0, 3.0),), (Vectors.sparse(3, {1: 1.0, 2: 5.5}),)],
                ["vec"],
            ).write.parquet(path6)

            path7 = f"{d}/df7.parquet"
            self.spark.createDataFrame(
                [
                    (Matrices.dense(3, 2, [0, 1, 4, 5, 9, 10]),),
                    (Matrices.sparse(1, 1, [0, 1], [0], [2.0]),),
                ],
                ["mat"],
            ).write.parquet(path7)

            for path in [path1, path2, path3, path4, path5, path6, path7]:
                self.assertEqual(
                    self.connect.read.parquet(path).schema,
                    self.spark.read.parquet(path).schema,
                )

    # DataStreamReader.name() tests - require source evolution configs
    def test_stream_reader_name_valid_names(self):
        """Test that various valid source name patterns work correctly."""
        with self.connect_conf(
            {
                "spark.sql.streaming.queryEvolution.enableSourceEvolution": "true",
                "spark.sql.streaming.offsetLog.formatVersion": "2",
            }
        ):
            valid_names = [
                "mySource",
                "my_source",
                "MySource123",
                "_private",
                "source_123_test",
                "123source",
            ]

            for name in valid_names:
                with tempfile.TemporaryDirectory(prefix=f"test_{name}_") as tmpdir:
                    self.connect.range(10).write.mode("overwrite").parquet(tmpdir)
                    df = (
                        self.connect.readStream.format("parquet")
                        .schema("id LONG")
                        .name(name)
                        .load(tmpdir)
                    )
                    self.assertTrue(
                        df.isStreaming, f"DataFrame should be streaming for name: {name}"
                    )

    def test_stream_reader_name_method_chaining(self):
        """Test that name() returns the reader for method chaining."""
        with self.connect_conf(
            {
                "spark.sql.streaming.queryEvolution.enableSourceEvolution": "true",
                "spark.sql.streaming.offsetLog.formatVersion": "2",
            }
        ):
            with tempfile.TemporaryDirectory(prefix="test_chaining_") as tmpdir:
                self.connect.range(10).write.mode("overwrite").parquet(tmpdir)
                df = (
                    self.connect.readStream.format("parquet")
                    .schema("id LONG")
                    .name("my_source")
                    .option("maxFilesPerTrigger", "1")
                    .load(tmpdir)
                )

                self.assertTrue(df.isStreaming, "DataFrame should be streaming")

    def test_stream_reader_name_before_format(self):
        """Test that order doesn't matter - name can be set before format."""
        with self.connect_conf(
            {
                "spark.sql.streaming.queryEvolution.enableSourceEvolution": "true",
                "spark.sql.streaming.offsetLog.formatVersion": "2",
            }
        ):
            with tempfile.TemporaryDirectory(prefix="test_before_format_") as tmpdir:
                self.connect.range(10).write.mode("overwrite").parquet(tmpdir)
                df = (
                    self.connect.readStream.name("my_source")
                    .format("parquet")
                    .schema("id LONG")
                    .load(tmpdir)
                )

                self.assertTrue(df.isStreaming, "DataFrame should be streaming")

    def test_stream_reader_invalid_names(self):
        """Test that various invalid source names are rejected."""
        with self.connect_conf(
            {
                "spark.sql.streaming.queryEvolution.enableSourceEvolution": "true",
                "spark.sql.streaming.offsetLog.formatVersion": "2",
            }
        ):
            invalid_names = [
                "",  # empty string
                "  ",  # whitespace only
                "my-source",  # hyphen
                "my source",  # space
                "my.source",  # dot
                "my@source",  # special char
                "my$source",  # dollar sign
                "my#source",  # hash
                "my!source",  # exclamation
            ]

            for invalid_name in invalid_names:
                with self.subTest(name=invalid_name):
                    with tempfile.TemporaryDirectory(prefix="test_invalid_") as tmpdir:
                        self.connect.range(10).write.mode("overwrite").parquet(tmpdir)
                        with self.assertRaises(PySparkValueError) as context:
                            self.connect.readStream.format("parquet").schema("id LONG").name(
                                invalid_name
                            ).load(tmpdir)

                        # The error message should contain information about invalid name
                        self.assertIn("source", str(context.exception).lower())

    def test_stream_reader_invalid_name_wrong_type(self):
        """Test that None and non-string types are rejected."""
        invalid_types = [None, 123, 45.67, [], {}]

        for invalid_value in invalid_types:
            with self.subTest(value=invalid_value):
                with self.assertRaises(PySparkTypeError):
                    self.connect.readStream.format("rate").name(invalid_value).load()

    def test_stream_reader_name_with_different_formats(self):
        """Test that name() works with different streaming data sources."""
        with self.connect_conf(
            {
                "spark.sql.streaming.queryEvolution.enableSourceEvolution": "true",
                "spark.sql.streaming.offsetLog.formatVersion": "2",
            }
        ):
            with tempfile.TemporaryDirectory(prefix="test_name_formats_") as tmpdir:
                # Create test data
                self.connect.range(10).write.mode("overwrite").parquet(tmpdir + "/parquet_data")
                self.connect.range(10).selectExpr("id", "CAST(id AS STRING) as value").write.mode(
                    "overwrite"
                ).json(tmpdir + "/json_data")

                # Test with parquet
                parquet_df = (
                    self.connect.readStream.format("parquet")
                    .name("parquet_source")
                    .schema("id LONG")
                    .load(tmpdir + "/parquet_data")
                )
                self.assertTrue(parquet_df.isStreaming, "Parquet DataFrame should be streaming")

                # Test with json - specify schema
                json_df = (
                    self.connect.readStream.format("json")
                    .name("json_source")
                    .schema("id LONG, value STRING")
                    .load(tmpdir + "/json_data")
                )
                self.assertTrue(json_df.isStreaming, "JSON DataFrame should be streaming")

    def test_stream_reader_name_persists_through_query(self):
        """Test that the name persists when starting a streaming query."""
        with self.connect_conf(
            {
                "spark.sql.streaming.queryEvolution.enableSourceEvolution": "true",
                "spark.sql.streaming.offsetLog.formatVersion": "2",
            }
        ):
            with tempfile.TemporaryDirectory(prefix="test_name_query_") as tmpdir:
                data_dir = tmpdir + "/data"
                checkpoint_dir = tmpdir + "/checkpoint"

                # Create test data
                self.connect.range(10).write.mode("overwrite").parquet(data_dir)

                df = (
                    self.connect.readStream.format("parquet")
                    .schema("id LONG")
                    .name("parquet_source_test")
                    .load(data_dir)
                )

                query = (
                    df.writeStream.format("noop")
                    .option("checkpointLocation", checkpoint_dir)
                    .start()
                )

                try:
                    # Let it run briefly
                    time.sleep(1)

                    # Verify query is running
                    self.assertTrue(query.isActive, "Query should be active")
                finally:
                    query.stop()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
