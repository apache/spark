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

from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.testing.sqlutils import ReusedSQLTestCase


class DataSourcesTests(ReusedSQLTestCase):

    def test_linesep_text(self):
        df = self.spark.read.text("python/test_support/sql/ages_newlines.csv", lineSep=",")
        expected = [Row(value=u'Joe'), Row(value=u'20'), Row(value=u'"Hi'),
                    Row(value=u'\nI am Jeo"\nTom'), Row(value=u'30'),
                    Row(value=u'"My name is Tom"\nHyukjin'), Row(value=u'25'),
                    Row(value=u'"I am Hyukjin\n\nI love Spark!"\n')]
        self.assertEqual(df.collect(), expected)

        tpath = tempfile.mkdtemp()
        shutil.rmtree(tpath)
        try:
            df.write.text(tpath, lineSep="!")
            expected = [Row(value=u'Joe!20!"Hi!'), Row(value=u'I am Jeo"'),
                        Row(value=u'Tom!30!"My name is Tom"'),
                        Row(value=u'Hyukjin!25!"I am Hyukjin'),
                        Row(value=u''), Row(value=u'I love Spark!"'),
                        Row(value=u'!')]
            readback = self.spark.read.text(tpath)
            self.assertEqual(readback.collect(), expected)
        finally:
            shutil.rmtree(tpath)

    def test_multiline_json(self):
        people1 = self.spark.read.json("python/test_support/sql/people.json")
        people_array = self.spark.read.json("python/test_support/sql/people_array.json",
                                            multiLine=True)
        self.assertEqual(people1.collect(), people_array.collect())

    def test_encoding_json(self):
        people_array = self.spark.read\
            .json("python/test_support/sql/people_array_utf16le.json",
                  multiLine=True, encoding="UTF-16LE")
        expected = [Row(age=30, name=u'Andy'), Row(age=19, name=u'Justin')]
        self.assertEqual(people_array.collect(), expected)

    def test_linesep_json(self):
        df = self.spark.read.json("python/test_support/sql/people.json", lineSep=",")
        expected = [Row(_corrupt_record=None, name=u'Michael'),
                    Row(_corrupt_record=u' "age":30}\n{"name":"Justin"', name=None),
                    Row(_corrupt_record=u' "age":19}\n', name=None)]
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
            "python/test_support/sql/ages_newlines.csv", multiLine=True)
        expected = [Row(_c0=u'Joe', _c1=u'20', _c2=u'Hi,\nI am Jeo'),
                    Row(_c0=u'Tom', _c1=u'30', _c2=u'My name is Tom'),
                    Row(_c0=u'Hyukjin', _c1=u'25', _c2=u'I am Hyukjin\n\nI love Spark!')]
        self.assertEqual(ages_newlines.collect(), expected)

    def test_ignorewhitespace_csv(self):
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        self.spark.createDataFrame([[" a", "b  ", " c "]]).write.csv(
            tmpPath,
            ignoreLeadingWhiteSpace=False,
            ignoreTrailingWhiteSpace=False)

        expected = [Row(value=u' a,b  , c ')]
        readback = self.spark.read.text(tmpPath)
        self.assertEqual(readback.collect(), expected)
        shutil.rmtree(tmpPath)

    def test_read_multiple_orc_file(self):
        df = self.spark.read.orc(["python/test_support/sql/orc_partitioned/b=0/c=0",
                                  "python/test_support/sql/orc_partitioned/b=1/c=1"])
        self.assertEqual(2, df.count())

    def test_read_text_file_list(self):
        df = self.spark.read.text(['python/test_support/sql/text-test.txt',
                                   'python/test_support/sql/text-test.txt'])
        count = df.count()
        self.assertEquals(count, 4)

    def test_json_sampling_ratio(self):
        rdd = self.spark.sparkContext.range(0, 100, 1, 1) \
            .map(lambda x: '{"a":0.1}' if x == 1 else '{"a":%s}' % str(x))
        schema = self.spark.read.option('inferSchema', True) \
            .option('samplingRatio', 0.5) \
            .json(rdd).schema
        self.assertEquals(schema, StructType([StructField("a", LongType(), True)]))

    def test_csv_sampling_ratio(self):
        rdd = self.spark.sparkContext.range(0, 100, 1, 1) \
            .map(lambda x: '0.1' if x == 1 else str(x))
        schema = self.spark.read.option('inferSchema', True)\
            .csv(rdd, samplingRatio=0.5).schema
        self.assertEquals(schema, StructType([StructField("_c0", IntegerType(), True)]))

    def test_checking_csv_header(self):
        path = tempfile.mkdtemp()
        shutil.rmtree(path)
        try:
            self.spark.createDataFrame([[1, 1000], [2000, 2]])\
                .toDF('f1', 'f2').write.option("header", "true").csv(path)
            schema = StructType([
                StructField('f2', IntegerType(), nullable=True),
                StructField('f1', IntegerType(), nullable=True)])
            df = self.spark.read.option('header', 'true').schema(schema)\
                .csv(path, enforceSchema=False)
            self.assertRaisesRegexp(
                Exception,
                "CSV header does not conform to the schema",
                lambda: df.collect())
        finally:
            shutil.rmtree(path)

    def test_ignore_column_of_all_nulls(self):
        path = tempfile.mkdtemp()
        shutil.rmtree(path)
        try:
            df = self.spark.createDataFrame([["""{"a":null, "b":1, "c":3.0}"""],
                                             ["""{"a":null, "b":null, "c":"string"}"""],
                                             ["""{"a":null, "b":null, "c":null}"""]])
            df.write.text(path)
            schema = StructType([
                StructField('b', LongType(), nullable=True),
                StructField('c', StringType(), nullable=True)])
            readback = self.spark.read.json(path, dropFieldIfAllNull=True)
            self.assertEquals(readback.schema, schema)
        finally:
            shutil.rmtree(path)


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_datasources import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
