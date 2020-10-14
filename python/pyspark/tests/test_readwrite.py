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
import unittest

from pyspark.testing.utils import ReusedPySparkTestCase, SPARK_HOME


class InputFormatTests(ReusedPySparkTestCase):

    @classmethod
    def setUpClass(cls):
        ReusedPySparkTestCase.setUpClass()
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(cls.tempdir.name)
        cls.sc._jvm.WriteInputFormatTestDataGenerator.generateData(cls.tempdir.name, cls.sc._jsc)

    @classmethod
    def tearDownClass(cls):
        ReusedPySparkTestCase.tearDownClass()
        shutil.rmtree(cls.tempdir.name)

    def test_oldhadoop(self):
        basepath = self.tempdir.name
        ints = sorted(self.sc.hadoopFile(basepath + "/sftestdata/sfint/",
                                         "org.apache.hadoop.mapred.SequenceFileInputFormat",
                                         "org.apache.hadoop.io.IntWritable",
                                         "org.apache.hadoop.io.Text").collect())
        ei = [(1, u'aa'), (1, u'aa'), (2, u'aa'), (2, u'bb'), (2, u'bb'), (3, u'cc')]
        self.assertEqual(ints, ei)

        hellopath = os.path.join(SPARK_HOME, "python/test_support/hello/hello.txt")
        oldconf = {"mapreduce.input.fileinputformat.inputdir": hellopath}
        hello = self.sc.hadoopRDD("org.apache.hadoop.mapred.TextInputFormat",
                                  "org.apache.hadoop.io.LongWritable",
                                  "org.apache.hadoop.io.Text",
                                  conf=oldconf).collect()
        result = [(0, u'Hello World!')]
        self.assertEqual(hello, result)

    def test_newhadoop(self):
        basepath = self.tempdir.name
        ints = sorted(self.sc.newAPIHadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text").collect())
        ei = [(1, u'aa'), (1, u'aa'), (2, u'aa'), (2, u'bb'), (2, u'bb'), (3, u'cc')]
        self.assertEqual(ints, ei)

        hellopath = os.path.join(SPARK_HOME, "python/test_support/hello/hello.txt")
        newconf = {"mapreduce.input.fileinputformat.inputdir": hellopath}
        hello = self.sc.newAPIHadoopRDD("org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                                        "org.apache.hadoop.io.LongWritable",
                                        "org.apache.hadoop.io.Text",
                                        conf=newconf).collect()
        result = [(0, u'Hello World!')]
        self.assertEqual(hello, result)

    def test_newolderror(self):
        basepath = self.tempdir.name
        self.assertRaises(Exception, lambda: self.sc.hadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text"))

        self.assertRaises(Exception, lambda: self.sc.newAPIHadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text"))

    def test_bad_inputs(self):
        basepath = self.tempdir.name
        self.assertRaises(Exception, lambda: self.sc.sequenceFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.io.NotValidWritable",
            "org.apache.hadoop.io.Text"))
        self.assertRaises(Exception, lambda: self.sc.hadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapred.NotValidInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text"))
        self.assertRaises(Exception, lambda: self.sc.newAPIHadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapreduce.lib.input.NotValidInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text"))

    def test_converters(self):
        # use of custom converters
        basepath = self.tempdir.name
        maps = sorted(self.sc.sequenceFile(
            basepath + "/sftestdata/sfmap/",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.MapWritable",
            keyConverter="org.apache.spark.api.python.TestInputKeyConverter",
            valueConverter="org.apache.spark.api.python.TestInputValueConverter").collect())
        em = [(u'\x01', []),
              (u'\x01', [3.0]),
              (u'\x02', [1.0]),
              (u'\x02', [1.0]),
              (u'\x03', [2.0])]
        self.assertEqual(maps, em)

    def test_binary_files(self):
        path = os.path.join(self.tempdir.name, "binaryfiles")
        os.mkdir(path)
        data = b"short binary data"
        with open(os.path.join(path, "part-0000"), 'wb') as f:
            f.write(data)
        [(p, d)] = self.sc.binaryFiles(path).collect()
        self.assertTrue(p.endswith("part-0000"))
        self.assertEqual(d, data)

    def test_binary_records(self):
        path = os.path.join(self.tempdir.name, "binaryrecords")
        os.mkdir(path)
        with open(os.path.join(path, "part-0000"), 'w') as f:
            for i in range(100):
                f.write('%04d' % i)
        result = self.sc.binaryRecords(path, 4).map(int).collect()
        self.assertEqual(list(range(100)), result)


class OutputFormatTests(ReusedPySparkTestCase):

    def setUp(self):
        self.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(self.tempdir.name)

    def tearDown(self):
        shutil.rmtree(self.tempdir.name, ignore_errors=True)

    def test_oldhadoop(self):
        basepath = self.tempdir.name
        dict_data = [(1, {}),
                     (1, {"row1": 1.0}),
                     (2, {"row2": 2.0})]
        self.sc.parallelize(dict_data).saveAsHadoopFile(
            basepath + "/oldhadoop/",
            "org.apache.hadoop.mapred.SequenceFileOutputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.MapWritable")
        result = self.sc.hadoopFile(
            basepath + "/oldhadoop/",
            "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.MapWritable").collect()
        for v in result:
            self.assertTrue(v, dict_data)

        conf = {
            "mapred.output.format.class": "org.apache.hadoop.mapred.SequenceFileOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.io.IntWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.MapWritable",
            "mapreduce.output.fileoutputformat.outputdir": basepath + "/olddataset/"
        }
        self.sc.parallelize(dict_data).saveAsHadoopDataset(conf)
        input_conf = {"mapreduce.input.fileinputformat.inputdir": basepath + "/olddataset/"}
        result = self.sc.hadoopRDD(
            "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.MapWritable",
            conf=input_conf).collect()
        for v in result:
            self.assertTrue(v, dict_data)

    def test_newhadoop(self):
        basepath = self.tempdir.name
        data = [(1, ""),
                (1, "a"),
                (2, "bcdf")]
        self.sc.parallelize(data).saveAsNewAPIHadoopFile(
            basepath + "/newhadoop/",
            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text")
        result = sorted(self.sc.newAPIHadoopFile(
            basepath + "/newhadoop/",
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text").collect())
        self.assertEqual(result, data)

        conf = {
            "mapreduce.job.outputformat.class":
                "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.io.IntWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.Text",
            "mapreduce.output.fileoutputformat.outputdir": basepath + "/newdataset/"
        }
        self.sc.parallelize(data).saveAsNewAPIHadoopDataset(conf)
        input_conf = {"mapreduce.input.fileinputformat.inputdir": basepath + "/newdataset/"}
        new_dataset = sorted(self.sc.newAPIHadoopRDD(
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text",
            conf=input_conf).collect())
        self.assertEqual(new_dataset, data)

    def test_newolderror(self):
        basepath = self.tempdir.name
        rdd = self.sc.parallelize(range(1, 4)).map(lambda x: (x, "a" * x))
        self.assertRaises(Exception, lambda: rdd.saveAsHadoopFile(
            basepath + "/newolderror/saveAsHadoopFile/",
            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"))
        self.assertRaises(Exception, lambda: rdd.saveAsNewAPIHadoopFile(
            basepath + "/newolderror/saveAsNewAPIHadoopFile/",
            "org.apache.hadoop.mapred.SequenceFileOutputFormat"))

    def test_bad_inputs(self):
        basepath = self.tempdir.name
        rdd = self.sc.parallelize(range(1, 4)).map(lambda x: (x, "a" * x))
        self.assertRaises(Exception, lambda: rdd.saveAsHadoopFile(
            basepath + "/badinputs/saveAsHadoopFile/",
            "org.apache.hadoop.mapred.NotValidOutputFormat"))
        self.assertRaises(Exception, lambda: rdd.saveAsNewAPIHadoopFile(
            basepath + "/badinputs/saveAsNewAPIHadoopFile/",
            "org.apache.hadoop.mapreduce.lib.output.NotValidOutputFormat"))

    def test_converters(self):
        # use of custom converters
        basepath = self.tempdir.name
        data = [(1, {3.0: u'bb'}),
                (2, {1.0: u'aa'}),
                (3, {2.0: u'dd'})]
        self.sc.parallelize(data).saveAsNewAPIHadoopFile(
            basepath + "/converters/",
            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
            keyConverter="org.apache.spark.api.python.TestOutputKeyConverter",
            valueConverter="org.apache.spark.api.python.TestOutputValueConverter")
        converted = sorted(self.sc.sequenceFile(basepath + "/converters/").collect())
        expected = [(u'1', 3.0),
                    (u'2', 1.0),
                    (u'3', 2.0)]
        self.assertEqual(converted, expected)

    def test_reserialization(self):
        basepath = self.tempdir.name
        x = range(1, 5)
        y = range(1001, 1005)
        data = list(zip(x, y))
        rdd = self.sc.parallelize(x).zip(self.sc.parallelize(y))
        rdd.saveAsSequenceFile(basepath + "/reserialize/sequence")
        result1 = sorted(self.sc.sequenceFile(basepath + "/reserialize/sequence").collect())
        self.assertEqual(result1, data)

        rdd.saveAsHadoopFile(
            basepath + "/reserialize/hadoop",
            "org.apache.hadoop.mapred.SequenceFileOutputFormat")
        result2 = sorted(self.sc.sequenceFile(basepath + "/reserialize/hadoop").collect())
        self.assertEqual(result2, data)

        rdd.saveAsNewAPIHadoopFile(
            basepath + "/reserialize/newhadoop",
            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat")
        result3 = sorted(self.sc.sequenceFile(basepath + "/reserialize/newhadoop").collect())
        self.assertEqual(result3, data)

        conf4 = {
            "mapred.output.format.class": "org.apache.hadoop.mapred.SequenceFileOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.io.IntWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.IntWritable",
            "mapreduce.output.fileoutputformat.outputdir": basepath + "/reserialize/dataset"}
        rdd.saveAsHadoopDataset(conf4)
        result4 = sorted(self.sc.sequenceFile(basepath + "/reserialize/dataset").collect())
        self.assertEqual(result4, data)

        conf5 = {"mapreduce.job.outputformat.class":
                 "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
                 "mapreduce.job.output.key.class": "org.apache.hadoop.io.IntWritable",
                 "mapreduce.job.output.value.class": "org.apache.hadoop.io.IntWritable",
                 "mapreduce.output.fileoutputformat.outputdir": basepath + "/reserialize/newdataset"
                 }
        rdd.saveAsNewAPIHadoopDataset(conf5)
        result5 = sorted(self.sc.sequenceFile(basepath + "/reserialize/newdataset").collect())
        self.assertEqual(result5, data)

    def test_malformed_RDD(self):
        basepath = self.tempdir.name
        # non-batch-serialized RDD[[(K, V)]] should be rejected
        data = [[(1, "a")], [(2, "aa")], [(3, "aaa")]]
        rdd = self.sc.parallelize(data, len(data))
        self.assertRaises(Exception, lambda: rdd.saveAsSequenceFile(
            basepath + "/malformed/sequence"))


if __name__ == "__main__":
    from pyspark.tests.test_readwrite import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
