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
import sys
import tempfile
import unittest
from array import array

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

    @unittest.skipIf(sys.version >= "3", "serialize array of byte")
    def test_sequencefiles(self):
        basepath = self.tempdir.name
        ints = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfint/",
                                           "org.apache.hadoop.io.IntWritable",
                                           "org.apache.hadoop.io.Text").collect())
        ei = [(1, u'aa'), (1, u'aa'), (2, u'aa'), (2, u'bb'), (2, u'bb'), (3, u'cc')]
        self.assertEqual(ints, ei)

        doubles = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfdouble/",
                                              "org.apache.hadoop.io.DoubleWritable",
                                              "org.apache.hadoop.io.Text").collect())
        ed = [(1.0, u'aa'), (1.0, u'aa'), (2.0, u'aa'), (2.0, u'bb'), (2.0, u'bb'), (3.0, u'cc')]
        self.assertEqual(doubles, ed)

        bytes = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfbytes/",
                                            "org.apache.hadoop.io.IntWritable",
                                            "org.apache.hadoop.io.BytesWritable").collect())
        ebs = [(1, bytearray('aa', 'utf-8')),
               (1, bytearray('aa', 'utf-8')),
               (2, bytearray('aa', 'utf-8')),
               (2, bytearray('bb', 'utf-8')),
               (2, bytearray('bb', 'utf-8')),
               (3, bytearray('cc', 'utf-8'))]
        self.assertEqual(bytes, ebs)

        text = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sftext/",
                                           "org.apache.hadoop.io.Text",
                                           "org.apache.hadoop.io.Text").collect())
        et = [(u'1', u'aa'),
              (u'1', u'aa'),
              (u'2', u'aa'),
              (u'2', u'bb'),
              (u'2', u'bb'),
              (u'3', u'cc')]
        self.assertEqual(text, et)

        bools = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfbool/",
                                            "org.apache.hadoop.io.IntWritable",
                                            "org.apache.hadoop.io.BooleanWritable").collect())
        eb = [(1, False), (1, True), (2, False), (2, False), (2, True), (3, True)]
        self.assertEqual(bools, eb)

        nulls = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfnull/",
                                            "org.apache.hadoop.io.IntWritable",
                                            "org.apache.hadoop.io.BooleanWritable").collect())
        en = [(1, None), (1, None), (2, None), (2, None), (2, None), (3, None)]
        self.assertEqual(nulls, en)

        maps = self.sc.sequenceFile(basepath + "/sftestdata/sfmap/",
                                    "org.apache.hadoop.io.IntWritable",
                                    "org.apache.hadoop.io.MapWritable").collect()
        em = [(1, {}),
              (1, {3.0: u'bb'}),
              (2, {1.0: u'aa'}),
              (2, {1.0: u'cc'}),
              (3, {2.0: u'dd'})]
        for v in maps:
            self.assertTrue(v in em)

        # arrays get pickled to tuples by default
        tuples = sorted(self.sc.sequenceFile(
            basepath + "/sftestdata/sfarray/",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable").collect())
        et = [(1, ()),
              (2, (3.0, 4.0, 5.0)),
              (3, (4.0, 5.0, 6.0))]
        self.assertEqual(tuples, et)

        # with custom converters, primitive arrays can stay as arrays
        arrays = sorted(self.sc.sequenceFile(
            basepath + "/sftestdata/sfarray/",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable",
            valueConverter="org.apache.spark.api.python.WritableToDoubleArrayConverter").collect())
        ea = [(1, array('d')),
              (2, array('d', [3.0, 4.0, 5.0])),
              (3, array('d', [4.0, 5.0, 6.0]))]
        self.assertEqual(arrays, ea)

        clazz = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfclass/",
                                            "org.apache.hadoop.io.Text",
                                            "org.apache.spark.api.python.TestWritable").collect())
        cname = u'org.apache.spark.api.python.TestWritable'
        ec = [(u'1', {u'__class__': cname, u'double': 1.0, u'int': 1, u'str': u'test1'}),
              (u'2', {u'__class__': cname, u'double': 2.3, u'int': 2, u'str': u'test2'}),
              (u'3', {u'__class__': cname, u'double': 3.1, u'int': 3, u'str': u'test3'}),
              (u'4', {u'__class__': cname, u'double': 4.2, u'int': 4, u'str': u'test4'}),
              (u'5', {u'__class__': cname, u'double': 5.5, u'int': 5, u'str': u'test56'})]
        self.assertEqual(clazz, ec)

        unbatched_clazz = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfclass/",
                                                      "org.apache.hadoop.io.Text",
                                                      "org.apache.spark.api.python.TestWritable",
                                                      ).collect())
        self.assertEqual(unbatched_clazz, ec)

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

    @unittest.skipIf(sys.version >= "3", "serialize array of byte")
    def test_sequencefiles(self):
        basepath = self.tempdir.name
        ei = [(1, u'aa'), (1, u'aa'), (2, u'aa'), (2, u'bb'), (2, u'bb'), (3, u'cc')]
        self.sc.parallelize(ei).saveAsSequenceFile(basepath + "/sfint/")
        ints = sorted(self.sc.sequenceFile(basepath + "/sfint/").collect())
        self.assertEqual(ints, ei)

        ed = [(1.0, u'aa'), (1.0, u'aa'), (2.0, u'aa'), (2.0, u'bb'), (2.0, u'bb'), (3.0, u'cc')]
        self.sc.parallelize(ed).saveAsSequenceFile(basepath + "/sfdouble/")
        doubles = sorted(self.sc.sequenceFile(basepath + "/sfdouble/").collect())
        self.assertEqual(doubles, ed)

        ebs = [(1, bytearray(b'\x00\x07spam\x08')), (2, bytearray(b'\x00\x07spam\x08'))]
        self.sc.parallelize(ebs).saveAsSequenceFile(basepath + "/sfbytes/")
        bytes = sorted(self.sc.sequenceFile(basepath + "/sfbytes/").collect())
        self.assertEqual(bytes, ebs)

        et = [(u'1', u'aa'),
              (u'2', u'bb'),
              (u'3', u'cc')]
        self.sc.parallelize(et).saveAsSequenceFile(basepath + "/sftext/")
        text = sorted(self.sc.sequenceFile(basepath + "/sftext/").collect())
        self.assertEqual(text, et)

        eb = [(1, False), (1, True), (2, False), (2, False), (2, True), (3, True)]
        self.sc.parallelize(eb).saveAsSequenceFile(basepath + "/sfbool/")
        bools = sorted(self.sc.sequenceFile(basepath + "/sfbool/").collect())
        self.assertEqual(bools, eb)

        en = [(1, None), (1, None), (2, None), (2, None), (2, None), (3, None)]
        self.sc.parallelize(en).saveAsSequenceFile(basepath + "/sfnull/")
        nulls = sorted(self.sc.sequenceFile(basepath + "/sfnull/").collect())
        self.assertEqual(nulls, en)

        em = [(1, {}),
              (1, {3.0: u'bb'}),
              (2, {1.0: u'aa'}),
              (2, {1.0: u'cc'}),
              (3, {2.0: u'dd'})]
        self.sc.parallelize(em).saveAsSequenceFile(basepath + "/sfmap/")
        maps = self.sc.sequenceFile(basepath + "/sfmap/").collect()
        for v in maps:
            self.assertTrue(v, em)

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

    @unittest.skipIf(sys.version >= "3", "serialize of array")
    def test_newhadoop_with_array(self):
        basepath = self.tempdir.name
        # use custom ArrayWritable types and converters to handle arrays
        array_data = [(1, array('d')),
                      (1, array('d', [1.0, 2.0, 3.0])),
                      (2, array('d', [3.0, 4.0, 5.0]))]
        self.sc.parallelize(array_data).saveAsNewAPIHadoopFile(
            basepath + "/newhadoop/",
            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable",
            valueConverter="org.apache.spark.api.python.DoubleArrayToWritableConverter")
        result = sorted(self.sc.newAPIHadoopFile(
            basepath + "/newhadoop/",
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable",
            valueConverter="org.apache.spark.api.python.WritableToDoubleArrayConverter").collect())
        self.assertEqual(result, array_data)

        conf = {
            "mapreduce.job.outputformat.class":
                "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.io.IntWritable",
            "mapreduce.job.output.value.class": "org.apache.spark.api.python.DoubleArrayWritable",
            "mapreduce.output.fileoutputformat.outputdir": basepath + "/newdataset/"
        }
        self.sc.parallelize(array_data).saveAsNewAPIHadoopDataset(
            conf,
            valueConverter="org.apache.spark.api.python.DoubleArrayToWritableConverter")
        input_conf = {"mapreduce.input.fileinputformat.inputdir": basepath + "/newdataset/"}
        new_dataset = sorted(self.sc.newAPIHadoopRDD(
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable",
            valueConverter="org.apache.spark.api.python.WritableToDoubleArrayConverter",
            conf=input_conf).collect())
        self.assertEqual(new_dataset, array_data)

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
    from pyspark.tests.test_readwrite import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
