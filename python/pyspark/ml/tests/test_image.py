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
import unittest

import py4j

from pyspark.ml.image import ImageSchema
from pyspark.testing.mlutils import PySparkTestCase, SparkSessionTestCase
from pyspark.sql import HiveContext, Row
from pyspark.testing.utils import QuietTest


class ImageReaderTest(SparkSessionTestCase):

    def test_read_images(self):
        data_path = 'data/mllib/images/origin/kittens'
        df = ImageSchema.readImages(data_path, recursive=True, dropImageFailures=True)
        self.assertEqual(df.count(), 4)
        first_row = df.take(1)[0][0]
        array = ImageSchema.toNDArray(first_row)
        self.assertEqual(len(array), first_row[1])
        self.assertEqual(ImageSchema.toImage(array, origin=first_row[0]), first_row)
        self.assertEqual(df.schema, ImageSchema.imageSchema)
        self.assertEqual(df.schema["image"].dataType, ImageSchema.columnSchema)
        expected = {'CV_8UC3': 16, 'Undefined': -1, 'CV_8U': 0, 'CV_8UC1': 0, 'CV_8UC4': 24}
        self.assertEqual(ImageSchema.ocvTypes, expected)
        expected = ['origin', 'height', 'width', 'nChannels', 'mode', 'data']
        self.assertEqual(ImageSchema.imageFields, expected)
        self.assertEqual(ImageSchema.undefinedImageType, "Undefined")

        with QuietTest(self.sc):
            self.assertRaisesRegexp(
                TypeError,
                "image argument should be pyspark.sql.types.Row; however",
                lambda: ImageSchema.toNDArray("a"))

        with QuietTest(self.sc):
            self.assertRaisesRegexp(
                ValueError,
                "image argument should have attributes specified in",
                lambda: ImageSchema.toNDArray(Row(a=1)))

        with QuietTest(self.sc):
            self.assertRaisesRegexp(
                TypeError,
                "array argument should be numpy.ndarray; however, it got",
                lambda: ImageSchema.toImage("a"))


class ImageReaderTest2(PySparkTestCase):

    @classmethod
    def setUpClass(cls):
        super(ImageReaderTest2, cls).setUpClass()
        cls.hive_available = True
        # Note that here we enable Hive's support.
        cls.spark = None
        try:
            cls.sc._jvm.org.apache.hadoop.hive.conf.HiveConf()
        except py4j.protocol.Py4JError:
            cls.tearDownClass()
            cls.hive_available = False
        except TypeError:
            cls.tearDownClass()
            cls.hive_available = False
        if cls.hive_available:
            cls.spark = HiveContext._createForTesting(cls.sc)

    def setUp(self):
        if not self.hive_available:
            self.skipTest("Hive is not available.")

    @classmethod
    def tearDownClass(cls):
        super(ImageReaderTest2, cls).tearDownClass()
        if cls.spark is not None:
            cls.spark.sparkSession.stop()
            cls.spark = None

    def test_read_images_multiple_times(self):
        # This test case is to check if `ImageSchema.readImages` tries to
        # initiate Hive client multiple times. See SPARK-22651.
        data_path = 'data/mllib/images/origin/kittens'
        ImageSchema.readImages(data_path, recursive=True, dropImageFailures=True)
        ImageSchema.readImages(data_path, recursive=True, dropImageFailures=True)


if __name__ == "__main__":
    from pyspark.ml.tests.test_image import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
