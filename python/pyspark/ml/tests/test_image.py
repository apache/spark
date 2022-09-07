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

from pyspark.ml.image import ImageSchema
from pyspark.testing.mlutils import SparkSessionTestCase
from pyspark.sql import Row
from pyspark.testing.utils import QuietTest


class ImageFileFormatTest(SparkSessionTestCase):
    def test_read_images(self):
        data_path = "data/mllib/images/origin/kittens"
        df = (
            self.spark.read.format("image")
            .option("dropInvalid", True)
            .option("recursiveFileLookup", True)
            .load(data_path)
        )
        self.assertEqual(df.count(), 4)
        first_row = df.take(1)[0][0]
        # compare `schema.simpleString()` instead of directly compare schema,
        # because the df loaded from datasource may change schema column nullability.
        self.assertEqual(df.schema.simpleString(), ImageSchema.imageSchema.simpleString())
        self.assertEqual(
            df.schema["image"].dataType.simpleString(), ImageSchema.columnSchema.simpleString()
        )
        array = ImageSchema.toNDArray(first_row)
        self.assertEqual(len(array), first_row[1])
        self.assertEqual(ImageSchema.toImage(array, origin=first_row[0]), first_row)
        expected = {"CV_8UC3": 16, "Undefined": -1, "CV_8U": 0, "CV_8UC1": 0, "CV_8UC4": 24}
        self.assertEqual(ImageSchema.ocvTypes, expected)
        expected = ["origin", "height", "width", "nChannels", "mode", "data"]
        self.assertEqual(ImageSchema.imageFields, expected)
        self.assertEqual(ImageSchema.undefinedImageType, "Undefined")

        with QuietTest(self.sc):
            self.assertRaisesRegex(
                TypeError,
                "image argument should be pyspark.sql.types.Row; however",
                lambda: ImageSchema.toNDArray("a"),
            )

        with QuietTest(self.sc):
            self.assertRaisesRegex(
                ValueError,
                "image argument should have attributes specified in",
                lambda: ImageSchema.toNDArray(Row(a=1)),
            )

        with QuietTest(self.sc):
            self.assertRaisesRegex(
                TypeError,
                "array argument should be numpy.ndarray; however, it got",
                lambda: ImageSchema.toImage("a"),
            )


if __name__ == "__main__":
    from pyspark.ml.tests.test_image import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
