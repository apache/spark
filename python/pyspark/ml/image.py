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

from pyspark import SparkContext
from pyspark.sql.types import Row, _create_row, _parse_datatype_json_string
from pyspark.sql import DataFrame, SparkSession
import numpy as np

undefinedImageType = "Undefined"

imageFields = ["origin", "height", "width", "nChannels", "mode", "data"]

class _ImageSchema(object):
    """
    Returns the image schema.

    :rtype StructType: a DataFrame with a single column of images named "image" (nullable)

    .. versionadded:: 2.3.0
    """
    @property
    def imageSchema(self):
        ctx = SparkContext._active_spark_context
        jschema = ctx._jvm.org.apache.spark.ml.image.ImageSchema.imageSchema()
        return _parse_datatype_json_string(jschema.json())


    """
    Returns the OpenCV type mapping supported

    :rtype dict: The OpenCV type mapping supported

    .. versionadded:: 2.3.0
    """
    @property
    def ocvTypes(self):
        ctx = SparkContext._active_spark_context
        return ctx._jvm.org.apache.spark.ml.image.ImageSchema._ocvTypes()

ImageSchema = _ImageSchema()


def toNDArray(image):
    """
    Converts an image to a one-dimensional array.

    :param image (object): The image to be converted
    :rtype array: The image as a one-dimensional array

    .. versionadded:: 2.3.0
    """
    height = image.height
    width = image.width
    nChannels = image.nChannels
    return np.ndarray(
        shape=(height, width, nChannels),
        dtype=np.uint8,
        buffer=image.data,
        strides=(width * nChannels, nChannels, 1))


def toImage(array, origin=""):
    """
    Converts a one-dimensional array to a two-dimensional image.

    :param array (array): The array to convert to image
    :param origin (str): Path to the image
    :rtype object: Two dimensional image

    .. versionadded:: 2.3.0
    """
    if array.ndim != 3:
        raise ValueError("Invalid array shape")
    height, width, nChannels = array.shape
    ocvTypes = ImageSchema.ocvTypes
    if nChannels == 1:
        mode = ocvTypes["CV_8UC1"]
    elif nChannels == 3:
        mode = ocvTypes["CV_8UC3"]
    elif nChannels == 4:
        mode = ocvTypes["CV_8UC4"]
    else:
        raise ValueError("Invalid number of channels")
    data = bytearray(array.astype(dtype=np.uint8).ravel())
    # Creating new Row with _create_row(), because Row(name = value, ... )
    # orders fields by name, which conflicts with expected schema order
    # when the new DataFrame is created by UDF
    return _create_row(imageFields,
                       [origin, height, width, nChannels, mode, data])


def readImages(path, recursive=False, numPartitions=0,
               dropImageFailures=False, sampleRatio=1.0):
    """
    Reads the directory of images from the local or remote source.

    :param path (str): Path to the image directory
    :param spark (SparkSession): The current spark session
    :param recursive (bool): Recursive search flag
    :param numPartitions (int): Number of DataFrame partitions
    :param dropImageFailures (bool): Drop the files that are not valid images
    :param sampleRatio (double): Fraction of the images loaded
    :rtype DataFrame: DataFrame with a single column of "images",
           see ImageSchema for details

    Examples:

    >>> df = readImages('python/test_support/image/kittens', recursive=True)
    >>> df.count
    4

    .. versionadded:: 2.3.0
    """
    ctx = SparkContext._active_spark_context
    spark = SparkSession(ctx)
    image_schema = ctx._jvm.org.apache.spark.ml.image.ImageSchema
    jsession = spark._jsparkSession
    jresult = image_schema.readImages(path, jsession, recursive, numPartitions,
                                      dropImageFailures, float(sampleRatio))
    return DataFrame(jresult, spark._wrapped)
