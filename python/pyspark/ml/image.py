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

import pyspark
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.types import Row, _create_row
from pyspark.sql import DataFrame
from pyspark.ml.param.shared import *
import numpy as np

undefinedImageType = "Undefined"

imageFields = ["origin", "height", "width", "nChannels", "mode", "data"]

ocvTypes = {
    undefinedImageType: -1,
    "CV_8U": 0, "CV_8UC1": 0, "CV_8UC2": 8, "CV_8UC3": 16, "CV_8UC4": 24,
    "CV_8S": 1, "CV_8SC1": 1, "CV_8SC2": 9, "CV_8SC3": 17, "CV_8SC4": 25,
    "CV_16U": 2, "CV_16UC1": 2, "CV_16UC2": 10, "CV_16UC3": 18, "CV_16UC4": 26,
    "CV_16S": 3, "CV_16SC1": 3, "CV_16SC2": 11, "CV_16SC3": 19, "CV_16SC4": 27,
    "CV_32S": 4, "CV_32SC1": 4, "CV_32SC2": 12, "CV_32SC3": 20, "CV_32SC4": 28,
    "CV_32F": 5, "CV_32FC1": 5, "CV_32FC2": 13, "CV_32FC3": 21, "CV_32FC4": 29,
    "CV_64F": 6, "CV_64FC1": 6, "CV_64FC2": 14, "CV_64FC3": 22, "CV_64FC4": 30
}

# DataFrame with a single column of images named "image" (nullable)
imageSchema = StructType(StructField("image", StructType([
    StructField(imageFields[0], StringType(),  True),
    StructField(imageFields[1], IntegerType(), False),
    StructField(imageFields[2], IntegerType(), False),
    StructField(imageFields[3], IntegerType(), False),
    # OpenCV-compatible type: CV_8UC3 in most cases
    StructField(imageFields[4], StringType(), False),
    # bytes in OpenCV-compatible order: row-wise BGR in most cases
    StructField(imageFields[5], BinaryType(), False)]), True))


# TODO: generalize to other datatypes and number of channels
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


# TODO: generalize to other datatypes and number of channels
def toImage(array, origin="", mode=ocvTypes["CV_8UC3"]):
    """
    Converts a one-dimensional array to a two-dimensional image.

    :param array (array): The array to convert to image
    :param origin (str): Path to the image
    :param mode (str): OpenCV compatible type

    :rtype object: Two dimensional image

    .. versionadded:: 2.3.0
    """
    data = bytearray(array.astype(dtype=np.uint8).ravel())
    height = array.shape[0]
    width = array.shape[1]
    nChannels = array.shape[2]
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
    :param recursive (bool): Recursive search flag
    :param numPartitions (int): Number of DataFrame partitions
    :param dropImageFailures (bool): Drop the files that are not valid images
    :param sampleRatio (double): Fraction of the images loaded
    :rtype DataFrame: DataFrame with a single column of "images",
           see ImageSchema for details

    Examples:

    >>> df = readImages('python/test_support/image/kittens', recursive=true)
    >>> df.count
    4

    .. versionadded:: 2.3.0
    """
    ctx = SparkContext.getOrCreate()
    schema = ctx._jvm.org.apache.spark.image.ImageSchema
    sql_ctx = pyspark.SQLContext.getOrCreate(ctx)
    jsession = sql_ctx.sparkSession._jsparkSession
    jresult = schema.readImages(path, jsession, recursive, numPartitions,
                                dropImageFailures, float(sampleRatio))
    return DataFrame(jresult, sql_ctx)
