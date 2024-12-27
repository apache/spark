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

"""
.. attribute:: ImageSchema

    An attribute of this module that contains the instance of :class:`_ImageSchema`.

.. autoclass:: _ImageSchema
   :members:
"""

import sys
from typing import Any, Dict, List, NoReturn, cast
from functools import cached_property

import numpy as np

from pyspark.sql.types import Row, StructType, _create_row, _parse_datatype_json_string
from pyspark.sql import SparkSession

__all__ = ["ImageSchema"]


class _ImageSchema:
    """
    Internal class for `pyspark.ml.image.ImageSchema` attribute. Meant to be private and
    not to be instantized. Use `pyspark.ml.image.ImageSchema` attribute to access the
    APIs of this class.
    """

    @cached_property
    def imageSchema(self) -> StructType:
        """
        Returns the image schema.

        Returns
        -------
        :class:`StructType`
            with a single column of images named "image" (nullable)
            and having the same type returned by :meth:`columnSchema`.

        .. versionadded:: 2.3.0
        """
        from pyspark.core.context import SparkContext

        ctx = SparkContext._active_spark_context
        assert ctx is not None and ctx._jvm is not None
        jschema = getattr(ctx._jvm, "org.apache.spark.ml.image.ImageSchema").imageSchema()
        return cast(StructType, _parse_datatype_json_string(jschema.json()))

    @cached_property
    def ocvTypes(self) -> Dict[str, int]:
        """
        Returns the OpenCV type mapping supported.

        Returns
        -------
        dict
            a dictionary containing the OpenCV type mapping supported.

        .. versionadded:: 2.3.0
        """
        from pyspark.core.context import SparkContext

        ctx = SparkContext._active_spark_context
        assert ctx is not None and ctx._jvm is not None
        return dict(getattr(ctx._jvm, "org.apache.spark.ml.image.ImageSchema").javaOcvTypes())

    @cached_property
    def columnSchema(self) -> StructType:
        """
        Returns the schema for the image column.

        Returns
        -------
        :class:`StructType`
            a schema for image column,
            ``struct<origin:string, height:int, width:int, nChannels:int, mode:int, data:binary>``.

        .. versionadded:: 2.4.0
        """
        from pyspark.core.context import SparkContext

        ctx = SparkContext._active_spark_context
        assert ctx is not None and ctx._jvm is not None
        jschema = getattr(ctx._jvm, "org.apache.spark.ml.image.ImageSchema").columnSchema()
        return cast(StructType, _parse_datatype_json_string(jschema.json()))

    @cached_property
    def imageFields(self) -> List[str]:
        """
        Returns field names of image columns.

        Returns
        -------
        list
            a list of field names.

        .. versionadded:: 2.3.0
        """
        from pyspark.core.context import SparkContext

        ctx = SparkContext._active_spark_context
        assert ctx is not None and ctx._jvm is not None
        return list(getattr(ctx._jvm, "org.apache.spark.ml.image.ImageSchema").imageFields())

    @cached_property
    def undefinedImageType(self) -> str:
        """
        Returns the name of undefined image type for the invalid image.

        .. versionadded:: 2.3.0
        """
        from pyspark.core.context import SparkContext

        ctx = SparkContext._active_spark_context
        assert ctx is not None and ctx._jvm is not None
        return getattr(ctx._jvm, "org.apache.spark.ml.image.ImageSchema").undefinedImageType()

    def toNDArray(self, image: Row) -> np.ndarray:
        """
        Converts an image to an array with metadata.

        Parameters
        ----------
        image : :class:`Row`
            image: A row that contains the image to be converted. It should
            have the attributes specified in `ImageSchema.imageSchema`.

        Returns
        -------
        :class:`numpy.ndarray`
            that is an image.

        .. versionadded:: 2.3.0
        """

        if not isinstance(image, Row):
            raise TypeError(
                "image argument should be pyspark.sql.types.Row; however, "
                "it got [%s]." % type(image)
            )

        if any(not hasattr(image, f) for f in self.imageFields):
            raise ValueError(
                "image argument should have attributes specified in "
                "ImageSchema.imageSchema [%s]." % ", ".join(self.imageFields)
            )

        height = image.height
        width = image.width
        nChannels = image.nChannels
        return np.ndarray(
            shape=(height, width, nChannels),
            dtype=np.uint8,
            buffer=image.data,
            strides=(width * nChannels, nChannels, 1),
        )

    def toImage(self, array: np.ndarray, origin: str = "") -> Row:
        """
        Converts an array with metadata to a two-dimensional image.

        Parameters
        ----------
        array : :class:`numpy.ndarray`
            The array to convert to image.
        origin : str
            Path to the image, optional.

        Returns
        -------
        :class:`Row`
            that is a two dimensional image.

        .. versionadded:: 2.3.0
        """

        if not isinstance(array, np.ndarray):
            raise TypeError(
                "array argument should be numpy.ndarray; however, it got [%s]." % type(array)
            )

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

        data = bytearray(array.astype(dtype=np.uint8).ravel().tobytes())

        # Creating new Row with _create_row(), because Row(name = value, ... )
        # orders fields by name, which conflicts with expected schema order
        # when the new DataFrame is created by UDF
        return _create_row(self.imageFields, [origin, height, width, nChannels, mode, data])


ImageSchema = _ImageSchema()


# Monkey patch to disallow instantiation of this class.
def _disallow_instance(_: Any) -> NoReturn:
    raise RuntimeError("Creating instance of _ImageSchema class is disallowed.")


_ImageSchema.__init__ = _disallow_instance  # type: ignore[assignment]


def _test() -> None:
    import doctest
    import pyspark.ml.image

    globs = pyspark.ml.image.__dict__.copy()
    spark = SparkSession.builder.master("local[2]").appName("ml.image tests").getOrCreate()
    globs["spark"] = spark

    (failure_count, test_count) = doctest.testmod(
        pyspark.ml.image, globs=globs, optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
