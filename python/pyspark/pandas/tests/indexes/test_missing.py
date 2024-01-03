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
import inspect
from datetime import timedelta

import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.pandas.missing.indexes import (
    MissingPandasLikeDatetimeIndex,
    MissingPandasLikeIndex,
    MissingPandasLikeMultiIndex,
    MissingPandasLikeTimedeltaIndex,
)


class MissingMixin:
    def test_missing(self):
        psdf = ps.DataFrame(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
                "c": pd.date_range("2011-01-01", freq="D", periods=3),
                "d": pd.Categorical(["a", "b", "c"]),
                "e": [timedelta(1), timedelta(2), timedelta(3)],
            }
        )

        # Index functions
        missing_functions = inspect.getmembers(MissingPandasLikeIndex, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index("a").index, name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.set_index("a").index, name)()

        # MultiIndex functions
        missing_functions = inspect.getmembers(MissingPandasLikeMultiIndex, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index(["a", "b"]).index, name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.set_index(["a", "b"]).index, name)()

        # DatetimeIndex functions
        missing_functions = inspect.getmembers(MissingPandasLikeDatetimeIndex, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index("c").index, name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.set_index("c").index, name)()

        # TimedeltaIndex functions
        missing_functions = inspect.getmembers(MissingPandasLikeTimedeltaIndex, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index("e").index, name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.set_index("e").index, name)()

        # Index properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeIndex, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index("a").index, name)

        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.set_index("a").index, name)

        # MultiIndex properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeMultiIndex, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index(["a", "b"]).index, name)

        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.set_index(["a", "b"]).index, name)

        # DatetimeIndex properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeDatetimeIndex, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index("c").index, name)

        # TimedeltaIndex properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeDatetimeIndex, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index("c").index, name)

    def test_multi_index_not_supported(self):
        psdf = ps.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})

        with self.assertRaisesRegex(TypeError, "cannot perform any with this index type"):
            psdf.set_index(["a", "b"]).index.any()

        with self.assertRaisesRegex(TypeError, "cannot perform all with this index type"):
            psdf.set_index(["a", "b"]).index.all()


class MissingTests(
    MissingMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_missing import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
