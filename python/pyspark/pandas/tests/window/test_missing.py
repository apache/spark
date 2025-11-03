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

import inspect

from pyspark import pandas as ps
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.pandas.missing.window import (
    MissingPandasLikeExpanding,
    MissingPandasLikeRolling,
    MissingPandasLikeExpandingGroupby,
    MissingPandasLikeRollingGroupby,
    MissingPandasLikeExponentialMoving,
    MissingPandasLikeExponentialMovingGroupby,
)
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class MissingMixin:
    def test_missing(self):
        psdf = ps.DataFrame({"a": [1, 2, 3, 4, 5, 6, 7, 8, 9]})

        # Expanding functions
        missing_functions = inspect.getmembers(MissingPandasLikeExpanding, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Expanding.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.expanding(1), name)()  # Frame

            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Expanding.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.a.expanding(1), name)()  # Series

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Expanding.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.expanding(1), name)()  # Frame

            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Expanding.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.a.expanding(1), name)()  # Series

        # Rolling functions
        missing_functions = inspect.getmembers(MissingPandasLikeRolling, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Rolling.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.rolling(1), name)()  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Rolling.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.a.rolling(1), name)()  # Series

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Rolling.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.rolling(1), name)()  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Rolling.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.a.rolling(1), name)()  # Series

        # ExponentialMoving functions
        missing_functions = inspect.getmembers(
            MissingPandasLikeExponentialMoving, inspect.isfunction
        )
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*ExponentialMoving.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.ewm(com=0.5), name)()  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*ExponentialMoving.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.a.ewm(com=0.5), name)()  # Series

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*ExponentialMoving.*{}.*is deprecated".format(name),
            ):
                getattr(psdf.ewm(com=0.5), name)()  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*ExponentialMoving.*{}.*is deprecated".format(name),
            ):
                getattr(psdf.a.ewm(com=0.5), name)()  # Series

        # Expanding properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeExpanding, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Expanding.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.expanding(1), name)  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Expanding.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.a.expanding(1), name)  # Series

        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*Expanding.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.expanding(1), name)  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*Expanding.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.a.expanding(1), name)  # Series

        # Rolling properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeRolling, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Rolling.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.rolling(1), name)()  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Rolling.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.a.rolling(1), name)()  # Series
        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*Rolling.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.rolling(1), name)()  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*Rolling.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.a.rolling(1), name)()  # Series

        # ExponentialMoving properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeExponentialMoving, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*ExponentialMoving.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.ewm(com=0.5), name)()  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*ExponentialMoving.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.a.ewm(com=0.5), name)()  # Series
        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*ExponentialMoving.*{}.*is deprecated".format(name),
            ):
                getattr(psdf.ewm(com=0.5), name)()  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*ExponentialMoving.*{}.*is deprecated".format(name),
            ):
                getattr(psdf.a.ewm(com=0.5), name)()  # Series

    def test_missing_groupby(self):
        psdf = ps.DataFrame({"a": [1, 2, 3, 4, 5, 6, 7, 8, 9]})

        # Expanding functions
        missing_functions = inspect.getmembers(
            MissingPandasLikeExpandingGroupby, inspect.isfunction
        )
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Expanding.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.groupby("a").expanding(1), name)()  # Frame

            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Expanding.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.a.groupby(psdf.a).expanding(1), name)()  # Series

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Expanding.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.groupby("a").expanding(1), name)()  # Frame

            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Expanding.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.a.groupby(psdf.a).expanding(1), name)()  # Series

        # Rolling functions
        missing_functions = inspect.getmembers(MissingPandasLikeRollingGroupby, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Rolling.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.groupby("a").rolling(1), name)()  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Rolling.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.a.groupby(psdf.a).rolling(1), name)()  # Series

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Rolling.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.rolling(1), name)()  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Rolling.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.a.rolling(1), name)()  # Series

        # ExponentialMoving functions
        missing_functions = inspect.getmembers(
            MissingPandasLikeExponentialMovingGroupby, inspect.isfunction
        )
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*ExponentialMoving.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.groupby("a").ewm(com=0.5), name)()  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*ExponentialMoving.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.a.groupby(psdf.a).ewm(com=0.5), name)()  # Series

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*ExponentialMoving.*{}.*is deprecated".format(name),
            ):
                getattr(psdf.ewm(com=0.5), name)()  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*ExponentialMoving.*{}.*is deprecated".format(name),
            ):
                getattr(psdf.a.ewm(com=0.5), name)()  # Series

        # Expanding properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeExpandingGroupby, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Expanding.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.groupby("a").expanding(1), name)()  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Expanding.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.a.groupby(psdf.a).expanding(1), name)()  # Series

        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*Expanding.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.expanding(1), name)  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*Expanding.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.a.expanding(1), name)  # Series

        # Rolling properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeRollingGroupby, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Rolling.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.groupby("a").rolling(1), name)()  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Rolling.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.a.groupby(psdf.a).rolling(1), name)()  # Series
        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*Rolling.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.rolling(1), name)()  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*Rolling.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.a.rolling(1), name)()  # Series

        # ExponentialMoving properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeExponentialMovingGroupby, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*ExponentialMoving.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.groupby("a").ewm(com=0.5), name)()  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*ExponentialMoving.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.a.groupby(psdf.a).ewm(com=0.5), name)()  # Series
        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*ExponentialMoving.*{}.*is deprecated".format(name),
            ):
                getattr(psdf.ewm(com=0.5), name)()  # Frame
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*ExponentialMoving.*{}.*is deprecated".format(name),
            ):
                getattr(psdf.a.ewm(com=0.5), name)()  # Series


class MissingTests(
    MissingMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.window.test_missing import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
