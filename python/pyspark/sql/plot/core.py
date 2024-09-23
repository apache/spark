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

from typing import Any, TYPE_CHECKING, Optional, Union
from types import ModuleType
from pyspark.errors import PySparkRuntimeError, PySparkValueError
from pyspark.sql.utils import require_minimum_plotly_version


if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    import pandas as pd
    from plotly.graph_objs import Figure


class PySparkTopNPlotBase:
    def get_top_n(self, sdf: "DataFrame") -> "pd.DataFrame":
        from pyspark.sql import SparkSession

        session = SparkSession.getActiveSession()
        if session is None:
            raise PySparkRuntimeError(errorClass="NO_ACTIVE_SESSION", messageParameters=dict())

        max_rows = int(
            session.conf.get("spark.sql.pyspark.plotting.max_rows")  # type: ignore[arg-type]
        )
        pdf = sdf.limit(max_rows + 1).toPandas()

        self.partial = False
        if len(pdf) > max_rows:
            self.partial = True
            pdf = pdf.iloc[:max_rows]

        return pdf


class PySparkSampledPlotBase:
    def get_sampled(self, sdf: "DataFrame") -> "pd.DataFrame":
        from pyspark.sql import SparkSession

        session = SparkSession.getActiveSession()
        if session is None:
            raise PySparkRuntimeError(errorClass="NO_ACTIVE_SESSION", messageParameters=dict())

        sample_ratio = session.conf.get("spark.sql.pyspark.plotting.sample_ratio")
        max_rows = int(
            session.conf.get("spark.sql.pyspark.plotting.max_rows")  # type: ignore[arg-type]
        )

        if sample_ratio is None:
            fraction = 1 / (sdf.count() / max_rows)
            fraction = min(1.0, fraction)
        else:
            fraction = float(sample_ratio)

        sampled_sdf = sdf.sample(fraction=fraction)
        pdf = sampled_sdf.toPandas()

        return pdf


class PySparkPlotAccessor:
    plot_data_map = {
        "bar": PySparkTopNPlotBase().get_top_n,
        "barh": PySparkTopNPlotBase().get_top_n,
        "line": PySparkSampledPlotBase().get_sampled,
    }
    _backends = {}  # type: ignore[var-annotated]

    def __init__(self, data: "DataFrame"):
        self.data = data

    def __call__(
        self, kind: str = "line", backend: Optional[str] = None, **kwargs: Any
    ) -> "Figure":
        plot_backend = PySparkPlotAccessor._get_plot_backend(backend)

        return plot_backend.plot_pyspark(self.data, kind=kind, **kwargs)

    @staticmethod
    def _get_plot_backend(backend: Optional[str] = None) -> ModuleType:
        backend = backend or "plotly"

        if backend in PySparkPlotAccessor._backends:
            return PySparkPlotAccessor._backends[backend]

        if backend == "plotly":
            require_minimum_plotly_version()
        else:
            raise PySparkValueError(
                errorClass="UNSUPPORTED_PLOT_BACKEND",
                messageParameters={"backend": backend, "supported_backends": ", ".join(["plotly"])},
            )
        from pyspark.sql.plot import plotly as module

        return module

    def line(self, x: str, y: Union[str, list[str]], **kwargs: Any) -> "Figure":
        """
        Plot DataFrame as lines.

        Parameters
        ----------
        x : str
            Name of column to use for the horizontal axis.
        y : str or list of str
            Name(s) of the column(s) to use for the vertical axis. Multiple columns can be plotted.
        **kwargs : optional
            Additional keyword arguments.

        Returns
        -------
        :class:`plotly.graph_objs.Figure`

        Examples
        --------
        >>> data = [("A", 10, 1.5), ("B", 30, 2.5), ("C", 20, 3.5)]
        >>> columns = ["category", "int_val", "float_val"]
        >>> df = spark.createDataFrame(data, columns)
        >>> df.plot.line(x="category", y="int_val")  # doctest: +SKIP
        >>> df.plot.line(x="category", y=["int_val", "float_val"])  # doctest: +SKIP
        """
        return self(kind="line", x=x, y=y, **kwargs)

    def bar(self, x: str, y: Union[str, list[str]], **kwargs: Any) -> "Figure":
        """
        Vertical bar plot.

        A bar plot is a plot that presents categorical data with rectangular bars with lengths
        proportional to the values that they represent. A bar plot shows comparisons among
        discrete categories. One axis of the plot shows the specific categories being compared,
        and the other axis represents a measured value.

        Parameters
        ----------
        x : str
            Name of column to use for the horizontal axis.
        y : str or list of str
            Name(s) of the column(s) to use for the vertical axis.
            Multiple columns can be plotted.
        **kwargs : optional
            Additional keyword arguments.

        Returns
        -------
        :class:`plotly.graph_objs.Figure`

        Examples
        --------
        >>> data = [("A", 10, 1.5), ("B", 30, 2.5), ("C", 20, 3.5)]
        >>> columns = ["category", "int_val", "float_val"]
        >>> df = spark.createDataFrame(data, columns)
        >>> df.plot.bar(x="category", y="int_val")  # doctest: +SKIP
        >>> df.plot.bar(x="category", y=["int_val", "float_val"])  # doctest: +SKIP
        """
        return self(kind="bar", x=x, y=y, **kwargs)

    def barh(self, x: str, y: Union[str, list[str]], **kwargs: Any) -> "Figure":
        """
        Make a horizontal bar plot.

        A horizontal bar plot is a plot that presents quantitative data with
        rectangular bars with lengths proportional to the values that they
        represent. A bar plot shows comparisons among discrete categories. One
        axis of the plot shows the specific categories being compared, and the
        other axis represents a measured value.

        Parameters
        ----------
        x : str or list of str
            Name(s) of the column(s) to use for the horizontal axis.
            Multiple columns can be plotted.
        y : str or list of str
            Name(s) of the column(s) to use for the vertical axis.
            Multiple columns can be plotted.
        **kwargs : optional
            Additional keyword arguments.

        Returns
        -------
        :class:`plotly.graph_objs.Figure`

        Notes
        -----
        In Plotly and Matplotlib, the interpretation of `x` and `y` for `barh` plots differs.
        In Plotly, `x` refers to the values and `y` refers to the categories.
        In Matplotlib, `x` refers to the categories and `y` refers to the values.
        Ensure correct axis labeling based on the backend used.

        Examples
        --------
        >>> data = [("A", 10, 1.5), ("B", 30, 2.5), ("C", 20, 3.5)]
        >>> columns = ["category", "int_val", "float_val"]
        >>> df = spark.createDataFrame(data, columns)
        >>> df.plot.barh(x="int_val", y="category")  # doctest: +SKIP
        >>> df.plot.barh(
        ...     x=["int_val", "float_val"], y="category"
        ... )  # doctest: +SKIP
        """
        return self(kind="barh", x=x, y=y, **kwargs)
