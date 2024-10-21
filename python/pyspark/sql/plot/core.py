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

import math

from typing import Any, TYPE_CHECKING, List, Optional, Union
from types import ModuleType
from pyspark.errors import (
    PySparkRuntimeError,
    PySparkTypeError,
    PySparkValueError,
)
from pyspark.sql import Column, functions as F
from pyspark.sql.types import NumericType
from pyspark.sql.utils import is_remote, require_minimum_plotly_version
from pandas.core.dtypes.inference import is_integer


if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row
    from pyspark.sql._typing import ColumnOrName
    import pandas as pd
    import numpy as np
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
        from pyspark.sql import SparkSession, Observation, functions as F

        session = SparkSession.getActiveSession()
        if session is None:
            raise PySparkRuntimeError(errorClass="NO_ACTIVE_SESSION", messageParameters=dict())

        max_rows = int(
            session.conf.get("spark.sql.pyspark.plotting.max_rows")  # type: ignore[arg-type]
        )

        observation = Observation("pyspark plotting")

        rand_col_name = "__pyspark_plotting_sampled_plot_base_rand__"
        id_col_name = "__pyspark_plotting_sampled_plot_base_id__"

        sampled_sdf = (
            sdf.observe(observation, F.count(F.lit(1)).alias("count"))
            .select(
                "*",
                F.rand().alias(rand_col_name),
                F.monotonically_increasing_id().alias(id_col_name),
            )
            .sort(rand_col_name)
            .limit(max_rows + 1)
            .coalesce(1)
            .sortWithinPartitions(id_col_name)
            .drop(rand_col_name, id_col_name)
        )
        pdf = sampled_sdf.toPandas()

        if len(pdf) > max_rows:
            try:
                self.fraction = float(max_rows) / observation.get["count"]
            except Exception:
                pass
            return pdf[:max_rows]
        else:
            self.fraction = 1.0
            return pdf


class PySparkPlotAccessor:
    plot_data_map = {
        "area": PySparkSampledPlotBase().get_sampled,
        "bar": PySparkTopNPlotBase().get_top_n,
        "barh": PySparkTopNPlotBase().get_top_n,
        "line": PySparkSampledPlotBase().get_sampled,
        "pie": PySparkTopNPlotBase().get_top_n,
        "scatter": PySparkSampledPlotBase().get_sampled,
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

    def scatter(self, x: str, y: str, **kwargs: Any) -> "Figure":
        """
        Create a scatter plot with varying marker point size and color.

        The coordinates of each point are defined by two dataframe columns and
        filled circles are used to represent each point. This kind of plot is
        useful to see complex correlations between two variables. Points could
        be for instance natural 2D coordinates like longitude and latitude in
        a map or, in general, any pair of metrics that can be plotted against
        each other.

        Parameters
        ----------
        x : str
            Name of column to use as horizontal coordinates for each point.
        y : str or list of str
            Name of column to use as vertical coordinates for each point.
        **kwargs: Optional
            Additional keyword arguments.

        Returns
        -------
        :class:`plotly.graph_objs.Figure`

        Examples
        --------
        >>> data = [(5.1, 3.5, 0), (4.9, 3.0, 0), (7.0, 3.2, 1), (6.4, 3.2, 1), (5.9, 3.0, 2)]
        >>> columns = ['length', 'width', 'species']
        >>> df = spark.createDataFrame(data, columns)
        >>> df.plot.scatter(x='length', y='width')  # doctest: +SKIP
        """
        return self(kind="scatter", x=x, y=y, **kwargs)

    def area(self, x: str, y: Union[str, list[str]], **kwargs: Any) -> "Figure":
        """
        Draw a stacked area plot.

        An area plot displays quantitative data visually.

        Parameters
        ----------
        x : str
            Name of column to use for the horizontal axis.
        y : str or list of str
            Name(s) of the column(s) to plot.
        **kwargs: Optional
            Additional keyword arguments.

        Returns
        -------
        :class:`plotly.graph_objs.Figure`

        Examples
        --------
        >>> from datetime import datetime
        >>> data = [
        ...     (3, 5, 20, datetime(2018, 1, 31)),
        ...     (2, 5, 42, datetime(2018, 2, 28)),
        ...     (3, 6, 28, datetime(2018, 3, 31)),
        ...     (9, 12, 62, datetime(2018, 4, 30))
        ... ]
        >>> columns = ["sales", "signups", "visits", "date"]
        >>> df = spark.createDataFrame(data, columns)
        >>> df.plot.area(x='date', y=['sales', 'signups', 'visits'])  # doctest: +SKIP
        """
        return self(kind="area", x=x, y=y, **kwargs)

    def pie(self, x: str, y: str, **kwargs: Any) -> "Figure":
        """
        Generate a pie plot.

        A pie plot is a proportional representation of the numerical data in a
        column.

        Parameters
        ----------
        x : str
            Name of column to be used as the category labels for the pie plot.
        y : str
            Name of the column to plot.
        **kwargs
            Additional keyword arguments.

        Returns
        -------
        :class:`plotly.graph_objs.Figure`

        Examples
        --------
        >>> from datetime import datetime
        >>> data = [
        ...     (3, 5, 20, datetime(2018, 1, 31)),
        ...     (2, 5, 42, datetime(2018, 2, 28)),
        ...     (3, 6, 28, datetime(2018, 3, 31)),
        ...     (9, 12, 62, datetime(2018, 4, 30))
        ... ]
        >>> columns = ["sales", "signups", "visits", "date"]
        >>> df = spark.createDataFrame(data, columns)
        >>> df.plot.pie(x='date', y='sales')  # doctest: +SKIP
        """
        schema = self.data.schema

        # Check if 'y' is a numerical column
        y_field = schema[y] if y in schema.names else None
        if y_field is None or not isinstance(y_field.dataType, NumericType):
            raise PySparkTypeError(
                errorClass="PLOT_NOT_NUMERIC_COLUMN",
                messageParameters={
                    "arg_name": "y",
                    "arg_type": str(y_field.dataType) if y_field else "None",
                },
            )
        return self(kind="pie", x=x, y=y, **kwargs)

    def box(self, column: Union[str, List[str]], **kwargs: Any) -> "Figure":
        """
        Make a box plot of the DataFrame columns.

        Make a box-and-whisker plot from DataFrame columns, optionally grouped by some
        other columns. A box plot is a method for graphically depicting groups of numerical
        data through their quartiles. The box extends from the Q1 to Q3 quartile values of
        the data, with a line at the median (Q2). The whiskers extend from the edges of box
        to show the range of the data. By default, they extend no more than
        1.5 * IQR (IQR = Q3 - Q1) from the edges of the box, ending at the farthest data point
        within that interval. Outliers are plotted as separate dots.

        Parameters
        ----------
        column: str or list of str
            Column name or list of names to be used for creating the boxplot.
        **kwargs
            Extra arguments to `precision`: refer to a float that is used by
            pyspark to compute approximate statistics for building a boxplot.
            The default value is 0.01. Use smaller values to get more precise statistics.

        Returns
        -------
        :class:`plotly.graph_objs.Figure`

        Examples
        --------
        >>> data = [
        ...     ("A", 50, 55),
        ...     ("B", 55, 60),
        ...     ("C", 60, 65),
        ...     ("D", 65, 70),
        ...     ("E", 70, 75),
        ...     ("F", 10, 15),
        ...     ("G", 85, 90),
        ...     ("H", 5, 150),
        ... ]
        >>> columns = ["student", "math_score", "english_score"]
        >>> df = spark.createDataFrame(data, columns)
        >>> df.plot.box(column="math_score")  # doctest: +SKIP
        >>> df.plot.box(column=["math_score", "english_score"])  # doctest: +SKIP
        """
        return self(kind="box", column=column, **kwargs)

    def kde(
        self,
        column: Union[str, List[str]],
        bw_method: Union[int, float],
        ind: Union["np.ndarray", int, None] = None,
        **kwargs: Any,
    ) -> "Figure":
        """
        Generate Kernel Density Estimate plot using Gaussian kernels.

        In statistics, kernel density estimation (KDE) is a non-parametric way to
        estimate the probability density function (PDF) of a random variable. This
        function uses Gaussian kernels and includes automatic bandwidth determination.

        Parameters
        ----------
        column: str or list of str
            Column name or list of names to be used for creating the kde plot.
        bw_method : int or float
            The method used to calculate the estimator bandwidth.
            See KernelDensity in PySpark for more information.
        ind : NumPy array or integer, optional
            Evaluation points for the estimated PDF. If None (default),
            1000 equally spaced points are used. If `ind` is a NumPy array, the
            KDE is evaluated at the points passed. If `ind` is an integer,
            `ind` number of equally spaced points are used.
        **kwargs : optional
            Additional keyword arguments.

        Returns
        -------
        :class:`plotly.graph_objs.Figure`

        Examples
        --------
        >>> data = [(5.1, 3.5, 0), (4.9, 3.0, 0), (7.0, 3.2, 1), (6.4, 3.2, 1), (5.9, 3.0, 2)]
        >>> columns = ["length", "width", "species"]
        >>> df = spark.createDataFrame(data, columns)
        >>> df.plot.kde(column=["length", "width"], bw_method=0.3)  # doctest: +SKIP
        >>> df.plot.kde(column="length", bw_method=0.3)  # doctest: +SKIP
        """
        return self(kind="kde", column=column, bw_method=bw_method, ind=ind, **kwargs)


class PySparkKdePlotBase:
    @staticmethod
    def get_ind(sdf: "DataFrame", ind: Union["np.ndarray", int, None]) -> "np.ndarray":
        from pyspark.sql.pandas.utils import require_minimum_numpy_version

        require_minimum_numpy_version()
        import numpy as np

        def calc_min_max() -> "Row":
            if len(sdf.columns) > 1:
                min_col = F.least(*map(F.min, sdf))  # type: ignore
                max_col = F.greatest(*map(F.max, sdf))  # type: ignore
            else:
                min_col = F.min(sdf.columns[-1])
                max_col = F.max(sdf.columns[-1])
            return sdf.select(min_col, max_col).first()  # type: ignore

        if ind is None:
            min_val, max_val = calc_min_max()
            sample_range = max_val - min_val
            ind = np.linspace(
                min_val - 0.5 * sample_range,
                max_val + 0.5 * sample_range,
                1000,
            )
        elif is_integer(ind):
            min_val, max_val = calc_min_max()
            sample_range = max_val - min_val
            ind = np.linspace(
                min_val - 0.5 * sample_range,
                max_val + 0.5 * sample_range,
                ind,
            )
        return ind  # type: ignore

    @staticmethod
    def compute_kde_col(
        input_col: Column,
        bw_method: Union[int, float],
        ind: "np.ndarray",
    ) -> Column:
        # refers to org.apache.spark.mllib.stat.KernelDensity
        assert bw_method is not None and isinstance(
            bw_method, (int, float)
        ), "'bw_method' must be set as a scalar number."

        assert ind is not None, "'ind' must be a scalar array."

        bandwidth = float(bw_method)
        points = [float(i) for i in ind]
        log_std_plus_half_log2_pi = math.log(bandwidth) + 0.5 * math.log(2 * math.pi)

        def norm_pdf(
            mean: Column,
            std: Column,
            log_std_plus_half_log2_pi: Column,
            x: Column,
        ) -> Column:
            x0 = x - mean
            x1 = x0 / std
            log_density = -0.5 * x1 * x1 - log_std_plus_half_log2_pi
            return F.exp(log_density)

        return F.array(
            [
                F.avg(
                    norm_pdf(
                        input_col.cast("double"),
                        F.lit(bandwidth),
                        F.lit(log_std_plus_half_log2_pi),
                        F.lit(point),
                    )
                )
                for point in points
            ]
        )


class PySparkBoxPlotBase:
    @staticmethod
    def compute_box(
        sdf: "DataFrame", colnames: List[str], whis: float, precision: float, showfliers: bool
    ) -> Optional["Row"]:
        assert len(colnames) > 0
        formatted_colnames = ["`{}`".format(colname) for colname in colnames]

        stats_scols = []
        for i, colname in enumerate(formatted_colnames):
            percentiles = F.percentile_approx(colname, [0.25, 0.50, 0.75], int(1.0 / precision))
            q1 = F.get(percentiles, 0)
            med = F.get(percentiles, 1)
            q3 = F.get(percentiles, 2)
            iqr = q3 - q1
            lfence = q1 - F.lit(whis) * iqr
            ufence = q3 + F.lit(whis) * iqr

            stats_scols.append(
                F.struct(
                    F.mean(colname).alias("mean"),
                    med.alias("med"),
                    q1.alias("q1"),
                    q3.alias("q3"),
                    lfence.alias("lfence"),
                    ufence.alias("ufence"),
                ).alias(f"_box_plot_stats_{i}")
            )

        sdf_stats = sdf.select(*stats_scols)

        result_scols = []
        for i, colname in enumerate(formatted_colnames):
            value = F.col(colname)

            lfence = F.col(f"_box_plot_stats_{i}.lfence")
            ufence = F.col(f"_box_plot_stats_{i}.ufence")
            mean = F.col(f"_box_plot_stats_{i}.mean")
            med = F.col(f"_box_plot_stats_{i}.med")
            q1 = F.col(f"_box_plot_stats_{i}.q1")
            q3 = F.col(f"_box_plot_stats_{i}.q3")

            outlier = ~value.between(lfence, ufence)

            # Computes min and max values of non-outliers - the whiskers
            upper_whisker = F.max(F.when(~outlier, value).otherwise(F.lit(None)))
            lower_whisker = F.min(F.when(~outlier, value).otherwise(F.lit(None)))

            # If it shows fliers, take the top 1k with the highest absolute values
            # Here we normalize the values by subtracting the median.
            if showfliers:
                pair = F.when(
                    outlier,
                    F.struct(F.abs(value - med), value.alias("val")),
                ).otherwise(F.lit(None))
                topk = collect_top_k(pair, 1001, False)
                fliers = F.when(F.size(topk) > 0, topk["val"]).otherwise(F.lit(None))
            else:
                fliers = F.lit(None)

            result_scols.append(
                F.struct(
                    F.first(mean).alias("mean"),
                    F.first(med).alias("med"),
                    F.first(q1).alias("q1"),
                    F.first(q3).alias("q3"),
                    upper_whisker.alias("upper_whisker"),
                    lower_whisker.alias("lower_whisker"),
                    fliers.alias("fliers"),
                ).alias(f"_box_plot_results_{i}")
            )

        sdf_result = sdf.join(sdf_stats.hint("broadcast")).select(*result_scols)
        return sdf_result.first()


def _invoke_internal_function_over_columns(name: str, *cols: "ColumnOrName") -> Column:
    if is_remote():
        from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns

        return _invoke_function_over_columns(name, *cols)

    else:
        from pyspark.sql.classic.column import _to_seq, _to_java_column
        from pyspark import SparkContext

        sc = SparkContext._active_spark_context
        return Column(
            sc._jvm.PythonSQLUtils.internalFn(  # type: ignore
                name, _to_seq(sc, cols, _to_java_column)  # type: ignore
            )
        )


def collect_top_k(col: Column, num: int, reverse: bool) -> Column:
    return _invoke_internal_function_over_columns("collect_top_k", col, F.lit(num), F.lit(reverse))
