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

import importlib
import math

import pandas as pd
import numpy as np
from pandas.core.base import PandasObject
from pandas.core.dtypes.inference import is_integer

from pyspark.sql import functions as F, Column
from pyspark.pandas.spark import functions as SF
from pyspark.pandas.missing import unsupported_function
from pyspark.pandas.config import get_option
from pyspark.pandas.utils import name_like_string


class TopNPlotBase:
    def get_top_n(self, data):
        from pyspark.pandas import DataFrame, Series

        max_rows = get_option("plotting.max_rows")
        # Simply use the first 1k elements and make it into a pandas dataframe
        # For categorical variables, it is likely called from df.x.value_counts().plot.xxx().
        if isinstance(data, (Series, DataFrame)):
            data = data.head(max_rows + 1)._to_pandas()
        else:
            raise TypeError("Only DataFrame and Series are supported for plotting.")

        self.partial = False
        if len(data) > max_rows:
            self.partial = True
            data = data.iloc[:max_rows]
        return data

    def set_result_text(self, ax):
        max_rows = get_option("plotting.max_rows")
        assert hasattr(self, "partial")

        if self.partial:
            ax.text(
                1,
                1,
                "showing top {} elements only".format(max_rows),
                size=6,
                ha="right",
                va="bottom",
                transform=ax.transAxes,
            )


class SampledPlotBase:
    def get_sampled(self, data):
        from pyspark.pandas import DataFrame, Series

        if not isinstance(data, (DataFrame, Series)):
            raise TypeError("Only DataFrame and Series are supported for plotting.")
        if isinstance(data, Series):
            data = data.to_frame()

        fraction = get_option("plotting.sample_ratio")
        if fraction is not None:
            self.fraction = fraction
            sampled = data._internal.resolved_copy.spark_frame.sample(fraction=self.fraction)
            return DataFrame(data._internal.with_new_sdf(sampled))._to_pandas()
        else:
            from pyspark.sql import Observation

            max_rows = get_option("plotting.max_rows")
            observation = Observation("ps plotting")
            sdf = data._internal.resolved_copy.spark_frame.observe(
                observation, F.count(F.lit(1)).alias("count")
            )

            rand_col_name = "__ps_plotting_sampled_plot_base_rand__"
            id_col_name = "__ps_plotting_sampled_plot_base_id__"

            sampled = (
                sdf.select(
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

            pdf = DataFrame(data._internal.with_new_sdf(sampled))._to_pandas()

            if len(pdf) > max_rows:
                try:
                    self.fraction = float(max_rows) / observation.get["count"]
                except Exception:
                    pass
                return pdf[:max_rows]
            else:
                self.fraction = 1.0
                return pdf

    def set_result_text(self, ax):
        assert hasattr(self, "fraction")

        if self.fraction < 1:
            ax.text(
                1,
                1,
                "showing the sampled result by fraction %s" % self.fraction,
                size=6,
                ha="right",
                va="bottom",
                transform=ax.transAxes,
            )


class NumericPlotBase:
    @staticmethod
    def prepare_numeric_data(data):
        from pyspark.pandas.series import Series

        if isinstance(data, Series):
            data = data.to_frame()

        numeric_data = data.select_dtypes(
            include=["byte", "decimal", "integer", "float", "long", "double", np.datetime64]
        )

        # no empty frames or series allowed
        if len(numeric_data.columns) == 0:
            raise TypeError(
                "Empty {0!r}: no numeric data to " "plot".format(numeric_data.__class__.__name__)
            )

        return data, numeric_data


class HistogramPlotBase(NumericPlotBase):
    @staticmethod
    def prepare_hist_data(data, bins):
        data, numeric_data = NumericPlotBase.prepare_numeric_data(data)
        if is_integer(bins):
            # computes boundaries for the column
            bins = HistogramPlotBase.get_bins(data._to_spark(), bins)

        return numeric_data, bins

    @staticmethod
    def get_bins(sdf, bins):
        # 'data' is a Spark DataFrame that selects all columns.
        if len(sdf.columns) > 1:
            min_col = F.least(*map(F.min, sdf))
            max_col = F.greatest(*map(F.max, sdf))
        else:
            min_col = F.min(sdf.columns[-1])
            max_col = F.max(sdf.columns[-1])
        boundaries = sdf.select(min_col, max_col).first()

        # divides the boundaries into bins
        if boundaries[0] == boundaries[1]:
            boundaries = (boundaries[0] - 0.5, boundaries[1] + 0.5)

        return np.linspace(boundaries[0], boundaries[1], bins + 1)

    @staticmethod
    def compute_hist(psdf, bins):
        # 'data' is a Spark DataFrame that selects one column.
        assert isinstance(bins, (np.ndarray, np.generic))
        assert len(bins) > 2, "the number of buckets must be higher than 2."

        sdf = psdf._internal.spark_frame
        scols = []
        input_column_names = []
        for label in psdf._internal.column_labels:
            input_column_name = name_like_string(label)
            input_column_names.append(input_column_name)
            scols.append(psdf._internal.spark_column_for(label).alias(input_column_name))
        sdf = sdf.select(*scols)

        # 1. Make the bucket output flat to:
        #     +----------+-------+
        #     |__group_id|buckets|
        #     +----------+-------+
        #     |0         |0.0    |
        #     |0         |0.0    |
        #     |0         |1.0    |
        #     |0         |2.0    |
        #     |0         |3.0    |
        #     |0         |3.0    |
        #     |1         |0.0    |
        #     |1         |1.0    |
        #     |1         |1.0    |
        #     |1         |2.0    |
        #     |1         |1.0    |
        #     |1         |0.0    |
        #     +----------+-------+
        colnames = sdf.columns
        bucket_names = ["__{}_bucket".format(colname) for colname in colnames]

        # refers to org.apache.spark.ml.feature.Bucketizer#binarySearchForBuckets
        def binary_search_for_buckets(value: Column):
            index = SF.array_binary_search(F.lit(bins), value)
            bucket = F.when(index >= 0, index).otherwise(-index - 2)
            unboundErrMsg = F.lit(f"value %s out of the bins bounds: [{bins[0]}, {bins[-1]}]")
            return (
                F.when(value == F.lit(bins[-1]), F.lit(len(bins) - 2))
                .when(value.between(F.lit(bins[0]), F.lit(bins[-1])), bucket)
                .otherwise(F.raise_error(F.printf(unboundErrMsg, value)))
            )

        output_df = (
            sdf.select(
                F.posexplode(
                    F.array([F.col(colname).cast("double") for colname in colnames])
                ).alias("__group_id", "__value")
            )
            .where(F.col("__value").isNotNull() & ~F.col("__value").isNaN())
            .select(
                F.col("__group_id"),
                binary_search_for_buckets(F.col("__value")).cast("double").alias("__bucket"),
            )
        )

        # 2. Calculate the count based on each group and bucket.
        #     +----------+-------+------+
        #     |__group_id|buckets| count|
        #     +----------+-------+------+
        #     |0         |0.0    |2     |
        #     |0         |1.0    |1     |
        #     |0         |2.0    |1     |
        #     |0         |3.0    |2     |
        #     |1         |0.0    |2     |
        #     |1         |1.0    |3     |
        #     |1         |2.0    |1     |
        #     +----------+-------+------+
        result = (
            output_df.groupby("__group_id", "__bucket")
            .agg(F.count("*").alias("count"))
            .toPandas()
            .sort_values(by=["__group_id", "__bucket"])
        )

        # 3. Fill empty bins and calculate based on each group id. From:
        #     +----------+--------+------+
        #     |__group_id|__bucket| count|
        #     +----------+--------+------+
        #     |0         |0.0     |2     |
        #     |0         |1.0     |1     |
        #     |0         |2.0     |1     |
        #     |0         |3.0     |2     |
        #     +----------+--------+------+
        #     +----------+--------+------+
        #     |__group_id|__bucket| count|
        #     +----------+--------+------+
        #     |1         |0.0     |2     |
        #     |1         |1.0     |3     |
        #     |1         |2.0     |1     |
        #     +----------+--------+------+
        #
        # to:
        #     +-----------------+
        #     |__values1__bucket|
        #     +-----------------+
        #     |2                |
        #     |1                |
        #     |1                |
        #     |2                |
        #     |0                |
        #     +-----------------+
        #     +-----------------+
        #     |__values2__bucket|
        #     +-----------------+
        #     |2                |
        #     |3                |
        #     |1                |
        #     |0                |
        #     |0                |
        #     +-----------------+
        output_series = []
        for i, (input_column_name, bucket_name) in enumerate(zip(input_column_names, bucket_names)):
            current_bucket_result = result[result["__group_id"] == i]
            # generates a pandas DF with one row for each bin
            # we need this as some of the bins may be empty
            indexes = pd.DataFrame({"__bucket": np.arange(0, len(bins) - 1)})
            # merges the bins with counts on it and fills remaining ones with zeros
            pdf = indexes.merge(current_bucket_result, how="left", on=["__bucket"]).fillna(0)[
                ["count"]
            ]
            pdf.columns = [input_column_name]
            output_series.append(pdf[input_column_name])

        return output_series


class BoxPlotBase:
    @staticmethod
    def compute_box(sdf, colnames, whis, precision, showfliers):
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
                topk = SF.collect_top_k(pair, 1001, False)
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


class KdePlotBase(NumericPlotBase):
    @staticmethod
    def prepare_kde_data(data):
        _, numeric_data = NumericPlotBase.prepare_numeric_data(data)
        return numeric_data

    @staticmethod
    def get_ind(sdf, ind):
        def calc_min_max():
            if len(sdf.columns) > 1:
                min_col = F.least(*map(F.min, sdf))
                max_col = F.greatest(*map(F.max, sdf))
            else:
                min_col = F.min(sdf.columns[-1])
                max_col = F.max(sdf.columns[-1])
            return sdf.select(min_col, max_col).first()

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
        return ind

    @staticmethod
    def compute_kde_col(input_col, bw_method=None, ind=None):
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

    @staticmethod
    def compute_kde(sdf, bw_method=None, ind=None):
        input_col = F.col(sdf.columns[0])
        kde_col = KdePlotBase.compute_kde_col(input_col, bw_method, ind).alias("kde")
        row = sdf.select(kde_col).first()
        return row[0]


class PandasOnSparkPlotAccessor(PandasObject):
    """
    Series/Frames plotting accessor and method.

    Uses the backend specified by the
    option ``plotting.backend``. By default, plotly is used.

    Plotting methods can also be accessed by calling the accessor as a method
    with the ``kind`` argument:
    ``s.plot(kind='hist')`` is equivalent to ``s.plot.hist()``
    """

    pandas_plot_data_map = {
        "pie": TopNPlotBase().get_top_n,
        "bar": TopNPlotBase().get_top_n,
        "barh": TopNPlotBase().get_top_n,
        "scatter": SampledPlotBase().get_sampled,
        "area": SampledPlotBase().get_sampled,
        "line": SampledPlotBase().get_sampled,
    }
    _backends = {}  # type: ignore[var-annotated]

    def __init__(self, data):
        self.data = data

    @staticmethod
    def _find_backend(backend):
        """
        Find a pandas-on-Spark plotting backend
        """
        try:
            return PandasOnSparkPlotAccessor._backends[backend]
        except KeyError:
            try:
                module = importlib.import_module(backend)
            except ImportError:
                # We re-raise later on.
                pass
            else:
                if hasattr(module, "plot") or hasattr(module, "plot_pandas_on_spark"):
                    # Validate that the interface is implemented when the option
                    # is set, rather than at plot time.
                    PandasOnSparkPlotAccessor._backends[backend] = module
                    return module

        raise ValueError(
            "Could not find plotting backend '{backend}'. Ensure that you've installed "
            "the package providing the '{backend}' entrypoint, or that the package has a "
            "top-level `.plot` method.".format(backend=backend)
        )

    @staticmethod
    def _get_plot_backend(backend=None):
        backend = backend or get_option("plotting.backend")
        # Shortcut
        if backend in PandasOnSparkPlotAccessor._backends:
            return PandasOnSparkPlotAccessor._backends[backend]

        if backend == "matplotlib":
            # Because matplotlib is an optional dependency,
            # we need to attempt an import here to raise an ImportError if needed.
            try:
                # test if matplotlib can be imported
                import matplotlib  # noqa: F401
                from pyspark.pandas.plot import matplotlib as module
            except ImportError:
                raise ImportError(
                    "matplotlib is required for plotting when the "
                    "default backend 'matplotlib' is selected."
                ) from None

            PandasOnSparkPlotAccessor._backends["matplotlib"] = module
        elif backend == "plotly":
            try:
                # test if plotly can be imported
                import plotly  # noqa: F401
                from pyspark.pandas.plot import plotly as module
            except ImportError:
                raise ImportError(
                    "plotly is required for plotting when the "
                    "default backend 'plotly' is selected."
                ) from None

            PandasOnSparkPlotAccessor._backends["plotly"] = module
        else:
            module = PandasOnSparkPlotAccessor._find_backend(backend)
            PandasOnSparkPlotAccessor._backends[backend] = module
        return module

    def __call__(self, kind="line", backend=None, **kwargs):
        plot_backend = PandasOnSparkPlotAccessor._get_plot_backend(backend)
        plot_data = self.data

        if hasattr(plot_backend, "plot_pandas_on_spark"):
            # use if there's pandas-on-Spark specific method.
            return plot_backend.plot_pandas_on_spark(plot_data, kind=kind, **kwargs)
        else:
            # fallback to use pandas'
            if not PandasOnSparkPlotAccessor.pandas_plot_data_map[kind]:
                raise NotImplementedError(
                    "'%s' plot is not supported with '%s' plot "
                    "backend yet." % (kind, plot_backend.__name__)
                )
            plot_data = PandasOnSparkPlotAccessor.pandas_plot_data_map[kind](plot_data)
            return plot_backend.plot(plot_data, kind=kind, **kwargs)

    def line(self, x=None, y=None, **kwargs):
        """
        Plot DataFrame/Series as lines.

        This function is useful to plot lines using DataFrame’s values
        as coordinates.

        Parameters
        ----------
        x : int or str, optional
            Columns to use for the horizontal axis.
            Either the location or the label of the columns to be used.
            By default, it will use the DataFrame indices.
        y : int, str, or list of them, optional
            The values to be plotted.
            Either the location or the label of the columns to be used.
            By default, it will use the remaining DataFrame numeric columns.
        **kwds
            Keyword arguments to pass on to :meth:`Series.plot` or :meth:`DataFrame.plot`.

        Returns
        -------
        :class:`plotly.graph_objs.Figure`
            Return an custom object when ``backend!=plotly``.
            Return an ndarray when ``subplots=True`` (matplotlib-only).

        See Also
        --------
        plotly.express.line : Plot y versus x as lines and/or markers (plotly).
        matplotlib.pyplot.plot : Plot y versus x as lines and/or markers (matplotlib).

        Examples
        --------
        Basic plot.

        For Series:

        .. plotly::

            >>> s = ps.Series([1, 3, 2])
            >>> s.plot.line()  # doctest: +SKIP

        For DataFrame:

        .. plotly::

            The following example shows the populations for some animals
            over the years.

            >>> df = ps.DataFrame({'pig': [20, 18, 489, 675, 1776],
            ...                    'horse': [4, 25, 281, 600, 1900]},
            ...                   index=[1990, 1997, 2003, 2009, 2014])
            >>> df.plot.line()  # doctest: +SKIP

        .. plotly::

            The following example shows the relationship between both
            populations.

            >>> df = ps.DataFrame({'pig': [20, 18, 489, 675, 1776],
            ...                    'horse': [4, 25, 281, 600, 1900]},
            ...                   index=[1990, 1997, 2003, 2009, 2014])
            >>> df.plot.line(x='pig', y='horse')  # doctest: +SKIP
        """
        return self(kind="line", x=x, y=y, **kwargs)

    def bar(self, x=None, y=None, **kwds):
        """
        Vertical bar plot.

        A bar plot is a plot that presents categorical data with rectangular
        bars with lengths proportional to the values that they represent. A
        bar plot shows comparisons among discrete categories. One axis of the
        plot shows the specific categories being compared, and the other axis
        represents a measured value.

        Parameters
        ----------
        x : label or position, optional
            Allows plotting of one column versus another.
            If not specified, the index of the DataFrame is used.
        y : label or position, optional
            Allows plotting of one column versus another.
            If not specified, all numerical columns are used.
        **kwds : optional
            Additional keyword arguments are documented in
            :meth:`pyspark.pandas.Series.plot` or
            :meth:`pyspark.pandas.DataFrame.plot`.

        Returns
        -------
        :class:`plotly.graph_objs.Figure`
            Return an custom object when ``backend!=plotly``.
            Return an ndarray when ``subplots=True`` (matplotlib-only).

        Examples
        --------
        Basic plot.

        For Series:

        .. plotly::

            >>> s = ps.Series([1, 3, 2])
            >>> s.plot.bar()  # doctest: +SKIP

        For DataFrame:

        .. plotly::

            >>> df = ps.DataFrame({'lab': ['A', 'B', 'C'], 'val': [10, 30, 20]})
            >>> df.plot.bar(x='lab', y='val')  # doctest: +SKIP

        Plot a whole dataframe to a bar plot. Each column is stacked with a
        distinct color along the horizontal axis.

        .. plotly::

            >>> speed = [0.1, 17.5, 40, 48, 52, 69, 88]
            >>> lifespan = [2, 8, 70, 1.5, 25, 12, 28]
            >>> index = ['snail', 'pig', 'elephant',
            ...          'rabbit', 'giraffe', 'coyote', 'horse']
            >>> df = ps.DataFrame({'speed': speed,
            ...                    'lifespan': lifespan}, index=index)
            >>> df.plot.bar()  # doctest: +SKIP

        Instead of stacking, the figure can be split by column with plotly
        APIs.

        .. plotly::

            >>> from plotly.subplots import make_subplots
            >>> speed = [0.1, 17.5, 40, 48, 52, 69, 88]
            >>> lifespan = [2, 8, 70, 1.5, 25, 12, 28]
            >>> index = ['snail', 'pig', 'elephant',
            ...          'rabbit', 'giraffe', 'coyote', 'horse']
            >>> df = ps.DataFrame({'speed': speed,
            ...                    'lifespan': lifespan}, index=index)
            >>> fig = (make_subplots(rows=2, cols=1)
            ...        .add_trace(df.plot.bar(y='speed').data[0], row=1, col=1)
            ...        .add_trace(df.plot.bar(y='speed').data[0], row=1, col=1)
            ...        .add_trace(df.plot.bar(y='lifespan').data[0], row=2, col=1))
            >>> fig  # doctest: +SKIP

        Plot a single column.

        .. plotly::

            >>> speed = [0.1, 17.5, 40, 48, 52, 69, 88]
            >>> lifespan = [2, 8, 70, 1.5, 25, 12, 28]
            >>> index = ['snail', 'pig', 'elephant',
            ...          'rabbit', 'giraffe', 'coyote', 'horse']
            >>> df = ps.DataFrame({'speed': speed,
            ...                    'lifespan': lifespan}, index=index)
            >>> df.plot.bar(y='speed')  # doctest: +SKIP

        Plot only selected categories for the DataFrame.

        .. plotly::

            >>> speed = [0.1, 17.5, 40, 48, 52, 69, 88]
            >>> lifespan = [2, 8, 70, 1.5, 25, 12, 28]
            >>> index = ['snail', 'pig', 'elephant',
            ...          'rabbit', 'giraffe', 'coyote', 'horse']
            >>> df = ps.DataFrame({'speed': speed,
            ...                    'lifespan': lifespan}, index=index)
            >>> df.plot.bar(x='lifespan')  # doctest: +SKIP
        """
        from pyspark.pandas import DataFrame, Series

        if isinstance(self.data, Series):
            return self(kind="bar", **kwds)
        elif isinstance(self.data, DataFrame):
            return self(kind="bar", x=x, y=y, **kwds)

    def barh(self, x=None, y=None, **kwargs):
        """
        Make a horizontal bar plot.

        A horizontal bar plot is a plot that presents quantitative data with
        rectangular bars with lengths proportional to the values that they
        represent. A bar plot shows comparisons among discrete categories. One
        axis of the plot shows the specific categories being compared, and the
        other axis represents a measured value.

        Parameters
        ----------
        x : label or position, default All numeric columns in dataframe
            Columns to be plotted from the DataFrame.
        y : label or position, default DataFrame.index
            Column to be used for categories.
        **kwds
            Keyword arguments to pass on to
            :meth:`pyspark.pandas.DataFrame.plot` or :meth:`pyspark.pandas.Series.plot`.

        Returns
        -------
        :class:`plotly.graph_objs.Figure`
            Return an custom object when ``backend!=plotly``.
            Return an ndarray when ``subplots=True`` (matplotlib-only).

        Notes
        -----
        In Plotly and Matplotlib, the interpretation of `x` and `y` for `barh` plots differs.
        In Plotly, `x` refers to the values and `y` refers to the categories.
        In Matplotlib, `x` refers to the categories and `y` refers to the values.
        Ensure correct axis labeling based on the backend used.

        See Also
        --------
        plotly.express.bar : Plot a vertical bar plot using plotly.
        matplotlib.axes.Axes.bar : Plot a vertical bar plot using matplotlib.

        Examples
        --------
        For Series:

        .. plotly::

            >>> df = ps.DataFrame({'lab': ['A', 'B', 'C'], 'val': [10, 30, 20]})
            >>> df.val.plot.barh()  # doctest: +SKIP

        For DataFrame:

        .. plotly::

            >>> df = ps.DataFrame({'lab': ['A', 'B', 'C'], 'val': [10, 30, 20]})
            >>> df.plot.barh(x='lab', y='val')  # doctest: +SKIP

        Plot a whole DataFrame to a horizontal bar plot

        .. plotly::

            >>> speed = [0.1, 17.5, 40, 48, 52, 69, 88]
            >>> lifespan = [2, 8, 70, 1.5, 25, 12, 28]
            >>> index = ['snail', 'pig', 'elephant',
            ...          'rabbit', 'giraffe', 'coyote', 'horse']
            >>> df = ps.DataFrame({'speed': speed,
            ...                    'lifespan': lifespan}, index=index)
            >>> df.plot.barh()  # doctest: +SKIP

        Plot a column of the DataFrame to a horizontal bar plot

        .. plotly::

            >>> speed = [0.1, 17.5, 40, 48, 52, 69, 88]
            >>> lifespan = [2, 8, 70, 1.5, 25, 12, 28]
            >>> index = ['snail', 'pig', 'elephant',
            ...          'rabbit', 'giraffe', 'coyote', 'horse']
            >>> df = ps.DataFrame({'speed': speed,
            ...                    'lifespan': lifespan}, index=index)
            >>> df.plot.barh(y='speed')  # doctest: +SKIP

        Plot DataFrame versus the desired column

        .. plotly::

            >>> speed = [0.1, 17.5, 40, 48, 52, 69, 88]
            >>> lifespan = [2, 8, 70, 1.5, 25, 12, 28]
            >>> index = ['snail', 'pig', 'elephant',
            ...          'rabbit', 'giraffe', 'coyote', 'horse']
            >>> df = ps.DataFrame({'speed': speed,
            ...                    'lifespan': lifespan}, index=index)
            >>> df.plot.barh(x='lifespan')  # doctest: +SKIP
        """
        from pyspark.pandas import DataFrame, Series

        if isinstance(self.data, Series):
            return self(kind="barh", **kwargs)
        elif isinstance(self.data, DataFrame):
            return self(kind="barh", x=x, y=y, **kwargs)

    def box(self, **kwds):
        """
        Make a box plot of the DataFrame columns.

        A box plot is a method for graphically depicting groups of numerical data through
        their quartiles. The box extends from the Q1 to Q3 quartile values of the data,
        with a line at the median (Q2). The whiskers extend from the edges of box to show
        the range of the data. The position of the whiskers is set by default to
        1.5*IQR (IQR = Q3 - Q1) from the edges of the box. Outlier points are those past
        the end of the whiskers.

        A consideration when using this chart is that the box and the whiskers can overlap,
        which is very common when plotting small sets of data.

        Parameters
        ----------
        **kwds : dict, optional
            Extra arguments to `precision`: refer to a float that is used by
            pandas-on-Spark to compute approximate statistics for building a
            boxplot. The default value is 0.01. Use smaller values to get more
            precise statistics. Additional keyword arguments are documented in
            :meth:`pyspark.pandas.Series.plot`.

        Returns
        -------
        :class:`plotly.graph_objs.Figure`
            Return an custom object when ``backend!=plotly``.
            Return an ndarray when ``subplots=True`` (matplotlib-only).

        Notes
        -----
        There are behavior differences between pandas-on-Spark and pandas.

        * pandas-on-Spark computes approximate statistics - expect differences between
          pandas and pandas-on-Spark boxplots, especially regarding 1st and 3rd quartiles.
        * The `whis` argument is only supported as a single number.
        * pandas-on-Spark doesn't support the following argument(s) (matplotlib-only).

          * `bootstrap` argument is not supported
          * `autorange` argument is not supported

        Examples
        --------
        Draw a box plot from a DataFrame with four columns of randomly
        generated data.

        For Series:

        .. plotly::

            >>> data = np.random.randn(25, 4)
            >>> df = ps.DataFrame(data, columns=list('ABCD'))
            >>> df['A'].plot.box()  # doctest: +SKIP

        This is an unsupported function for DataFrame type
        """
        from pyspark.pandas import DataFrame, Series

        if isinstance(self.data, (Series, DataFrame)):
            return self(kind="box", **kwds)

    def hist(self, bins=10, **kwds):
        """
        Draw one histogram of the DataFrame’s columns.

        A `histogram`_ is a representation of the distribution of data.
        This function calls :meth:`plotting.backend.plot`,
        on each series in the DataFrame, resulting in one histogram per column.
        This is useful when the DataFrame’s Series are in a similar scale.

        .. _histogram: https://en.wikipedia.org/wiki/Histogram

        Parameters
        ----------
        bins : integer or sequence, default 10
            Number of histogram bins to be used. If an integer is given, bins + 1
            bin edges are calculated and returned. If bins is a sequence, it gives
            bin edges, including left edge of first bin and right edge of last
            bin. In this case, bins are returned unmodified.
        **kwds
            All other plotting keyword arguments to be passed to
            plotting backend.

        Returns
        -------
        :class:`plotly.graph_objs.Figure`
            Return an custom object when ``backend!=plotly``.
            Return an ndarray when ``subplots=True`` (matplotlib-only).

        Examples
        --------
        Basic plot.

        For Series:

        .. plotly::

            >>> s = ps.Series([1, 3, 2])
            >>> s.plot.hist()  # doctest: +SKIP

        For DataFrame:

        .. plotly::

            >>> df = pd.DataFrame(
            ...     np.random.randint(1, 7, 6000),
            ...     columns=['one'])
            >>> df['two'] = df['one'] + np.random.randint(1, 7, 6000)
            >>> df = ps.from_pandas(df)
            >>> df.plot.hist(bins=12, alpha=0.5)  # doctest: +SKIP
        """
        return self(kind="hist", bins=bins, **kwds)

    def kde(self, bw_method=None, ind=None, **kwargs):
        """
        Generate Kernel Density Estimate plot using Gaussian kernels.

        In statistics, kernel density estimation (KDE) is a non-parametric way to
        estimate the probability density function (PDF) of a random variable. This
        function uses Gaussian kernels and includes automatic bandwidth determination.

        Parameters
        ----------
        bw_method : scalar
            The method used to calculate the estimator bandwidth.
            See KernelDensity in PySpark for more information.
        ind : NumPy array or integer, optional
            Evaluation points for the estimated PDF. If None (default),
            1000 equally spaced points are used. If `ind` is a NumPy array, the
            KDE is evaluated at the points passed. If `ind` is an integer,
            `ind` number of equally spaced points are used.
        **kwargs : optional
            Keyword arguments to pass on to :meth:`pandas-on-Spark.Series.plot`.

        Returns
        -------
        :class:`plotly.graph_objs.Figure`
            Return an custom object when ``backend!=plotly``.
            Return an ndarray when ``subplots=True`` (matplotlib-only).

        Examples
        --------
        A scalar bandwidth should be specified. Using a small bandwidth value can
        lead to over-fitting, while using a large bandwidth value may result
        in under-fitting:

        .. plotly::

            >>> s = ps.Series([1, 2, 2.5, 3, 3.5, 4, 5])
            >>> s.plot.kde(bw_method=0.3)  # doctest: +SKIP

        .. plotly::

            >>> s = ps.Series([1, 2, 2.5, 3, 3.5, 4, 5])
            >>> s.plot.kde(bw_method=3)  # doctest: +SKIP

        The `ind` parameter determines the evaluation points for the
        plot of the estimated KDF:

        .. plotly::

            >>> s = ps.Series([1, 2, 2.5, 3, 3.5, 4, 5])
            >>> s.plot.kde(ind=[1, 2, 3, 4, 5], bw_method=0.3)  # doctest: +SKIP

        For DataFrame, it works in the same way as Series:

        .. plotly::

            >>> df = ps.DataFrame({
            ...     'x': [1, 2, 2.5, 3, 3.5, 4, 5],
            ...     'y': [4, 4, 4.5, 5, 5.5, 6, 6],
            ... })
            >>> df.plot.kde(bw_method=0.3)  # doctest: +SKIP

        .. plotly::

            >>> df = ps.DataFrame({
            ...     'x': [1, 2, 2.5, 3, 3.5, 4, 5],
            ...     'y': [4, 4, 4.5, 5, 5.5, 6, 6],
            ... })
            >>> df.plot.kde(bw_method=3)  # doctest: +SKIP

        .. plotly::

            >>> df = ps.DataFrame({
            ...     'x': [1, 2, 2.5, 3, 3.5, 4, 5],
            ...     'y': [4, 4, 4.5, 5, 5.5, 6, 6],
            ... })
            >>> df.plot.kde(ind=[1, 2, 3, 4, 5, 6], bw_method=0.3)  # doctest: +SKIP
        """
        return self(kind="kde", bw_method=bw_method, ind=ind, **kwargs)

    density = kde

    def area(self, x=None, y=None, **kwds):
        """
        Draw a stacked area plot.

        An area plot displays quantitative data visually.
        This function wraps the plotly area function.

        Parameters
        ----------
        x : label or position, optional
            Coordinates for the X axis. By default it uses the index.
        y : label or position, optional
            Column to plot. By default it uses all columns.
        stacked : bool, default True
            Area plots are stacked by default. Set to False to create an
            unstacked plot (matplotlib-only).
        **kwds : optional
            Additional keyword arguments are documented in
            :meth:`DataFrame.plot`.

        Returns
        -------
        :class:`plotly.graph_objs.Figure`
            Return an custom object when ``backend!=plotly``.
            Return an ndarray when ``subplots=True`` (matplotlib-only).

        Examples
        --------

        For Series

        .. plotly::

            >>> df = ps.DataFrame({
            ...     'sales': [3, 2, 3, 9, 10, 6],
            ...     'signups': [5, 5, 6, 12, 14, 13],
            ...     'visits': [20, 42, 28, 62, 81, 50],
            ... }, index=pd.date_range(start='2018/01/01', end='2018/07/01',
            ...                        freq='ME'))
            >>> df.sales.plot.area()  # doctest: +SKIP

        For DataFrame

        .. plotly::

            >>> df = ps.DataFrame({
            ...     'sales': [3, 2, 3, 9, 10, 6],
            ...     'signups': [5, 5, 6, 12, 14, 13],
            ...     'visits': [20, 42, 28, 62, 81, 50],
            ... }, index=pd.date_range(start='2018/01/01', end='2018/07/01',
            ...                        freq='ME'))
            >>> df.plot.area()  # doctest: +SKIP
        """
        from pyspark.pandas import DataFrame, Series

        if isinstance(self.data, Series):
            return self(kind="area", **kwds)
        elif isinstance(self.data, DataFrame):
            return self(kind="area", x=x, y=y, **kwds)

    def pie(self, **kwds):
        """
        Generate a pie plot.

        A pie plot is a proportional representation of the numerical data in a
        column. This function wraps :meth:`plotly.express.pie` for the
        specified column.

        Parameters
        ----------
        y : int or label, optional
            Label or position of the column to plot.
            If not provided, ``subplots=True`` argument must be passed (matplotlib-only).
        **kwds
            Keyword arguments to pass on to :meth:`pandas-on-Spark.Series.plot`.

        Returns
        -------
        :class:`plotly.graph_objs.Figure`
            Return an custom object when ``backend!=plotly``.
            Return an ndarray when ``subplots=True`` (matplotlib-only).

        Examples
        --------

        For Series:

        .. plotly::

            >>> df = ps.DataFrame({'mass': [0.330, 4.87, 5.97],
            ...                    'radius': [2439.7, 6051.8, 6378.1]},
            ...                   index=['Mercury', 'Venus', 'Earth'])
            >>> df.mass.plot.pie()  # doctest: +SKIP


        For DataFrame:

        .. plotly::

            >>> df = ps.DataFrame({'mass': [0.330, 4.87, 5.97],
            ...                    'radius': [2439.7, 6051.8, 6378.1]},
            ...                   index=['Mercury', 'Venus', 'Earth'])
            >>> df.plot.pie(y='mass')  # doctest: +SKIP
        """
        from pyspark.pandas import DataFrame, Series

        if isinstance(self.data, Series):
            return self(kind="pie", **kwds)
        else:
            # pandas will raise an error if y is None and subplots if not True
            if (
                isinstance(self.data, DataFrame)
                and kwds.get("y", None) is None
                and not kwds.get("subplots", False)
            ):
                raise ValueError(
                    "pie requires either y column or 'subplots=True' (matplotlib-only)"
                )
            return self(kind="pie", **kwds)

    def scatter(self, x, y, **kwds):
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
        x : int or str
            The column name or column position to be used as horizontal
            coordinates for each point.
        y : int or str
            The column name or column position to be used as vertical
            coordinates for each point.
        s : scalar or array_like, optional
            (matplotlib-only).
        c : str, int or array_like, optional
            (matplotlib-only).

        **kwds: Optional
            Keyword arguments to pass on to :meth:`pyspark.pandas.DataFrame.plot`.

        Returns
        -------
        :class:`plotly.graph_objs.Figure`
            Return an custom object when ``backend!=plotly``.
            Return an ndarray when ``subplots=True`` (matplotlib-only).

        See Also
        --------
        plotly.express.scatter : Scatter plot using multiple input data
            formats (plotly).
        matplotlib.pyplot.scatter : Scatter plot using multiple input data
            formats (matplotlib).

        Examples
        --------
        Let's see how to draw a scatter plot using coordinates from the values
        in a DataFrame's columns.

        .. plotly::

            >>> df = ps.DataFrame([[5.1, 3.5, 0], [4.9, 3.0, 0], [7.0, 3.2, 1],
            ...                    [6.4, 3.2, 1], [5.9, 3.0, 2]],
            ...                   columns=['length', 'width', 'species'])
            >>> df.plot.scatter(x='length', y='width')  # doctest: +SKIP

        And now with dark scheme:

        .. plotly::

            >>> df = ps.DataFrame([[5.1, 3.5, 0], [4.9, 3.0, 0], [7.0, 3.2, 1],
            ...                    [6.4, 3.2, 1], [5.9, 3.0, 2]],
            ...                   columns=['length', 'width', 'species'])
            >>> fig = df.plot.scatter(x='length', y='width')
            >>> fig.update_layout(template="plotly_dark")  # doctest: +SKIP
        """
        return self(kind="scatter", x=x, y=y, **kwds)

    def hexbin(self, **kwds):
        return unsupported_function(class_name="pd.DataFrame", method_name="hexbin")()
